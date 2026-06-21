package com.selftech.smartlock.config;

import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.support.DefaultPahoMessageConverter;
import org.springframework.integration.router.ExpressionEvaluatingRouter;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

import org.apache.avro.specific.SpecificRecord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.selftech.kafka.config.KafkaTopicRegistry;
import com.selftech.kafka.config.TopicNameResolver;
import com.selftech.kafka.converter.EventConverterFactory;
import com.selftech.kafka.core.publisher.CentralEventCoordinator;
import com.selftech.smartlock.models.dto.request.SimpleMessageRequest;
import com.selftech.smartlock.models.dto.request.sensor.SensorDataRequest;
import com.selftech.smartlock.service.concretes.LockBoxOperationManager;
import com.selftech.smartlock.service.concretes.MqttToKafkaBridgeService;

import lombok.RequiredArgsConstructor;

@Configuration
@RequiredArgsConstructor
public class MQTTConfig {

    private static final Logger logger = LoggerFactory.getLogger(MQTTConfig.class);
    private final LockBoxOperationManager lockBoxOperationManager;
    private final ObjectMapper objectMapper;
    private final MqttToKafkaBridgeService mqttToKafkaBridgeService;
    private final KafkaTopicRegistry kafkaTopicRegistry;
    private final EventConverterFactory eventConverterFactory;
    private final CentralEventCoordinator eventCoordinator;

    @Value("${mqtt.broker.url}")
    private String brokerUrl;

    @Value("${mqtt.client.id}")
    private String clientId;

    @Value("${mqtt.topic}")
    private String rootTopic;

    @Bean
    public MqttPahoClientFactory mqttClientFactory() {
        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();
        MqttConnectOptions options = new MqttConnectOptions();
        options.setServerURIs(new String[] { brokerUrl });
        options.setCleanSession(true);
        factory.setConnectionOptions(options);
        return factory;
    }

    @Bean
    public MessageChannel mqttInputChannel() {
        return new DirectChannel();
    }

    @Bean
    public MessageProducer inbound() {
        MqttPahoMessageDrivenChannelAdapter adapter = new MqttPahoMessageDrivenChannelAdapter(clientId + "_inbound",
                mqttClientFactory(), rootTopic);
        adapter.setCompletionTimeout(5000);
        adapter.setConverter(new DefaultPahoMessageConverter());
        adapter.setQos(1);
        adapter.setOutputChannel(mqttInputChannel());
        return adapter;
    }

    @Bean
    @ServiceActivator(inputChannel = "mqttInputChannel")
    public ExpressionEvaluatingRouter topicRouter() {
        ExpressionEvaluatingRouter router = new ExpressionEvaluatingRouter(
                "headers['mqtt_receivedTopic'].matches('selfpark/.*/data') ? 'sensorDataChannel' : " +
                        "headers['mqtt_receivedTopic'].matches('selfpark/.*/message') ? 'messageChannel' : 'unmatchedMessagesChannel'");
        return router;
    }

    @Bean
    public MessageChannel sensorDataChannel() {
        return new DirectChannel();
    }

    @Bean
    @ServiceActivator(inputChannel = "sensorDataChannel")
    public MessageHandler sensorDataHandler() {
        return message -> {
            String topic = (String) message.getHeaders().get("mqtt_receivedTopic");
            String payload = message.getPayload().toString();
            logger.info("Sensör verisi alındı. Topic: {}, Payload: {}", topic, payload);

            String[] topicParts = topic.split("/");
            if (topicParts.length != 3) { // selfpark, box-code, data
                logger.warn("Geçersiz sensör verisi formatı. Konu: {}", topic);
                return;
            }
            String boxCodeFromTopic = topicParts[1];

            SensorDataRequest data;
            try {
                data = objectMapper.readValue(payload, SensorDataRequest.class);
            } catch (Exception e) {
                logger.error("Sensör verisi JSON formatından dönüştürülemedi: {}. Veri MQTT topic'ten reddedildi.", e.getMessage());
                // DLQ routing artık CentralEventConsumer tarafından otomatik olarak handle edilir
                return;
            }

            // ADIM 1: İş mantığını çalıştır ve cihaz verisini işle (validation)
            // Bu işlem, cihazın varlığını kontrol edecek
            try {
                lockBoxOperationManager.processDeviceData(boxCodeFromTopic, data);
            } catch (Exception validationException) {
                // Validation hatası - veriyi doğrudan DLQ'ya gönder (Avro formatında)
                logger.error("Cihaz validasyonu başarısız: {} - Error: {}. Veri Avro formatında DLQ'ya yönlendiriliyor.",
                    validationException.getClass().getSimpleName(), validationException.getMessage());

                try {
                    // Step 1: Convert DTO to JSON
                    String payloadJson = objectMapper.writeValueAsString(data);

                    // Step 2: Convert JSON to Avro (SensorDataEvent) using EventConverterFactory
                    String normalTopic = kafkaTopicRegistry.getTopicName("smartlock-mqtt-sensor-data");
                    SpecificRecord avroEvent = eventConverterFactory.convert(normalTopic, payloadJson);

                    // Step 3: Publish Avro event directly to DLQ via CentralEventCoordinator
                    String dlqTopic = TopicNameResolver.dlq(normalTopic);
                    eventCoordinator.publishEvent(dlqTopic, boxCodeFromTopic, avroEvent);

                    logger.info("Validation error: Sensor data successfully routed to DLQ as Avro. DLQ Topic: {}, Box: {}", dlqTopic, boxCodeFromTopic);
                } catch (Exception dlqException) {
                    logger.error("Failed to route validation error to DLQ: {}", dlqException.getMessage(), dlqException);
                }
                return; // ÖNEMLI: normal topic'e gitmesin
            }

            // ADIM 2: Kafka event'ini gönder
            // NOT: DLQ routing CentralEventConsumer tarafından otomatik olarak handle edilir
            // MQTTConfig sadece normal topic'e yazar, handler başarısızlığı CentralEventConsumer'da kontrol edilir
            logger.info("Publishing sensor data to Kafka. Box: {}, Topic: smartlock.mqtt.sensorData.v0",
                boxCodeFromTopic);
            try {
                mqttToKafkaBridgeService.forwardSensorDataToKafka(boxCodeFromTopic, data, kafkaTopicRegistry.getTopicName("smartlock-mqtt-sensor-data"));
                logger.info("Sensor data successfully published to Kafka. Box: {}", boxCodeFromTopic);
            } catch (Exception kafkaException) {
                // Kafka publish hatası - loglama yap, işlem devam etsin
                // DLQ routing CentralEventConsumer tarafından otomatik olarak yapılacak
                logger.error("Failed to publish sensor data to Kafka - Exception Type: {}, Message: {}",
                    kafkaException.getClass().getSimpleName(), kafkaException.getMessage(), kafkaException);
            }
        };
    }

    @Bean
    public MessageChannel kafkaFailureChannel() {
        return new DirectChannel();
    }

    @Bean
    @ServiceActivator(inputChannel = "kafkaFailureChannel")
    public MessageHandler kafkaFailureHandler() {
        return message -> logger.error("❌ Kafka'ya mesaj gönderilemedi. Hata: {}", message.getPayload());
    }

    @Bean
    public MessageChannel unmatchedMessagesChannel() {
        return new DirectChannel();
    }

    @Bean
    @ServiceActivator(inputChannel = "unmatchedMessagesChannel")
    public MessageHandler unmatchedMessagesHandler() {
        return message -> {
            String topic = (String) message.getHeaders().get("mqtt_receivedTopic");
            logger.warn("Eşleşen bir yönlendirme kuralı bulunamayan mesaj alındı. Topic: {}, Payload: {}",
                    topic, message.getPayload());
        };
    }

    @Bean
    public MessageChannel messageChannel() {
        return new DirectChannel();
    }

    @Bean
    @ServiceActivator(inputChannel = "messageChannel")
    public MessageHandler messageHandler() {
        return message -> {
            logger.info("Genel mesaj alındı. Topic: {}, Payload: {}", message.getHeaders().get("mqtt_receivedTopic"),
                    message.getPayload());
            try {
                SimpleMessageRequest data = objectMapper.readValue(message.getPayload().toString(),
                        SimpleMessageRequest.class);

                String topic = (String) message.getHeaders().get("mqtt_receivedTopic");
                String[] topicParts = topic.split("/");
                String deviceId = "unknown";
                if (topicParts.length == 3) { // selfpark, device-id, message
                    deviceId = topicParts[1];
                }
                logger.info("[{}] cihazından gelen bildirim mesajı: '{}'", deviceId, data.getMessage());
            } catch (Exception e) {
                logger.error("Genel mesaj işlenirken hata oluştu: ", e);
            }
        };
    }
}
