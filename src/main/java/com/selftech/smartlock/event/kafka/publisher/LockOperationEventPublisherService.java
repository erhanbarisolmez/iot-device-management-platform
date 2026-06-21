package com.selftech.smartlock.event.kafka.publisher;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.selftech.kafka.config.KafkaTopicRegistry;
import com.selftech.kafka.config.TopicKey;
import com.selftech.kafka.core.publisher.CentralEventCoordinator;
import com.selftech.smartlock.kafka.events.LockOperationEvent;
import com.selftech.smartlock.models.entity.DeviceOperation;
import com.selftech.smartlock.models.entity.LockBox;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class LockOperationEventPublisherService {

    private static final Logger logger = LoggerFactory.getLogger(LockOperationEventPublisherService.class);

    private final CentralEventCoordinator eventCoordinator;
    private final KafkaTopicRegistry topicRegistry;

    /**
     * Cihaz ile ilgili operasyonları 'lock.operations' topic'ine yayınlar.
     * @param operation Gerçekleşen cihaz operasyonu.
     * @param eventType Operasyonun tipi (örn: INITIALIZE, UNLOCK, RETURN).
     */
    public void publishLockOperationEvent(DeviceOperation operation, String eventType) {
        try {
            LockOperationEvent event = LockOperationEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setOperationId(operation.getId())
                    .setDeviceId(operation.getDevice().getDeviceCode())
                    .setBoxCode(operation.getLockBox() != null ? operation.getLockBox().getBoxCode() : null)
                    .setLockCode(operation.getLockCode())
                    .setEventType(eventType)
                    .setStatus(operation.getStatus().name())
          
                    .build();

            // Cihaz operasyonları için anahtar olarak deviceCode kullanmak daha mantıklı olabilir.
            String key = operation.getDevice() != null ? operation.getDevice().getDeviceCode() : operation.getLockCode();
            eventCoordinator.publishEvent(topicRegistry.getTopicName(TopicKey.SMARTLOCK_DEVICE_OPERATIONS), key, event);
        } catch (Exception e) {
            logger.error("Cihaz operasyon olayı Kafka'ya gönderilemedi: {}", operation.getId(), e);
        }
    }

    /**
     * Kutu ile ilgili olayları 'lock.operations' topic'ine yayınlar.
     * @param box Etkilenen kutu.
     * @param eventType Olayın tipi (örn: BOX_OCCUPIED, BOX_EMPTY).
     */
    public void publishLockOperationEvent(LockBox box, String eventType) {
        try {
            LockOperationEvent event = LockOperationEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setOperationId(null)
                    .setDeviceId(box.getDevices().isEmpty() ? null : box.getDevices().get(0).getDeviceCode())
                    .setBoxCode(box.getBoxCode())
                    .setLockCode(null)
                    .setEventType(eventType)
                    .setStatus(box.getStatus().name())
                    .build();

            eventCoordinator.publishEvent(topicRegistry.getTopicName(TopicKey.SMARTLOCK_BOX_OPERATION), box.getBoxCode(), event);
        } catch (Exception e) {
            logger.error("Kutu olayı Kafka'ya gönderilemedi: {}", box.getBoxCode(), e);
        }
    }
}
