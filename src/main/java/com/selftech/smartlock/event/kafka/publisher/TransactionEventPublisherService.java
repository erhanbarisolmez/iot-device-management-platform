package com.selftech.smartlock.event.kafka.publisher;

import java.util.UUID;
import java.time.Instant;
import java.time.Instant;

import org.slf4j.Logger;
import java.time.Instant;
import org.slf4j.LoggerFactory;
import java.time.Instant;
import org.springframework.stereotype.Service;
import java.time.Instant;

import com.selftech.kafka.config.KafkaTopicRegistry;
import java.time.Instant;
import com.selftech.kafka.config.TopicKey;
import java.time.Instant;
import com.selftech.kafka.core.publisher.CentralEventCoordinator;
import java.time.Instant;
import com.selftech.smartlock.avro.TransactionEvent;
import java.time.Instant;

import lombok.RequiredArgsConstructor;
import java.time.Instant;

/**
 * Müşteri işlem olaylarını Kafka'ya yayınlayan servis.
 *
 * Cihaz alma, iade, kod doğrulama gibi müşteri aksiyonlarını
 * event olarak Kafka'ya gönderir. Partition key olarak userId kullanılır.
 */
@Service
@RequiredArgsConstructor
public class TransactionEventPublisherService {

    private static final Logger logger = LoggerFactory.getLogger(TransactionEventPublisherService.class);
    private final CentralEventCoordinator eventCoordinator;
    private final KafkaTopicRegistry topicRegistry;

    /**
     * Cihaz alma (alma kodunun doğrulandığı anı) olayını yayınla
     */
    public void publishDeviceRetrievedEvent(String userId, String deviceCode, String boxCode,
                                            String lockCode, String result) {
        try {
            TransactionEvent event = TransactionEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setTransactionId(UUID.randomUUID().toString())
                    .setEventType("DeviceRetrieved")
                    .setUserId(userId)
                    .setDeviceCode(deviceCode)
                    .setBoxCode(boxCode)
                    .setLockCode(lockCode)
                    .setResult(result)
                    .setReason(null)
                    .setAttemptCount(1)
                    .setOperationType("TAKE_FROM_BOX")
                    .setTimestamp(Instant.now())
                    .setDuration(null)
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            eventCoordinator.publishEvent(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_CUSTOMER_TRANSACTIONS),
                    userId,  // Partition key
                    event
            );

            logger.info("Device retrieved event published: userId={}, deviceCode={}, result={}",
                    userId, deviceCode, result);

        } catch (Exception e) {
            logger.error("Failed to publish device retrieved event: userId={}, deviceCode={}",
                    userId, deviceCode, e);
        }
    }

    /**
     * Cihaz iade olayını yayınla
     */
    public void publishDeviceReturnedEvent(String userId, String deviceCode, String boxCode, String result) {
        try {
            TransactionEvent event = TransactionEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setTransactionId(UUID.randomUUID().toString())
                    .setEventType("DeviceReturned")
                    .setUserId(userId)
                    .setDeviceCode(deviceCode)
                    .setBoxCode(boxCode)
                    .setLockCode(null)
                    .setResult(result)
                    .setReason(null)
                    .setAttemptCount(1)
                    .setOperationType("RETURN_TO_BOX")
                    .setTimestamp(Instant.now())
                    .setDuration(null)
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            eventCoordinator.publishEvent(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_CUSTOMER_TRANSACTIONS),
                    userId,
                    event
            );

            logger.info("Device returned event published: userId={}, deviceCode={}", userId, deviceCode);

        } catch (Exception e) {
            logger.error("Failed to publish device returned event: userId={}, deviceCode={}",
                    userId, deviceCode, e);
        }
    }

    /**
     * Kod doğrulama olayını yayınla (başarılı veya başarısız)
     */
    public void publishCodeValidatedEvent(String userId, String deviceCode, String boxCode,
                                         String codeType, String result, String reason, int attemptCount) {
        try {
            TransactionEvent event = TransactionEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setTransactionId(UUID.randomUUID().toString())
                    .setEventType("CodeValidated")
                    .setUserId(userId)
                    .setDeviceCode(deviceCode)
                    .setBoxCode(boxCode)
                    .setLockCode(null)
                    .setResult(result)
                    .setReason(reason)
                    .setAttemptCount(attemptCount)
                    .setOperationType(codeType)  // RETRIEVAL_CODE, OPENING_CODE, etc.
                    .setTimestamp(Instant.now())
                    .setDuration(null)
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            eventCoordinator.publishEvent(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_CUSTOMER_TRANSACTIONS),
                    userId,
                    event
            );

            logger.info("Code validated event published: userId={}, codeType={}, result={}, attempts={}",
                    userId, codeType, result, attemptCount);

        } catch (Exception e) {
            logger.error("Failed to publish code validated event: userId={}", userId, e);
        }
    }

    /**
     * Personel için kod üretme olayını yayınla
     */
    public void publishCodeGeneratedForPersonnelEvent(String personelUserId, String boxCode, String codeType) {
        try {
            TransactionEvent event = TransactionEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setTransactionId(UUID.randomUUID().toString())
                    .setEventType("CodeGeneratedForPersonnel")
                    .setUserId(personelUserId)
                    .setDeviceCode(null)
                    .setBoxCode(boxCode)
                    .setLockCode(null)
                    .setResult("SUCCESS")
                    .setReason(null)
                    .setAttemptCount(1)
                    .setOperationType(codeType)  // OPENING_CODE, RETRIEVAL_CODE, etc.
                    .setTimestamp(Instant.now())
                    .setDuration(null)
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            eventCoordinator.publishEvent(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_CUSTOMER_TRANSACTIONS),
                    personelUserId,
                    event
            );

            logger.info("Code generated for personnel event published: personelId={}, boxCode={}, codeType={}",
                    personelUserId, boxCode, codeType);

        } catch (Exception e) {
            logger.error("Failed to publish code generated event: personelId={}", personelUserId, e);
        }
    }

    /**
     * Genel işlem olayını yayınla (flexibility için)
     */
    public void publishTransactionEvent(String userId, String deviceCode, String boxCode,
                                       String eventType, String operationType, String result, String reason) {
        try {
            TransactionEvent event = TransactionEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setTransactionId(UUID.randomUUID().toString())
                    .setEventType(eventType)
                    .setUserId(userId)
                    .setDeviceCode(deviceCode)
                    .setBoxCode(boxCode)
                    .setLockCode(null)
                    .setResult(result)
                    .setReason(reason)
                    .setAttemptCount(1)
                    .setOperationType(operationType)
                    .setTimestamp(Instant.now())
                    .setDuration(null)
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            eventCoordinator.publishEvent(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_CUSTOMER_TRANSACTIONS),
                    userId,
                    event
            );

            logger.info("Transaction event published: userId={}, eventType={}, result={}",
                    userId, eventType, result);

        } catch (Exception e) {
            logger.error("Failed to publish transaction event: userId={}, eventType={}", userId, eventType, e);
        }
    }
}
