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
import com.selftech.smartlock.models.entity.DeviceOperation;
import java.time.Instant;
// import generated Avro class
import com.selftech.smartlock.avro.PaymentEvent;
import java.time.Instant;

import lombok.RequiredArgsConstructor;
import java.time.Instant;

/**
 * Ödeme ile ilgili olayları Kafka'ya yayınlayan servis.
 *
 * Ödeme başlatma, tamamlama, başarısızlık, iade gibi işlemleri
 * event olarak Kafka'ya gönderir. Partition key olarak userId kullanılır.
 *
 * OPTIMIZATION (Phase 4.6):
 * - Old: deviceCode (causes hot partitions, uneven distribution)
 * - New: userId (better distribution, user-centric aggregation)
 *
 * FAZE 2 ENTERPRISE UPGRADE:
 * - Migrated from KafkaTopicConfig to KafkaTopicRegistry
 * - Topic names loaded from application.yml
 */
@Service
@RequiredArgsConstructor
public class PaymentEventPublisherService implements IPaymentEventPublisher {

    private static final Logger logger = LoggerFactory.getLogger(PaymentEventPublisherService.class);
    private final CentralEventCoordinator eventCoordinator;
    private final KafkaTopicRegistry topicRegistry;

    @Override
    public String getPublisherName() {
        return "PaymentEventPublisher";
    }

    @Override
    public boolean isHealthy() {
        return eventCoordinator != null && topicRegistry != null;
    }

    /**
     * Ödeme başlatma olayını yayınla
     */
    public void publishPaymentInitiatedEvent(DeviceOperation operation) {
        try {
            PaymentEvent event = PaymentEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setPaymentId(operation.getPaymentId() != null ? operation.getPaymentId() : "PENDING")
                    .setLockCode(operation.getLockCode())
                    .setDeviceCode(operation.getDevice().getDeviceCode())
                    .setEventType("PaymentInitiated")
                    .setAmount(operation.getTotalAmount() != null ? operation.getTotalAmount().doubleValue() : 0.0)
                    .setCurrency("TRY")
                    .setPreviousStatus(null)
                    .setStatus(operation.getPaymentStatus() != null ? operation.getPaymentStatus().name() : "PENDING")
                    .setUserId(operation.getUser() != null ? operation.getUser().getId().toString() : null)
                    .setPaymentMethod(null)
                    .setFailureReason(null)
                    .setDepositAmount(
                            operation.getDepositAmount() != null ? operation.getDepositAmount().doubleValue() : null)
                    .setPenaltyAmount(
                            operation.getPenaltyAmount() != null ? operation.getPenaltyAmount().doubleValue() : null)
                    .setLockFee(operation.getLockFee() != null ? operation.getLockFee().doubleValue() : null)
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            eventCoordinator.publishEvent(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_PAYMENT_EVENTS),
                    operation.getUser() != null ? operation.getUser().getId().toString() : "UNKNOWN", // Partition key: userId
                    event);

            logger.info("Payment initiated event published: lockCode={}, amount={}, eventId={}",
                    operation.getLockCode(), operation.getTotalAmount(), event.getEventId());

        } catch (Exception e) {
            logger.error("Failed to publish payment initiated event: lockCode={}",
                    operation.getLockCode(), e);
        }
    }

    /**
     * Ödeme başarılı olayını yayınla
     */
    public void publishPaymentCompletedEvent(DeviceOperation operation) {
        try {
            PaymentEvent event = PaymentEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setPaymentId(operation.getPaymentId())
                    .setLockCode(operation.getLockCode())
                    .setDeviceCode(operation.getDevice().getDeviceCode())
                    .setEventType("PaymentCompleted")
                    .setAmount(operation.getTotalAmount() != null ? operation.getTotalAmount().doubleValue() : 0.0)
                    .setCurrency("TRY")
                    .setPreviousStatus("PENDING")
                    .setStatus("COMPLETED")
                    .setUserId(operation.getUser() != null ? operation.getUser().getId().toString() : null)
                    .setPaymentMethod(null)
                    .setFailureReason(null)
                    .setDepositAmount(
                            operation.getDepositAmount() != null ? operation.getDepositAmount().doubleValue() : null)
                    .setPenaltyAmount(
                            operation.getPenaltyAmount() != null ? operation.getPenaltyAmount().doubleValue() : null)
                    .setLockFee(operation.getLockFee() != null ? operation.getLockFee().doubleValue() : null)
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            eventCoordinator.publishEvent(
            topicRegistry.getTopicName(TopicKey.SMARTLOCK_PAYMENT_EVENTS),
            operation.getUser() != null ? operation.getUser().getId().toString() : "UNKNOWN", // Partition key: userId
            event
            );

            logger.info("Payment completed event published: lockCode={}, paymentId={}",
                    operation.getLockCode(), operation.getPaymentId());

        } catch (Exception e) {
            logger.error("Failed to publish payment completed event: lockCode={}",
                    operation.getLockCode(), e);
        }
    }

    /**
     * Ödeme başarısız olayını yayınla
     */
    public void publishPaymentFailedEvent(DeviceOperation operation, String failureReason) {
        try {
            PaymentEvent event = PaymentEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setPaymentId(operation.getPaymentId() != null ? operation.getPaymentId() : "FAILED")
                    .setLockCode(operation.getLockCode())
                    .setDeviceCode(operation.getDevice().getDeviceCode())
                    .setEventType("PaymentFailed")
                    .setAmount(operation.getTotalAmount() != null ? operation.getTotalAmount().doubleValue() : 0.0)
                    .setCurrency("TRY")
                    .setPreviousStatus("PENDING")
                    .setStatus("FAILED")
                    .setUserId(operation.getUser() != null ? operation.getUser().getId().toString() : null)
                    .setPaymentMethod(null)
                    .setFailureReason(failureReason)
                    .setDepositAmount(
                            operation.getDepositAmount() != null ? operation.getDepositAmount().doubleValue() : null)
                    .setPenaltyAmount(
                            operation.getPenaltyAmount() != null ? operation.getPenaltyAmount().doubleValue() : null)
                    .setLockFee(operation.getLockFee() != null ? operation.getLockFee().doubleValue() : null)
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            eventCoordinator.publishEvent(
            topicRegistry.getTopicName(TopicKey.SMARTLOCK_PAYMENT_EVENTS),
            operation.getDevice().getDeviceCode(),
            event
            );

            logger.warn("Payment failed event published: lockCode={}, reason={}",
                    operation.getLockCode(), failureReason);

        } catch (Exception e) {
            logger.error("Failed to publish payment failed event: lockCode={}",
                    operation.getLockCode(), e);
        }
    }

    /**
     * Iade işlemi olayını yayınla
     */
    public void publishRefundProcessedEvent(DeviceOperation operation) {
        try {
            PaymentEvent event = PaymentEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setPaymentId(operation.getPaymentId())
                    .setLockCode(operation.getLockCode())
                    .setDeviceCode(operation.getDevice().getDeviceCode())
                    .setEventType("RefundProcessed")
                    .setAmount(operation.getRefundAmount() != null ? operation.getRefundAmount().doubleValue() : 0.0)
                    .setCurrency("TRY")
                    .setPreviousStatus("COMPLETED")
                    .setStatus("REFUNDED")
                    .setUserId(operation.getUser() != null ? operation.getUser().getId().toString() : null)
                    .setPaymentMethod(null)
                    .setFailureReason(null)
                    .setDepositAmount(
                            operation.getDepositAmount() != null ? operation.getDepositAmount().doubleValue() : null)
                    .setPenaltyAmount(
                            operation.getPenaltyAmount() != null ? operation.getPenaltyAmount().doubleValue() : null)
                    .setLockFee(operation.getLockFee() != null ? operation.getLockFee().doubleValue() : null)
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            eventCoordinator.publishEvent(
            topicRegistry.getTopicName(TopicKey.SMARTLOCK_PAYMENT_EVENTS),
            operation.getDevice().getDeviceCode(),
            event
            );

            logger.info("Refund processed event published: lockCode={}, refundAmount={}",
                    operation.getLockCode(), operation.getRefundAmount());

        } catch (Exception e) {
            logger.error("Failed to publish refund processed event: lockCode={}",
                    operation.getLockCode(), e);
        }
    }

    /**
     * Ödeme başlatma olayını Outbox Pattern ile yayınla (Reliable)
     *
     * Phase 4 Enhancement: Critical payment events require guaranteed delivery
     * - Persists event to outbox table in same transaction as payment initiation
     * - OutboxPoller publishes to Kafka asynchronously
     * - Guarantees exactly-once delivery even if Kafka unavailable
     *
     * @param operation DeviceOperation that initiated payment
     * @throws Exception if outbox save fails (transaction will rollback)
     */
    public void publishPaymentInitiatedEventReliable(DeviceOperation operation) throws Exception {
        try {
            PaymentEvent event = PaymentEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setPaymentId(operation.getPaymentId() != null ? operation.getPaymentId() : "PENDING")
                    .setLockCode(operation.getLockCode())
                    .setDeviceCode(operation.getDevice().getDeviceCode())
                    .setEventType("PaymentInitiated")
                    .setAmount(operation.getTotalAmount() != null ? operation.getTotalAmount().doubleValue() : 0.0)
                    .setCurrency("TRY")
                    .setPreviousStatus(null)
                    .setStatus(operation.getPaymentStatus() != null ? operation.getPaymentStatus().name() : "PENDING")
                    .setUserId(operation.getUser() != null ? operation.getUser().getId().toString() : null)
                    .setPaymentMethod(null)
                    .setFailureReason(null)
                    .setDepositAmount(
                            operation.getDepositAmount() != null ? operation.getDepositAmount().doubleValue() : null)
                    .setPenaltyAmount(
                            operation.getPenaltyAmount() != null ? operation.getPenaltyAmount().doubleValue() : null)
                    .setLockFee(operation.getLockFee() != null ? operation.getLockFee().doubleValue() : null)
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            var outboxEvent = eventCoordinator.publishEventReliable(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_PAYMENT_EVENTS),
                    operation.getUser() != null ? operation.getUser().getId().toString() : "UNKNOWN",
                    event);

            logger.info("Payment initiated event published to outbox (reliable): lockCode={}, amount={}, eventId={}, OutboxId={}",
                    operation.getLockCode(), operation.getTotalAmount(), event.getEventId(), outboxEvent);

        } catch (Exception e) {
            logger.error("Failed to publish payment initiated event to outbox: lockCode={}",
                    operation.getLockCode(), e);
            throw e;
        }
    }

    /**
     * Ödeme başarılı olayını Outbox Pattern ile yayınla (Reliable)
     *
     * Phase 4 Enhancement: Critical payment events require guaranteed delivery
     * - Persists event to outbox table in same transaction as payment completion
     * - OutboxPoller publishes to Kafka asynchronously
     * - Guarantees exactly-once delivery even if Kafka unavailable
     *
     * @param operation DeviceOperation that completed payment
     * @throws Exception if outbox save fails (transaction will rollback)
     */
    public void publishPaymentCompletedEventReliable(DeviceOperation operation) throws Exception {
        try {
            PaymentEvent event = PaymentEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setPaymentId(operation.getPaymentId())
                    .setLockCode(operation.getLockCode())
                    .setDeviceCode(operation.getDevice().getDeviceCode())
                    .setEventType("PaymentCompleted")
                    .setAmount(operation.getTotalAmount() != null ? operation.getTotalAmount().doubleValue() : 0.0)
                    .setCurrency("TRY")
                    .setPreviousStatus("PENDING")
                    .setStatus("COMPLETED")
                    .setUserId(operation.getUser() != null ? operation.getUser().getId().toString() : null)
                    .setPaymentMethod(null)
                    .setFailureReason(null)
                    .setDepositAmount(
                            operation.getDepositAmount() != null ? operation.getDepositAmount().doubleValue() : null)
                    .setPenaltyAmount(
                            operation.getPenaltyAmount() != null ? operation.getPenaltyAmount().doubleValue() : null)
                    .setLockFee(operation.getLockFee() != null ? operation.getLockFee().doubleValue() : null)
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            var outboxEvent = eventCoordinator.publishEventReliable(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_PAYMENT_EVENTS),
                    operation.getUser() != null ? operation.getUser().getId().toString() : "UNKNOWN",
                    event);

            logger.info("Payment completed event published to outbox (reliable): lockCode={}, paymentId={}, OutboxId={}",
                    operation.getLockCode(), operation.getPaymentId(), outboxEvent);

        } catch (Exception e) {
            logger.error("Failed to publish payment completed event to outbox: lockCode={}",
                    operation.getLockCode(), e);
            throw e;
        }
    }

    /**
     * Ödeme başarısız olayını Outbox Pattern ile yayınla (Reliable)
     *
     * Phase 4 Enhancement: Critical payment failure events require guaranteed delivery
     * - Persists event to outbox table in same transaction as payment failure
     * - OutboxPoller publishes to Kafka asynchronously
     * - Guarantees exactly-once delivery even if Kafka unavailable
     * - Important for failure analysis and retry logic
     *
     * @param operation DeviceOperation that failed
     * @param failureReason Reason for payment failure
     * @throws Exception if outbox save fails (transaction will rollback)
     */
    public void publishPaymentFailedEventReliable(DeviceOperation operation, String failureReason) throws Exception {
        try {
            PaymentEvent event = PaymentEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setPaymentId(operation.getPaymentId() != null ? operation.getPaymentId() : "FAILED")
                    .setLockCode(operation.getLockCode())
                    .setDeviceCode(operation.getDevice().getDeviceCode())
                    .setEventType("PaymentFailed")
                    .setAmount(operation.getTotalAmount() != null ? operation.getTotalAmount().doubleValue() : 0.0)
                    .setCurrency("TRY")
                    .setPreviousStatus("PENDING")
                    .setStatus("FAILED")
                    .setUserId(operation.getUser() != null ? operation.getUser().getId().toString() : null)
                    .setPaymentMethod(null)
                    .setFailureReason(failureReason)
                    .setDepositAmount(
                            operation.getDepositAmount() != null ? operation.getDepositAmount().doubleValue() : null)
                    .setPenaltyAmount(
                            operation.getPenaltyAmount() != null ? operation.getPenaltyAmount().doubleValue() : null)
                    .setLockFee(operation.getLockFee() != null ? operation.getLockFee().doubleValue() : null)
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            var outboxEvent = eventCoordinator.publishEventReliable(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_PAYMENT_EVENTS),
                    operation.getUser() != null ? operation.getUser().getId().toString() : "UNKNOWN",
                    event);

            logger.warn("Payment failed event published to outbox (reliable): lockCode={}, reason={}, OutboxId={}",
                    operation.getLockCode(), failureReason, outboxEvent);

        } catch (Exception e) {
            logger.error("Failed to publish payment failed event to outbox: lockCode={}",
                    operation.getLockCode(), e);
            throw e;
        }
    }

    /**
     * Iade işlemi olayını Outbox Pattern ile yayınla (Reliable)
     *
     * Phase 4 Enhancement: Financial refund events require guaranteed delivery
     * - Persists event to outbox table in same transaction as refund processing
     * - OutboxPoller publishes to Kafka asynchronously
     * - Guarantees exactly-once delivery even if Kafka unavailable
     * - Critical for financial reconciliation
     *
     * @param operation DeviceOperation that was refunded
     * @throws Exception if outbox save fails (transaction will rollback)
     */
    public void publishRefundProcessedEventReliable(DeviceOperation operation) throws Exception {
        try {
            PaymentEvent event = PaymentEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setPaymentId(operation.getPaymentId())
                    .setLockCode(operation.getLockCode())
                    .setDeviceCode(operation.getDevice().getDeviceCode())
                    .setEventType("RefundProcessed")
                    .setAmount(operation.getRefundAmount() != null ? operation.getRefundAmount().doubleValue() : 0.0)
                    .setCurrency("TRY")
                    .setPreviousStatus("COMPLETED")
                    .setStatus("REFUNDED")
                    .setUserId(operation.getUser() != null ? operation.getUser().getId().toString() : null)
                    .setPaymentMethod(null)
                    .setFailureReason(null)
                    .setDepositAmount(
                            operation.getDepositAmount() != null ? operation.getDepositAmount().doubleValue() : null)
                    .setPenaltyAmount(
                            operation.getPenaltyAmount() != null ? operation.getPenaltyAmount().doubleValue() : null)
                    .setLockFee(operation.getLockFee() != null ? operation.getLockFee().doubleValue() : null)
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            var outboxEvent = eventCoordinator.publishEventReliable(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_PAYMENT_EVENTS),
                    operation.getUser() != null ? operation.getUser().getId().toString() : "UNKNOWN",
                    event);

            logger.info("Refund processed event published to outbox (reliable): lockCode={}, refundAmount={}, OutboxId={}",
                    operation.getLockCode(), operation.getRefundAmount(), outboxEvent);

        } catch (Exception e) {
            logger.error("Failed to publish refund processed event to outbox: lockCode={}",
                    operation.getLockCode(), e);
            throw e;
        }
    }
}
