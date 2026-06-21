package com.selftech.smartlock.event.kafka.publisher;

import java.time.Instant;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.selftech.kafka.config.KafkaTopicRegistry;
import com.selftech.kafka.config.TopicKey;
import com.selftech.kafka.core.publisher.CentralEventCoordinator;
import com.selftech.kafka.core.publisher.CentralEventCoordinator.EventPublishingException;
import com.selftech.kafka.models.avro.SmsEvent;
import com.selftech.smartlock.models.entity.SmsLog;

import lombok.RequiredArgsConstructor;

/**
 * SMS Event Publisher Service - PHASE 1 PROFESSIONAL ARCHITECTURE
 *
 * Publishes SMS-related events using CentralEventCoordinator.
 * SMS sent/failed events are tracked for audit and monitoring.
 *
 * UPGRADE from FAZE 4.5:
 * - Old: Direct EventProducerService calls
 * - New: CentralEventCoordinator for unified event management
 */
@Service
@RequiredArgsConstructor
public class SmsEventPublisherService {

  private static final Logger logger = LoggerFactory.getLogger(SmsEventPublisherService.class);
  private final CentralEventCoordinator eventCoordinator;
  private final KafkaTopicRegistry topicRegistry;

  /**
   * Publish SMS status event to Kafka when SMS is sent or fails.
   *
   * Follows project-wide event publishing pattern:
   * - eventId: UUID for idempotency and distributed tracing
   * - timestamp: System.currentTimeMillis() for Avro long compatibility
   * - correlationId: null (managed by CentralEventCoordinator ThreadLocal context)
   * - eventType: Indicates SMS lifecycle stage (SmsSent, SmsDelivered, SmsFailed)
   *
   * @param smsLog The saved SMS log entity containing all details.
   * @throws RuntimeException If publishing fails
   */
  public void publishSmsStatusEvent(SmsLog smsLog) {
    try {
      SmsEvent event = SmsEvent.newBuilder()
          .setEventId(UUID.randomUUID().toString())                    // ✅ UUID for event uniqueness
          .setOperationId(smsLog.getLockOperation().getId())
          .setLockCode(smsLog.getLockOperation().getLockCode())
          .setPhoneNumber(smsLog.getPhoneNumber())
          .setMessage(smsLog.getMessage())
          .setStatus(smsLog.getStatus().name())
          .setProviderMessageId(smsLog.getProviderMessageId())
          .setTimestamp(Instant.ofEpochMilli(System.currentTimeMillis())) // ✅ Instant from epoch millis
          .setCorrelationId(null)                                      // ✅ Coordinator manages
          .setEventType("SmsSent")                                     // ✅ Event lifecycle type
          .setSchemaVersion("1.0")                                     // ✅ Schema versioning
          .build();

      eventCoordinator.publishEventAsync(
          topicRegistry.getTopicName(TopicKey.SMARTLOCK_SMS_EVENTS),
          event.getLockCode().toString(),                              // Partition key
          event);

      logger.info("SMS event published via coordinator - lockCode: {}, eventId: {}, status: {}",
          event.getLockCode(), event.getEventId(), event.getStatus());

    } catch (EventPublishingException e) {
      logger.error("Failed to publish SMS event - lockCode: {}, error: {}",
          smsLog.getLockOperation().getLockCode(), e.getMessage(), e);
      throw new RuntimeException("Failed to publish SMS event", e);
    }
  }
}
