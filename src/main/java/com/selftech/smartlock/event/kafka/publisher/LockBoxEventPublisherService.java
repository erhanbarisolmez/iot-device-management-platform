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
import com.selftech.smartlock.models.dto.enums.OperationType;
import java.time.Instant;
import com.selftech.smartlock.models.entity.BoxOperation;
import java.time.Instant;
import com.selftech.smartlock.models.entity.LockBox;
import java.time.Instant;
// import generated Avro class
import com.selftech.smartlock.avro.LockBoxEvent;
import java.time.Instant;

import lombok.RequiredArgsConstructor;
import java.time.Instant;

/**
 * Lock Box Event Publisher Service - PHASE 1 PROFESSIONAL ARCHITECTURE
 *
 * Publishes lock box-related events (creation, status changes, door opens, maintenance)
 * to Kafka using the professional CentralEventCoordinator facade.
 *
 * UPGRADE from FAZE 4.5:
 * - Old: Direct EventProducerService.sendEvent() calls
 * - New: CentralEventCoordinator for unified event management
 * - Benefit: Metadata enrichment, correlation tracking, consistent error handling
 *
 * Event Types Published:
 * - BoxCreated: When a new smart lock box is created
 * - BoxStatusChanged: When box status changes (active, inactive, maintenance, etc)
 * - BoxOpened: When a door is opened (security event)
 * - WeightChanged: When weight changes (anomaly detection trigger)
 * - BoxOperationPerformed: Generic box operation
 *
 * Partition Key Strategy:
 * - Uses boxCode as partition key
 * - All events for same box go to same partition (ordering guarantee)
 * - Enables per-box event sequence processing
 *
 * Event Publishing Features:
 * - Metadata enrichment: CentralEventCoordinator automatically:
 *   - Generates eventId if missing
 *   - Sets correlation ID (from ThreadLocal context)
 *   - Adds timestamp
 *   - Sets event type
 *   - Manages version
 * - Error handling: Exceptions thrown to caller for DLQ routing
 * - Logging: Comprehensive info and error logging
 * - Type-safe: Uses generated Avro LockBoxEvent class
 *
 * @author PHASE 1 Professional Architecture Upgrade
 */
@Service
@RequiredArgsConstructor
public class LockBoxEventPublisherService implements ILockBoxEventPublisher {

    private static final Logger logger = LoggerFactory.getLogger(LockBoxEventPublisherService.class);

    // Professional coordinator (new - PHASE 1)
    private final CentralEventCoordinator eventCoordinator;
    // Configuration
    private final KafkaTopicRegistry topicRegistry;

    @Override
    public String getPublisherName() {
        return "LockBoxEventPublisher";
    }

    @Override
    public boolean isHealthy() {
        return eventCoordinator != null && topicRegistry != null;
    }

    /**
     * Publish box creation event using professional coordinator
     *
     * @param box LockBox entity
     */
    public void publishBoxCreatedEvent(LockBox box) {
        try {
            LockBoxEvent event = LockBoxEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setBoxCode(box.getBoxCode())
                    .setEventType("BoxCreated")
                    .setPreviousStatus(null)
                    .setStatus(box.getStatus().name())
                    .setDoorOpen(box.getDoorOpen() != null ? box.getDoorOpen() : false)
                    .setCurrentWeight(box.getCurrentWeight() != null ? box.getCurrentWeight() : 0.0)
                    .setPreviousWeight(null)
                    .setExpectedWeight(box.getExpectedWeight())
                    .setDeviceCount(box.getDevices() != null ? box.getDevices().size() : 0)
                    .setUserId(null)
                    .setOperationType("CREATE")
                    .setTemperature(null)
                    .setHumidity(null)
                    .setGpsCoordinates(box.getLat() != null && box.getLng() != null
                        ? box.getLat() + "," + box.getLng() : null)
                    .setBatteryLevel(box.getBatteryLevel())
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            // Use CentralEventCoordinator for professional event management
            eventCoordinator.publishEvent(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_BOX_OPERATION),
                    box.getBoxCode(),  // Partition key: boxCode
                    event
            );

            logger.info("Box created event published via coordinator: boxCode={}, eventId={}",
                    box.getBoxCode(), event.getEventId());

        } catch (Exception e) {
            logger.error("Failed to publish box created event: boxCode={}", box.getBoxCode(), e);
            throw new RuntimeException("Failed to publish box created event", e);
        }
    }

    /**
     * Publish box status change event using professional coordinator
     *
     * @param box LockBox entity
     * @param previousStatus Previous box status
     */
    public void publishBoxStatusChangedEvent(LockBox box, String previousStatus) {
        try {
            LockBoxEvent event = LockBoxEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setBoxCode(box.getBoxCode())
                    .setEventType("BoxStatusChanged")
                    .setPreviousStatus(previousStatus)
                    .setStatus(box.getStatus().name())
                    .setDoorOpen(box.getDoorOpen() != null ? box.getDoorOpen() : false)
                    .setCurrentWeight(box.getCurrentWeight() != null ? box.getCurrentWeight() : 0.0)
                    .setPreviousWeight(null)
                    .setExpectedWeight(box.getExpectedWeight())
                    .setDeviceCount(box.getDevices() != null ? box.getDevices().size() : 0)
                    .setUserId(null)
                    .setOperationType("UPDATE")
                    .setTemperature(null)
                    .setHumidity(null)
                    .setGpsCoordinates(box.getLat() != null && box.getLng() != null
                        ? box.getLat() + "," + box.getLng() : null)
                    .setBatteryLevel(box.getBatteryLevel())
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            eventCoordinator.publishEvent(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_BOX_OPERATION),
                    box.getBoxCode(),
                    event
            );

            logger.info("Box status changed event published via coordinator: boxCode={}, {} -> {}",
                    box.getBoxCode(), previousStatus, box.getStatus().name());

        } catch (Exception e) {
            logger.error("Failed to publish box status changed event: boxCode={}", box.getBoxCode(), e);
            throw new RuntimeException("Failed to publish box status changed event", e);
        }
    }

    /**
     * Publish box door open event using professional coordinator
     *
     * @param box LockBox entity
     */
    public void publishBoxOpenedEvent(LockBox box) {
        try {
            LockBoxEvent event = LockBoxEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setBoxCode(box.getBoxCode())
                    .setEventType("BoxOpened")
                    .setPreviousStatus(null)
                    .setStatus(box.getStatus().name())
                    .setDoorOpen(true)
                    .setCurrentWeight(box.getCurrentWeight() != null ? box.getCurrentWeight() : 0.0)
                    .setPreviousWeight(null)
                    .setExpectedWeight(box.getExpectedWeight())
                    .setDeviceCount(box.getDevices() != null ? box.getDevices().size() : 0)
                    .setUserId(null)
                    .setOperationType("OPEN")
                    .setTemperature(null)
                    .setHumidity(null)
                    .setGpsCoordinates(null)
                    .setBatteryLevel(box.getBatteryLevel())
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            eventCoordinator.publishEvent(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_BOX_OPERATION),
                    box.getBoxCode(),
                    event
            );

            logger.info("Box opened event published via coordinator: boxCode={}", box.getBoxCode());

        } catch (Exception e) {
            logger.error("Failed to publish box opened event: boxCode={}", box.getBoxCode(), e);
            throw new RuntimeException("Failed to publish box opened event", e);
        }
    }

    /**
     * Publish box weight change event using professional coordinator (anomaly detection trigger)
     *
     * @param box LockBox entity
     * @param previousWeight Previous weight value
     */
    public void publishBoxWeightChangedEvent(LockBox box, Double previousWeight) {
        try {
            LockBoxEvent event = LockBoxEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setBoxCode(box.getBoxCode())
                    .setEventType("WeightChanged")
                    .setPreviousStatus(null)
                    .setStatus(box.getStatus().name())
                    .setDoorOpen(box.getDoorOpen() != null ? box.getDoorOpen() : false)
                    .setCurrentWeight(box.getCurrentWeight() != null ? box.getCurrentWeight() : 0.0)
                    .setPreviousWeight(previousWeight)
                    .setExpectedWeight(box.getExpectedWeight())
                    .setDeviceCount(box.getDevices() != null ? box.getDevices().size() : 0)
                    .setUserId(null)
                    .setOperationType("WEIGHT_CHANGE")
                    .setTemperature(null)
                    .setHumidity(null)
                    .setGpsCoordinates(null)
                    .setBatteryLevel(box.getBatteryLevel())
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            eventCoordinator.publishEvent(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_BOX_OPERATION),
                    box.getBoxCode(),
                    event
            );

            logger.info("Box weight changed event published via coordinator: boxCode={}, {} kg -> {} kg",
                    box.getBoxCode(), previousWeight, box.getCurrentWeight());

        } catch (Exception e) {
            logger.error("Failed to publish box weight changed event: boxCode={}", box.getBoxCode(), e);
            throw new RuntimeException("Failed to publish box weight changed event", e);
        }
    }

    /**
     * Publish generic box operation event using professional coordinator
     *
     * @param boxOp BoxOperation entity
     */
    public void publishBoxOperationEvent(BoxOperation boxOp) {
        try {
            LockBox box = boxOp.getLockBox();

            LockBoxEvent event = LockBoxEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setBoxCode(box.getBoxCode())
                    .setEventType(mapOperationType(boxOp.getOperationType()))
                    .setPreviousStatus(boxOp.getPreviousStatus())
                    .setStatus(boxOp.getNewStatus())
                    .setDoorOpen(box.getDoorOpen() != null ? box.getDoorOpen() : false)
                    .setCurrentWeight(box.getCurrentWeight() != null ? box.getCurrentWeight() : 0.0)
                    .setPreviousWeight(null)
                    .setExpectedWeight(box.getExpectedWeight())
                    .setDeviceCount(box.getDevices() != null ? box.getDevices().size() : 0)
                    .setUserId(boxOp.getUser() != null ? boxOp.getUser().getId().toString() : null)
                    .setOperationType(boxOp.getOperationType().name())
                    .setTemperature(null)
                    .setHumidity(null)
                    .setGpsCoordinates(null)
                    .setBatteryLevel(box.getBatteryLevel())
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            eventCoordinator.publishEvent(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_BOX_OPERATION),
                    box.getBoxCode(),
                    event
            );

            logger.info("Box operation event published via coordinator: boxCode={}, operation={}, eventId={}",
                    box.getBoxCode(), boxOp.getOperationType(), event.getEventId());

        } catch (Exception e) {
            logger.error("Failed to publish box operation event: boxId={}",
                    boxOp.getLockBox().getId(), e);
            throw new RuntimeException("Failed to publish box operation event", e);
        }
    }

    /**
     * Publish box creation event with Outbox Pattern (Reliable)
     *
     * Phase 4 Enhancement: Critical events require guaranteed delivery
     * - Persists event to outbox table in same transaction as box creation
     * - OutboxPoller publishes to Kafka asynchronously
     * - Guarantees exactly-once delivery even if Kafka unavailable
     *
     * @param box LockBox entity
     * @throws Exception if outbox save fails (transaction will rollback)
     */
    public void publishBoxCreatedEventReliable(LockBox box) throws Exception {
        try {
            LockBoxEvent event = LockBoxEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setBoxCode(box.getBoxCode())
                    .setEventType("BoxCreated")
                    .setPreviousStatus(null)
                    .setStatus(box.getStatus().name())
                    .setDoorOpen(box.getDoorOpen() != null ? box.getDoorOpen() : false)
                    .setCurrentWeight(box.getCurrentWeight() != null ? box.getCurrentWeight() : 0.0)
                    .setPreviousWeight(null)
                    .setExpectedWeight(box.getExpectedWeight())
                    .setDeviceCount(box.getDevices() != null ? box.getDevices().size() : 0)
                    .setUserId(null)
                    .setOperationType("CREATE")
                    .setTemperature(null)
                    .setHumidity(null)
                    .setGpsCoordinates(box.getLat() != null && box.getLng() != null
                        ? box.getLat() + "," + box.getLng() : null)
                    .setBatteryLevel(box.getBatteryLevel())
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            // Use publishEventReliable for Outbox Pattern
            var outboxEvent = eventCoordinator.publishEventReliable(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_BOX_OPERATION),
                    box.getBoxCode(),
                    event
            );

            logger.info("Box created event published to outbox (reliable) via coordinator: boxCode={}, eventId={}, OutboxId={}",
                    box.getBoxCode(), event.getEventId(), outboxEvent);

        } catch (Exception e) {
            logger.error("Failed to publish box created event to outbox: boxCode={}", box.getBoxCode(), e);
            throw e;
        }
    }

    /**
     * Publish box status change event with Outbox Pattern (Reliable)
     *
     * Phase 4 Enhancement: Critical events require guaranteed delivery
     * - Persists event to outbox table in same transaction as box status update
     * - OutboxPoller publishes to Kafka asynchronously
     * - Guarantees exactly-once delivery even if Kafka unavailable
     *
     * @param box LockBox entity
     * @param previousStatus Previous box status
     * @throws Exception if outbox save fails (transaction will rollback)
     */
    public void publishBoxStatusChangedEventReliable(LockBox box, String previousStatus) throws Exception {
        try {
            LockBoxEvent event = LockBoxEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setBoxCode(box.getBoxCode())
                    .setEventType("BoxStatusChanged")
                    .setPreviousStatus(previousStatus)
                    .setStatus(box.getStatus().name())
                    .setDoorOpen(box.getDoorOpen() != null ? box.getDoorOpen() : false)
                    .setCurrentWeight(box.getCurrentWeight() != null ? box.getCurrentWeight() : 0.0)
                    .setPreviousWeight(null)
                    .setExpectedWeight(box.getExpectedWeight())
                    .setDeviceCount(box.getDevices() != null ? box.getDevices().size() : 0)
                    .setUserId(null)
                    .setOperationType("UPDATE")
                    .setTemperature(null)
                    .setHumidity(null)
                    .setGpsCoordinates(box.getLat() != null && box.getLng() != null
                        ? box.getLat() + "," + box.getLng() : null)
                    .setBatteryLevel(box.getBatteryLevel())
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            var outboxEvent = eventCoordinator.publishEventReliable(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_BOX_OPERATION),
                    box.getBoxCode(),
                    event
            );

            logger.info("Box status changed event published to outbox (reliable) via coordinator: boxCode={}, {} -> {}, OutboxId={}",
                    box.getBoxCode(), previousStatus, box.getStatus().name(), outboxEvent);

        } catch (Exception e) {
            logger.error("Failed to publish box status changed event to outbox: boxCode={}", box.getBoxCode(), e);
            throw e;
        }
    }

    /**
     * Publish box door open event with Outbox Pattern (Reliable)
     *
     * Phase 4 Enhancement: Security-critical events require guaranteed delivery
     * - Persists event to outbox table in same transaction as box open
     * - OutboxPoller publishes to Kafka asynchronously
     * - Guarantees exactly-once delivery even if Kafka unavailable
     *
     * @param box LockBox entity
     * @throws Exception if outbox save fails (transaction will rollback)
     */
    public void publishBoxOpenedEventReliable(LockBox box) throws Exception {
        try {
            LockBoxEvent event = LockBoxEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setBoxCode(box.getBoxCode())
                    .setEventType("BoxOpened")
                    .setPreviousStatus(null)
                    .setStatus(box.getStatus().name())
                    .setDoorOpen(true)
                    .setCurrentWeight(box.getCurrentWeight() != null ? box.getCurrentWeight() : 0.0)
                    .setPreviousWeight(null)
                    .setExpectedWeight(box.getExpectedWeight())
                    .setDeviceCount(box.getDevices() != null ? box.getDevices().size() : 0)
                    .setUserId(null)
                    .setOperationType("OPEN")
                    .setTemperature(null)
                    .setHumidity(null)
                    .setGpsCoordinates(null)
                    .setBatteryLevel(box.getBatteryLevel())
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            var outboxEvent = eventCoordinator.publishEventReliable(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_BOX_OPERATION),
                    box.getBoxCode(),
                    event
            );

            logger.info("Box opened event published to outbox (reliable) via coordinator: boxCode={}, OutboxId={}",
                    box.getBoxCode(), outboxEvent);

        } catch (Exception e) {
            logger.error("Failed to publish box opened event to outbox: boxCode={}", box.getBoxCode(), e);
            throw e;
        }
    }

    /**
     * Publish box weight change event with Outbox Pattern (Reliable - Anomaly Detection Trigger)
     *
     * Phase 4 Enhancement: Anomaly detection events require guaranteed delivery
     * - Persists event to outbox table in same transaction as weight update
     * - OutboxPoller publishes to Kafka asynchronously
     * - Guarantees exactly-once delivery even if Kafka unavailable
     *
     * @param box LockBox entity
     * @param previousWeight Previous weight value
     * @throws Exception if outbox save fails (transaction will rollback)
     */
    public void publishBoxWeightChangedEventReliable(LockBox box, Double previousWeight) throws Exception {
        try {
            LockBoxEvent event = LockBoxEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setBoxCode(box.getBoxCode())
                    .setEventType("WeightChanged")
                    .setPreviousStatus(null)
                    .setStatus(box.getStatus().name())
                    .setDoorOpen(box.getDoorOpen() != null ? box.getDoorOpen() : false)
                    .setCurrentWeight(box.getCurrentWeight() != null ? box.getCurrentWeight() : 0.0)
                    .setPreviousWeight(previousWeight)
                    .setExpectedWeight(box.getExpectedWeight())
                    .setDeviceCount(box.getDevices() != null ? box.getDevices().size() : 0)
                    .setUserId(null)
                    .setOperationType("WEIGHT_CHANGE")
                    .setTemperature(null)
                    .setHumidity(null)
                    .setGpsCoordinates(null)
                    .setBatteryLevel(box.getBatteryLevel())
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            var outboxEvent = eventCoordinator.publishEventReliable(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_BOX_OPERATION),
                    box.getBoxCode(),
                    event
            );

            logger.info("Box weight changed event published to outbox (reliable) via coordinator: boxCode={}, {} kg -> {} kg, OutboxId={}",
                    box.getBoxCode(), previousWeight, box.getCurrentWeight(), outboxEvent);

        } catch (Exception e) {
            logger.error("Failed to publish box weight changed event to outbox: boxCode={}", box.getBoxCode(), e);
            throw e;
        }
    }

    /**
     * Publish generic box operation event with Outbox Pattern (Reliable)
     *
     * Phase 4 Enhancement: All box operations require guaranteed delivery
     * - Persists event to outbox table in same transaction as box operation
     * - OutboxPoller publishes to Kafka asynchronously
     * - Guarantees exactly-once delivery even if Kafka unavailable
     *
     * @param boxOp BoxOperation entity
     * @throws Exception if outbox save fails (transaction will rollback)
     */
    public void publishBoxOperationEventReliable(BoxOperation boxOp) throws Exception {
        try {
            LockBox box = boxOp.getLockBox();

            LockBoxEvent event = LockBoxEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setBoxCode(box.getBoxCode())
                    .setEventType(mapOperationType(boxOp.getOperationType()))
                    .setPreviousStatus(boxOp.getPreviousStatus())
                    .setStatus(boxOp.getNewStatus())
                    .setDoorOpen(box.getDoorOpen() != null ? box.getDoorOpen() : false)
                    .setCurrentWeight(box.getCurrentWeight() != null ? box.getCurrentWeight() : 0.0)
                    .setPreviousWeight(null)
                    .setExpectedWeight(box.getExpectedWeight())
                    .setDeviceCount(box.getDevices() != null ? box.getDevices().size() : 0)
                    .setUserId(boxOp.getUser() != null ? boxOp.getUser().getId().toString() : null)
                    .setOperationType(boxOp.getOperationType().name())
                    .setTemperature(null)
                    .setHumidity(null)
                    .setGpsCoordinates(null)
                    .setBatteryLevel(box.getBatteryLevel())
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            var outboxEvent = eventCoordinator.publishEventReliable(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_BOX_OPERATION),
                    box.getBoxCode(),
                    event
            );

            logger.info("Box operation event published to outbox (reliable) via coordinator: boxCode={}, operation={}, eventId={}, OutboxId={}",
                    box.getBoxCode(), boxOp.getOperationType(), event.getEventId(), outboxEvent);

        } catch (Exception e) {
            logger.error("Failed to publish box operation event to outbox: boxId={}",
                    boxOp.getLockBox().getId(), e);
            throw e;
        }
    }

    /**
     * OperationType'ı event type'a çevir
     */
    private String mapOperationType(OperationType opType) {
        return switch (opType) {
            case CREATE_DEVICE -> "BoxCreated";
            case STATUS_CHANGE -> "BoxStatusChanged";
            case MAINTENANCE_START -> "MaintenanceStarted";
            case MAINTENANCE_END -> "MaintenanceCompleted";
            case GENERATE_OPENING_CODE -> "OpeningCodeGenerated";
            case RETURN_TO_BOX -> "DeviceReturnedToBox";
            case TAKE_FROM_BOX -> "DeviceTakenFromBox";
            case LOCK_DEVICE, UNLOCK_DEVICE -> "BoxOpened";
            default -> "BoxOperationPerformed";
        };
    }
}
