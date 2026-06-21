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
import com.selftech.smartlock.models.entity.DeviceOperation;
import java.time.Instant;
import com.selftech.smartlock.models.entity.LockDevice;
import java.time.Instant;
// import generated Avro class
import com.selftech.smartlock.avro.LockDeviceEvent;
import java.time.Instant;

import lombok.RequiredArgsConstructor;
import java.time.Instant;

/**
 * Lock Device Event Publisher Service - PHASE 1 PROFESSIONAL ARCHITECTURE
 *
 * Publishes lock device-related events using CentralEventCoordinator.
 * Devices creation, status changes, assignment to boxes, locking/unlocking
 * are published as events. Uses deviceCode as partition key.
 *
 * UPGRADE from FAZE 4.5:
 * - Old: Direct EventProducerService calls
 * - New: CentralEventCoordinator for unified event management
 */
@Service
@RequiredArgsConstructor
public class LockDeviceEventPublisherService implements ILockDeviceEventPublisher {

    private static final Logger logger = LoggerFactory.getLogger(LockDeviceEventPublisherService.class);
    private final CentralEventCoordinator eventCoordinator;
    private final KafkaTopicRegistry topicRegistry;

    @Override
    public String getPublisherName() {
        return "LockDeviceEventPublisher";
    }

    @Override
    public boolean isHealthy() {
        return eventCoordinator != null && topicRegistry != null;
    }

    /**
     * Cihaz oluşturma olayını yayınla
     */
    public void publishDeviceCreatedEvent(LockDevice device) {
        try {
            LockDeviceEvent event = LockDeviceEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setDeviceCode(device.getDeviceCode())
                    .setEventType("DeviceCreated")
                    .setPreviousStatus(null)
                    .setStatus(device.getStatus().name())
                    .setBoxCode(device.getCurrentBox() != null ? device.getCurrentBox().getBoxCode() : null)
                    .setLocation(device.getCurrentBox() != null ? device.getCurrentBox().getLocation() : null)
                    .setUserId(null)
                    .setVehicleId(device.getCurrentUser() != null ? device.getCurrentUser().getId().toString() : null)
                    .setBatteryLevel(null)
                    .setSignalStrength(null)
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            eventCoordinator.publishEvent(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_DEVICE_OPERATIONS),
                    device.getDeviceCode(),
                    event);

            logger.info("Device created event published: deviceCode={}, eventId={}",
                    device.getDeviceCode(), event.getEventId());

        } catch (Exception e) {
            logger.error("Failed to publish device created event: deviceCode={}", device.getDeviceCode(), e);
            throw new RuntimeException("Failed to publish device created event", e);
        }
    }

    /**
     * Cihaz durumu değişim olayını yayınla
     */
    public void publishDeviceStatusChangedEvent(LockDevice device, String previousStatus) {
        try {
            LockDeviceEvent event = LockDeviceEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setDeviceCode(device.getDeviceCode())
                    .setEventType("DeviceStatusChanged")
                    .setPreviousStatus(previousStatus)
                    .setStatus(device.getStatus().name())
                    .setBoxCode(device.getCurrentBox() != null ? device.getCurrentBox().getBoxCode() : null)
                    .setLocation(device.getCurrentBox() != null ? device.getCurrentBox().getLocation() : null)
                    .setUserId(null)
                    .setVehicleId(device.getCurrentUser() != null ? device.getCurrentUser().getId().toString() : null)
                    .setBatteryLevel(null)
                    .setSignalStrength(null)
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            eventCoordinator.publishEvent(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_DEVICE_OPERATIONS),
                    device.getDeviceCode(),
                    event);

            logger.info("Device status changed event published: deviceCode={}, {} -> {}",
                    device.getDeviceCode(), previousStatus, device.getStatus().name());

        } catch (Exception e) {
            logger.error("Failed to publish device status changed event: deviceCode={}", device.getDeviceCode(), e);
            throw new RuntimeException("Failed to publish device status changed event", e);
        }
    }

    /**
     * Cihazın kutuya atanması olayını yayınla
     */
    public void publishDeviceAssignedToBoxEvent(LockDevice device) {
        try {
            LockDeviceEvent event = LockDeviceEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setDeviceCode(device.getDeviceCode())
                    .setEventType("DeviceAssignedToBox")
                    .setPreviousStatus(null)
                    .setStatus(device.getStatus().name())
                    .setBoxCode(device.getCurrentBox() != null ? device.getCurrentBox().getBoxCode() : null)
                    .setLocation(device.getCurrentBox() != null ? device.getCurrentBox().getLocation() : null)
                    .setUserId(null)
                    .setVehicleId(null)
                    .setBatteryLevel(null)
                    .setSignalStrength(null)
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            eventCoordinator.publishEvent(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_DEVICE_OPERATIONS),
                    device.getDeviceCode(),
                    event);

            logger.info("Device assigned to box event published: deviceCode={}, boxCode={}",
                    device.getDeviceCode(), device.getCurrentBox().getBoxCode());

        } catch (Exception e) {
            logger.error("Failed to publish device assigned event: deviceCode={}", device.getDeviceCode(), e);
            throw new RuntimeException("Failed to publish device assigned event", e);
        }
    }

    /**
     * Cihazın kutudan çıkarılması olayını yayınla
     */
    public void publishDeviceRemovedFromBoxEvent(LockDevice device, String boxCode) {
        try {
            LockDeviceEvent event = LockDeviceEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setDeviceCode(device.getDeviceCode())
                    .setEventType("DeviceRemovedFromBox")
                    .setPreviousStatus(null)
                    .setStatus(device.getStatus().name())
                    .setBoxCode(boxCode)
                    .setLocation(null)
                    .setUserId(device.getCurrentUser() != null ? device.getCurrentUser().getId().toString() : null)
                    .setVehicleId(device.getCurrentUser() != null ? device.getCurrentUser().getId().toString() : null)
                    .setBatteryLevel(null)
                    .setSignalStrength(null)
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            eventCoordinator.publishEvent(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_DEVICE_OPERATIONS),
                    device.getDeviceCode(),
                    event);

            logger.info("Device removed from box event published: deviceCode={}, boxCode={}",
                    device.getDeviceCode(), boxCode);

        } catch (Exception e) {
            logger.error("Failed to publish device removed event: deviceCode={}", device.getDeviceCode(), e);
            throw new RuntimeException("Failed to publish device removed event", e);
        }
    }

    /**
     * DeviceOperation entity'sinden event oluştur ve yayınla
     */
    public void publishDeviceOperationEvent(DeviceOperation deviceOp) {
        try {
            LockDevice device = deviceOp.getDevice();
            String boxCode = device.getCurrentBox() != null ? device.getCurrentBox().getBoxCode() : null;

            LockDeviceEvent event = LockDeviceEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setDeviceCode(device.getDeviceCode())
                    .setEventType(mapOperationType(deviceOp.getOperationType()))
                    .setPreviousStatus(null)
                    .setStatus(device.getStatus().name())
                    .setBoxCode(boxCode)
                    .setLocation(device.getCurrentBox() != null ? device.getCurrentBox().getLocation() : null)
                    .setUserId(deviceOp.getUser() != null ? deviceOp.getUser().getId().toString() : null)
                    .setVehicleId(deviceOp.getVehicle() != null ? deviceOp.getVehicle().getId().toString() : null)
                    .setBatteryLevel(null)
                    .setSignalStrength(null)
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            eventCoordinator.publishEvent(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_DEVICE_OPERATIONS),
                    device.getDeviceCode(),
                    event);

            logger.info("Device operation event published: deviceCode={}, operation={}, eventId={}",
                    device.getDeviceCode(), deviceOp.getOperationType(), event.getEventId());

        } catch (Exception e) {
            logger.error("Failed to publish device operation event: deviceId={}",
                    deviceOp.getDevice().getId(), e);
            throw new RuntimeException("Failed to publish device operation event", e);
        }
    }

    /**
     * Publish device creation event with Outbox Pattern (Reliable)
     *
     * Phase 4.8 Enhancement: Critical device events require guaranteed delivery
     * - Persists event to outbox table in same transaction as device creation
     * - OutboxPoller publishes to Kafka asynchronously
     * - Guarantees exactly-once delivery even if Kafka unavailable
     *
     * @param device LockDevice entity representing created device
     * @throws Exception if outbox save fails (transaction will rollback)
     */
    @Override
    public void publishDeviceCreatedEventReliable(LockDevice device) throws Exception {
        try {
            LockDeviceEvent event = LockDeviceEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setDeviceCode(device.getDeviceCode())
                    .setEventType("DeviceCreated")
                    .setPreviousStatus(null)
                    .setStatus(device.getStatus().name())
                    .setBoxCode(device.getCurrentBox() != null ? device.getCurrentBox().getBoxCode() : null)
                    .setLocation(device.getCurrentBox() != null ? device.getCurrentBox().getLocation() : null)
                    .setUserId(null)
                    .setVehicleId(device.getCurrentUser() != null ? device.getCurrentUser().getId().toString() : null)
                    .setBatteryLevel(null)
                    .setSignalStrength(null)
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            var outboxEvent = eventCoordinator.publishEventReliable(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_DEVICE_OPERATIONS),
                    device.getDeviceCode(),
                    event);

            logger.info("Device created event published to outbox (reliable): deviceCode={}, eventId={}, OutboxId={}",
                    device.getDeviceCode(), event.getEventId(), outboxEvent);

        } catch (Exception e) {
            logger.error("Failed to publish device created event to outbox: deviceCode={}", device.getDeviceCode(), e);
            throw e;
        }
    }

    /**
     * Publish device status change event with Outbox Pattern (Reliable)
     *
     * Phase 4.8 Enhancement: Critical status changes require guaranteed delivery
     * - Persists event to outbox table in same transaction as status update
     * - OutboxPoller publishes to Kafka asynchronously
     * - Guarantees exactly-once delivery even if Kafka unavailable
     *
     * @param device LockDevice entity with new status
     * @param previousStatus Previous device status (as string)
     * @throws Exception if outbox save fails (transaction will rollback)
     */
    @Override
    public void publishDeviceStatusChangedEventReliable(LockDevice device, String previousStatus) throws Exception {
        try {
            LockDeviceEvent event = LockDeviceEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setDeviceCode(device.getDeviceCode())
                    .setEventType("DeviceStatusChanged")
                    .setPreviousStatus(previousStatus)
                    .setStatus(device.getStatus().name())
                    .setBoxCode(device.getCurrentBox() != null ? device.getCurrentBox().getBoxCode() : null)
                    .setLocation(device.getCurrentBox() != null ? device.getCurrentBox().getLocation() : null)
                    .setUserId(null)
                    .setVehicleId(device.getCurrentUser() != null ? device.getCurrentUser().getId().toString() : null)
                    .setBatteryLevel(null)
                    .setSignalStrength(null)
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            var outboxEvent = eventCoordinator.publishEventReliable(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_DEVICE_OPERATIONS),
                    device.getDeviceCode(),
                    event);

            logger.info("Device status changed event published to outbox (reliable): deviceCode={}, {} -> {}, OutboxId={}",
                    device.getDeviceCode(), previousStatus, device.getStatus().name(), outboxEvent);

        } catch (Exception e) {
            logger.error("Failed to publish device status changed event to outbox: deviceCode={}", device.getDeviceCode(), e);
            throw e;
        }
    }

    /**
     * Publish device operation event with Outbox Pattern (Reliable)
     *
     * Phase 4.8 Enhancement: Device operations require guaranteed delivery
     * - Persists event to outbox table in same transaction as operation
     * - OutboxPoller publishes to Kafka asynchronously
     * - Guarantees exactly-once delivery even if Kafka unavailable
     *
     * @param deviceOperation DeviceOperation entity
     * @throws Exception if outbox save fails (transaction will rollback)
     */
    @Override
    public void publishDeviceOperationEventReliable(DeviceOperation deviceOperation) throws Exception {
        try {
            LockDevice device = deviceOperation.getDevice();
            String boxCode = device.getCurrentBox() != null ? device.getCurrentBox().getBoxCode() : null;

            LockDeviceEvent event = LockDeviceEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setDeviceCode(device.getDeviceCode())
                    .setEventType(mapOperationType(deviceOperation.getOperationType()))
                    .setPreviousStatus(null)
                    .setStatus(device.getStatus().name())
                    .setBoxCode(boxCode)
                    .setLocation(device.getCurrentBox() != null ? device.getCurrentBox().getLocation() : null)
                    .setUserId(deviceOperation.getUser() != null ? deviceOperation.getUser().getId().toString() : null)
                    .setVehicleId(deviceOperation.getVehicle() != null ? deviceOperation.getVehicle().getId().toString() : null)
                    .setBatteryLevel(null)
                    .setSignalStrength(null)
                    .setTimestamp(Instant.now())
                    .setCorrelationId(null)
                    .setSchemaVersion("1.0")
                    .setMetadata(null)
                    .build();

            var outboxEvent = eventCoordinator.publishEventReliable(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_DEVICE_OPERATIONS),
                    device.getDeviceCode(),
                    event);

            logger.info("Device operation event published to outbox (reliable): deviceCode={}, operation={}, eventId={}, OutboxId={}",
                    device.getDeviceCode(), deviceOperation.getOperationType(), event.getEventId(), outboxEvent);

        } catch (Exception e) {
            logger.error("Failed to publish device operation event to outbox: deviceId={}",
                    deviceOperation.getDevice().getId(), e);
            throw e;
        }
    }

    /**
     * OperationType'ı event type'a çevir
     */
    private String mapOperationType(OperationType opType) {
        return switch (opType) {
            case CREATE_DEVICE -> "DeviceCreated";
            case LOCK_DEVICE -> "DeviceLocked";
            case UNLOCK_DEVICE -> "DeviceUnlocked";
            case RETURN_TO_BOX -> "DeviceReturnedToBox";
            case TAKE_FROM_BOX -> "DeviceTakenFromBox";
            case STATUS_CHANGE -> "DeviceStatusChanged";
            case MAINTENANCE_START -> "MaintenanceStarted";
            case MAINTENANCE_END -> "MaintenanceCompleted";
            case GENERATE_OPENING_CODE -> "OpeningCodeGenerated";
            default -> "DeviceOperationPerformed";
        };
    }
}
