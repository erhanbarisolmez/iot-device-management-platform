package com.selftech.smartlock.event.kafka.publisher;

import java.util.UUID;
import java.time.Instant;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.time.Instant;

import org.slf4j.Logger;
import java.time.Instant;
import org.slf4j.LoggerFactory;
import java.time.Instant;
import org.springframework.kafka.support.SendResult;
import java.time.Instant;
import org.springframework.stereotype.Service;
import java.time.Instant;

import com.selftech.kafka.config.KafkaTopicRegistry;
import java.time.Instant;
import com.selftech.kafka.config.TopicKey;
import java.time.Instant;
import com.selftech.kafka.core.publisher.CentralEventCoordinator;
import java.time.Instant;
import com.selftech.kafka.models.avro.SensorDataEvent;
import java.time.Instant;

import lombok.RequiredArgsConstructor;
import java.time.Instant;

/**
 * Sensor Data Event Publisher Service - PHASE 1 PROFESSIONAL ARCHITECTURE
 *
 * Professional IoT sensor event publishing service for MQTT-sourced real-time data.
 * Uses CentralEventCoordinator as unified event management facade instead of direct
 * EventProducerService calls.
 *
 * Responsibilities:
 * 1. Convert sensor data (from MQTT or HTTP) to SensorDataEvent Avro objects
 * 2. Enrich event metadata (eventId, correlation tracking, timestamps)
 * 3. Delegate publishing to CentralEventCoordinator for unified handling
 * 4. Provide domain-specific methods for different sensor event types
 * 5. Handle errors with comprehensive logging
 * 6. Support partition key routing by device code/ID
 *
 * Features:
 * - Thread-safe: CentralEventCoordinator is thread-safe
 * - Real-time publishing: Direct Kafka publish (not outbox-based) for IoT data
 * - Correlation tracking: Supports distributed tracing via correlation ID
 * - Metadata enrichment: CentralEventCoordinator adds eventId, timestamps, etc.
 * - Async-ready: CentralEventCoordinator.publishEventAsync() for non-blocking
 * - Type-safe: Uses generated Avro SensorDataEvent class
 *
 * Integration Points:
 * - MqttToKafkaBridgeService: Forwards sensor data to this publisher
 * - CentralEventCoordinator: Unified event publishing facade
 * - KafkaTopicConfig: Topic name configuration
 *
 * Event Flow (Professional Architecture):
 * ```
 * MQTT Message
 *     ↓
 * MQTTConfig.sensorDataHandler()
 *     ↓
 * MqttToKafkaBridgeService.forwardSensorDataToKafka()
 *     ↓
 * SensorDataEventPublisherService.publishSensorData() ← YOU ARE HERE
 *     ↓
 * CentralEventCoordinator.publishEvent()
 *     ↓
 * EventProducerService.sendEvent() [low-level]
 *     ↓
 * Kafka Topic: smartlock.mqtt.sensorData.v0
 *     ↓
 * CentralEventConsumer
 *     ↓
 * EventHandler (if registered)
 * ```
 *
 * Example Usage:
 * ```
 * @Service
 * public class SensorDataProcessor {
 *     @Autowired
 *     private SensorDataEventPublisherService publisherService;
 *
 *     public void processSensorReading(SensorDTO sensor) {
 *         // Direct publish method
 *         publisherService.publishSensorData(
 *             sensor.getDeviceCode(),
 *             sensor.getBattery(),
 *             sensor.getWeight(),
 *             sensor.getTemperature()
 *         );
 *
 *         // Builder pattern for complex sensor data
 *         SensorDataEvent event = SensorDataEvent.newBuilder()
 *             .setEventId(UUID.randomUUID().toString())
 *             .setBatteryLevel(sensor.getBattery())
 *             .setWeight(sensor.getWeight())
 *             .setTemperature(sensor.getTemperature())
 *             .setTimestamp(Instant.now())
 *             .build();
 *         publisherService.publishSensorDataEvent(event, sensor.getDeviceCode());
 *     }
 * }
 * ```
 *
 * Why CentralEventCoordinator Instead of EventProducerService?
 *
 * Old Approach (EventProducerService direct):
 * - No metadata enrichment (missing eventId generation)
 * - No correlation ID tracking (can't trace distributed requests)
 * - No timestamp management (manual timestamping needed)
 * - Scattered error handling (not unified)
 * - Hard to add cross-cutting concerns (metrics, audit)
 *
 * New Approach (CentralEventCoordinator):
 * ✅ Automatic metadata enrichment (eventId, correlationId, timestamp)
 * ✅ Unified event handling (single entry point for all events)
 * ✅ Consistent error handling (DLQ routing, exception categorization)
 * ✅ Easy to add features (metrics, tracing, audit - in coordinator)
 * ✅ Transaction support (@Transactional when needed)
 * ✅ Async publishing support (CompletableFuture)
 *
 * Future Enhancements:
 * - publishSensorDataAsync() for non-blocking operations
 * - publishBulkSensorData() for batch processing
 * - Metrics integration (event rate, latency)
 * - Anomaly detection integration (forward high-priority events to anomaly topic)
 */
@Service
@RequiredArgsConstructor
public class SensorDataEventPublisherService {

    private static final Logger logger = LoggerFactory.getLogger(SensorDataEventPublisherService.class);

    // Dependencies injected by Spring
    private final CentralEventCoordinator eventCoordinator;
    private final KafkaTopicRegistry topicRegistry;

    // ========================================
    // Direct Publishing Methods (Simple Cases)
    // ========================================

    /**
     * Publish sensor data with common fields.
     * Useful for typical MQTT sensor readings with basic metrics.
     *
     * @param deviceCode Device/box code (used as partition key)
     * @param batteryLevel Battery percentage (0-100)
     * @param weight Current weight in kg
     * @param temperature Temperature in Celsius
     */
    public void publishSensorData(String deviceCode, Double batteryLevel, Double weight, Double temperature) {
        try {
            SensorDataEvent event = SensorDataEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setBatteryLevel(batteryLevel)
                    .setWeight(weight)
                    .setTemperature(temperature)
                    .setTimestamp(java.time.Instant.ofEpochMilli(System.currentTimeMillis()))
                    .build();

            publishSensorDataEvent(event, deviceCode);

        } catch (Exception e) {
            logger.error("Failed to publish sensor data for device: {}", deviceCode, e);
            throw new RuntimeException("Failed to publish sensor data: " + e.getMessage(), e);
        }
    }

    /**
     * Publish sensor data with full details.
     * For comprehensive sensor readings including door status, GPS, humidity, etc.
     *
     * @param deviceCode Device/box code (partition key)
     * @param batteryLevel Battery level (0-100)
     * @param doorOpen Door open status
     * @param weight Current weight
     * @param temperature Temperature
     * @param humidity Humidity percentage
     * @param gpsCoordinates GPS coordinates as "lat,lng"
     * @param lockStatus Lock status (LOCKED, UNLOCKED, etc)
     * @param deviceStatus Device operational status
     */
    public void publishSensorDataFull(
            String deviceCode,
            Double batteryLevel,
            Boolean doorOpen,
            Double weight,
            Double temperature,
            Double humidity,
            String gpsCoordinates,
            String lockStatus,
            String deviceStatus) {
        try {
            SensorDataEvent event = SensorDataEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setBatteryLevel(batteryLevel)
                    .setDoorOpen(doorOpen)
                    .setWeight(weight)
                    .setTemperature(temperature)
                    .setHumidity(humidity)
                    .setGpsCoordinates(gpsCoordinates)
                    .setLockStatus(lockStatus)
                    .setDeviceStatus(deviceStatus)
                    .setTimestamp(java.time.Instant.ofEpochMilli(System.currentTimeMillis()))
                    .build();

            publishSensorDataEvent(event, deviceCode);

        } catch (Exception e) {
            logger.error("Failed to publish full sensor data for device: {}", deviceCode, e);
            throw new RuntimeException("Failed to publish full sensor data: " + e.getMessage(), e);
        }
    }

    // ========================================
    // Core Publishing Method (Direct Avro)
    // ========================================

    /**
     * Core method to publish SensorDataEvent to Kafka.
     * Called by all other publishing methods.
     *
     * Uses CentralEventCoordinator.publishEvent() for:
     * - Real-time IoT data (direct, best-effort delivery)
     * - Not using outbox pattern (real-time data doesn't need guaranteed delivery)
     * - Metadata enrichment (eventId generation if missing, timestamp setting)
     *
     * Partition Key Strategy:
     * - Uses deviceCode as partition key
     * - Ensures all sensor readings from same device go to same partition
     * - Enables per-device ordering guarantee
     *
     * @param event SensorDataEvent Avro object
     * @param deviceCode Device/box code (partition key for Kafka)
     * @throws RuntimeException If publishing fails
     */
    public void publishSensorDataEvent(SensorDataEvent event, String deviceCode) {
        try {
            // Ensure eventId is set (CentralEventCoordinator will also generate if needed)
            if (event.getEventId() == null || event.getEventId().isEmpty()) {
                event.setEventId(UUID.randomUUID().toString());
            }

            // Delegate to CentralEventCoordinator for unified event handling
            // Uses publishEvent() for real-time IoT data (not publishEventReliable)
            eventCoordinator.publishEvent(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_MQTT_SENSOR_DATA),
                    deviceCode,  // Partition key: all readings from device code go to same partition
                    event
            );

            logger.info(
                    "Sensor data event published - Topic: {}, DeviceCode: {}, EventId: {}, Battery: {}%, Weight: {}kg, Temp: {}°C",
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_MQTT_SENSOR_DATA),
                    deviceCode,
                    event.getEventId(),
                    event.getBatteryLevel() != null ? event.getBatteryLevel() : "N/A",
                    event.getWeight() != null ? event.getWeight() : "N/A",
                    event.getTemperature() != null ? event.getTemperature() : "N/A"
            );

        } catch (Exception e) {
            logger.error(
                    "Failed to publish sensor data event - DeviceCode: {}, EventId: {}, Error: {}",
                    deviceCode,
                    event.getEventId(),
                    e.getMessage(),
                    e
            );
            throw new RuntimeException("Failed to publish sensor data event: " + e.getMessage(), e);
        }
    }

    // ========================================
    // Async Publishing Methods (Non-blocking)
    // ========================================

    /**
     * Publish sensor data asynchronously without waiting for broker acknowledgment.
     * RECOMMENDED for high-throughput IoT scenarios (1000+ devices).
     *
     * Benefits:
     * - MQTT handler returns immediately (< 1ms) - NO BLOCKING
     * - Kafka ack happens in background thread pool
     * - Supports 1000+ concurrent devices without thread starvation
     * - Zero handler block tolerance requirement met
     *
     * Trade-offs:
     * - Best-effort delivery (not guaranteed like outbox)
     * - Caller must handle CompletableFuture for error monitoring
     * - Failed events should be routed to DLQ for recovery
     *
     * @param event SensorDataEvent object
     * @param deviceCode Device/box code (partition key)
     * @return CompletableFuture that completes when Kafka ack received
     *
     * Example:
     * ```
     * publisherService.publishSensorDataAsync(event, deviceCode)
     *     .thenAccept(result -> log.info("Published to partition: {}, offset: {}",
     *                                    result.getRecordMetadata().partition(),
     *                                    result.getRecordMetadata().offset()))
     *     .exceptionally(ex -> {
     *         log.error("Failed to publish sensor data", ex);
     *         // TODO: Route to DLQ for manual recovery
     *         return null;
     *     });
     * ```
     *
     * When to use:
     * - MQTT sensor data publishing (real-time, high volume)
     * - Handler blocking tolerance: ZERO
     * - Data loss tolerance: Can be mitigated with proper error handling
     *
     * When NOT to use:
     * - Critical business transactions needing guaranteed delivery
     * - Domain events (use publishEventReliable with outbox instead)
     * - Situations where handler must wait for confirmation
     */
    public CompletableFuture<SendResult<String, Object>>
            publishSensorDataAsync(SensorDataEvent event, String deviceCode) {
        try {
            // Ensure eventId is set for idempotency
            if (event.getEventId() == null || event.getEventId().isEmpty()) {
                event.setEventId(UUID.randomUUID().toString());
            }

            logger.info(
                    "Publishing sensor data asynchronously (non-blocking) - Topic: {}, DeviceCode: {}, EventId: {}",
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_MQTT_SENSOR_DATA),
                    deviceCode,
                    event.getEventId()
            );

            // Delegate to CentralEventCoordinator async Avro overload
            // This returns immediately without waiting for Kafka ack
            return eventCoordinator.publishEventAsync(
                    topicRegistry.getTopicName(TopicKey.SMARTLOCK_MQTT_SENSOR_DATA),
                    deviceCode,  // Partition key: all readings from device go to same partition
                    event
            );

        } catch (Exception e) {
            logger.error("Failed to publish sensor data async - DeviceCode: {}, Error: {}",
                    deviceCode, e.getMessage(), e);

            CompletableFuture<SendResult<String, Object>>
                    failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(e);
            return failedFuture;
        }
    }

    // ========================================
    // Topic Getter (for external use)
    // ========================================

    /**
     * Get the topic name used by this publisher.
     * Useful for monitoring, debugging, and configuration verification.
     *
     * @return Topic name (e.g., "smartlock.mqtt.sensorData.v0")
     */
    public String getTopicName() {
        return topicRegistry.getTopicName(TopicKey.SMARTLOCK_MQTT_SENSOR_DATA);
    }
}
