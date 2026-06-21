package com.selftech.smartlock.service.concretes;

import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.selftech.kafka.converter.EventConversionException;
import com.selftech.kafka.converter.EventConverterFactory;
import com.selftech.kafka.core.publisher.CentralEventCoordinator;
import com.selftech.kafka.models.avro.SensorDataEvent;
import com.selftech.smartlock.event.kafka.publisher.SensorDataEventPublisherService;
import com.selftech.smartlock.models.dto.request.sensor.SensorDataRequest;

import lombok.RequiredArgsConstructor;

import java.util.Map;

/**
 * MQTT to Kafka Bridge Service - PHASE 6 PROFESSIONAL ARCHITECTURE
 *
 * Bridges JSON data from MQTT to Kafka using pure professional architecture:
 * 1. SensorDataEventPublisherService (professional) for sensor data events
 * 2. CentralEventCoordinator (unified facade) for all event publishing
 * 3. Polymorphic event converters for generic data types
 *
 * Architecture:
 * ```
 * MQTT Message (JSON)
 *     ↓
 * MqttToKafkaBridgeService
 *     ├─ SensorData → SensorDataEventPublisherService
 *     │              ↓
 *     │              CentralEventCoordinator (unified management)
 *     │              ↓
 *     │              EventProducerService (low-level)
 *     │              ↓
 *     │              Kafka Topic
 *     │
 *     └─ Error → CentralEventCoordinator (DLQ Publishing)
 *                ↓
 *                EventProducerService (low-level)
 *                ↓
 *                DLQ Topic
 * ```
 *
 * Evolution from PHASE 5:
 * - Old: Hybrid approach with EventProducerService fallback
 * - New: Pure professional architecture using ONLY CentralEventCoordinator
 * - Benefit: Unified event management, no legacy code paths, easier maintenance
 *
 * Features:
 * - Pure professional event publishing (via CentralEventCoordinator only)
 * - Polymorphic JSON to Avro conversion based on topic
 * - Professional error handling with DLQ routing
 * - Supports multiple IoT device types
 * - Configurable topic patterns for each converter
 * - No fallback mechanisms or legacy code paths
 *
 * Error Handling:
 * - No converter found for topic: logs error, routes to DLQ via CentralEventCoordinator
 * - Conversion failure: logs error with details, routes to DLQ via CentralEventCoordinator
 * - Type mismatch: throws exception (converter guarantees correct type)
 * - Kafka send failure: propagates exception for retry in MQTTConfig handler
 *
 * @author PHASE 6 Professional Architecture - Complete Coordinator Adoption
 */
@Service
@RequiredArgsConstructor
public class MqttToKafkaBridgeService {

  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(MqttToKafkaBridgeService.class);

  // Professional event publishing services
  private final SensorDataEventPublisherService sensorDataPublisher;
  private final CentralEventCoordinator eventCoordinator;
  private final EventConverterFactory converterFactory;
  private final ObjectMapper objectMapper = new ObjectMapper();

  /**
   * Forward JSON sensor data to Kafka using professional publisher (PHASE 1)
   *
   * Uses SensorDataEventPublisherService which wraps CentralEventCoordinator
   * for unified event management, correlation tracking, and metadata enrichment.
   *
   * Flow:
   * 1. Convert DTO to JSON
   * 2. Find appropriate converter based on topic pattern
   * 3. Convert JSON to SensorDataEvent Avro object
   * 4. Delegate to SensorDataEventPublisherService for professional publishing
   * 5. Publisher uses CentralEventCoordinator for unified handling
   *
   * Benefits over direct EventProducerService:
   * - Automatic metadata enrichment (eventId, timestamps)
   * - Correlation ID tracking for distributed tracing
   * - Unified event handling (single entry point)
   * - Better error handling and DLQ routing
   * - Easy to add cross-cutting concerns (metrics, audit)
   *
   * @param boxCodeFromTopic The device's box code, used as Kafka message key for partitioning
   * @param data The sensor data to be sent (DTO format)
   * @param topic The target Kafka topic (e.g., "smartlock.mqtt.sensorData.v0")
   * @throws RuntimeException If conversion or publishing fails (will trigger DLQ routing in MQTTConfig)
   */
  public void forwardSensorDataToKafka(String boxCodeFromTopic, SensorDataRequest data, String topic) {
    try {
      // Step 1: Convert DTO to JSON
      String jsonInput = objectMapper.writeValueAsString(data);

      // Step 2: Find appropriate converter and convert to Avro SensorDataEvent
      SpecificRecord event = converterFactory.convert(topic, jsonInput);

      // Step 3: Type-safe cast to SensorDataEvent (expected type from converter factory)
      if (!(event instanceof SensorDataEvent)) {
        logger.error(
            "Converter returned unexpected event type '{}' for topic '{}' - Expected SensorDataEvent",
            event.getClass().getSimpleName(),
            topic
        );
        throw new RuntimeException(
            "Converter returned unexpected event type: " + event.getClass().getSimpleName()
        );
      }

      SensorDataEvent sensorEvent = (SensorDataEvent) event;

      // Step 3.5: CRITICAL - Enrich metadata with deviceCode from MQTT topic path
      // This ensures anomaly detector can identify the device instead of showing "UNKNOWN"
      // IMPORTANT: Metadata must be initialized in SensorDataEventConverter (set to null)
      // otherwise Avro serialization won't persist the metadata enrichment
      Map<CharSequence, CharSequence> metadata = sensorEvent.getMetadata();
      if (metadata == null) {
        metadata = new java.util.HashMap<>();
      }
      metadata.put("deviceCode", boxCodeFromTopic);
      sensorEvent.setMetadata(metadata);

      logger.debug("SensorDataEvent enriched with deviceCode: {} for eventId: {}",
          boxCodeFromTopic, sensorEvent.getEventId());

      // Step 4: Delegate to professional publisher service - ASYNC (non-blocking)
      // Uses publishSensorDataAsync() which returns immediately without waiting for Kafka ack
      // Handler never blocks, enabling 1000+ concurrent MQTT devices
      // Kafka acknowledgment happens asynchronously in background thread pool
      sensorDataPublisher.publishSensorDataAsync(sensorEvent, boxCodeFromTopic)
          .thenAccept(result ->
              logger.info(
                  "Sensor data for box '{}' successfully published to Kafka topic '{}' partition {} offset {} with Event ID '{}'",
                  boxCodeFromTopic, topic,
                  result.getRecordMetadata().partition(),
                  result.getRecordMetadata().offset(),
                  sensorEvent.getEventId()
              )
          )
          .exceptionally(ex -> {
              logger.error(
                  "Async publish failed for box '{}' to topic '{}'. Error: {}. Event ID: '{}' - Message will be routed to DLQ by MQTTConfig",
                  boxCodeFromTopic, topic, ex.getMessage(), sensorEvent.getEventId(), ex
              );
              // Re-throw so MQTTConfig.sensorDataHandler() catches and routes to DLQ
              throw new RuntimeException("Async publish failed: " + ex.getMessage(), ex);
          });

    } catch (EventConversionException e) {
      // Conversion failed - log error with details
      logger.error(
          "Failed to convert sensor data for box '{}' to topic '{}': {} - Details: {}",
          boxCodeFromTopic, topic, e.getMessage(), e.getFailureReason(), e
      );
      // Exception will be caught by MQTTConfig.sensorDataHandler() and routed to DLQ
      throw new RuntimeException("Sensor data conversion failed", e);

    } catch (EventConverterFactory.ConverterNotFoundException e) {
      // No converter found for topic
      logger.error(
          "No converter found for topic '{}' - box: '{}'. Available topics: {}",
          topic, boxCodeFromTopic, converterFactory.getAvailableTopicPatterns()
      );
      // Exception will trigger DLQ routing in MQTTConfig
      throw new RuntimeException("No converter found for topic: " + topic, e);

    } catch (Exception e) {
      // Other errors (JSON serialization, Kafka errors, etc.)
      logger.error(
          "Unexpected error while forwarding sensor data for box '{}' to topic '{}': {}",
          boxCodeFromTopic, topic, e.getMessage(), e
      );
      throw new RuntimeException("Failed to forward sensor data to Kafka", e);
    }
  }

  /**
   * Forward JSON data to Kafka using professional CentralEventCoordinator
   *
   * Alternative method when you already have JSON data (not DTO).
   * Converts JSON to Avro and publishes using CentralEventCoordinator
   * for unified event management and professional error handling.
   *
   * @param jsonInput Raw JSON string
   * @param topic Target Kafka topic
   * @param messageKey Kafka message key (partition key)
   * @throws RuntimeException If conversion or publishing fails
   */
  public void forwardJsonToKafka(String jsonInput, String topic, String messageKey) {
    try {
      // Step 1: Convert JSON to Avro using factory
      SpecificRecord event = converterFactory.convert(topic, jsonInput);

      // Step 2: Publish to Kafka using professional CentralEventCoordinator
      eventCoordinator.publishEvent(topic, messageKey, event);

      logger.info(
          "JSON data successfully published via CentralEventCoordinator to Kafka topic '{}' with key '{}'",
          topic, messageKey
      );

    } catch (EventConversionException e) {
      logger.error(
          "Failed to convert JSON data to topic '{}': {}",
          topic, e.getMessage(), e
      );
      throw new RuntimeException("JSON conversion failed", e);

    } catch (EventConverterFactory.ConverterNotFoundException e) {
      logger.error(
          "No converter found for topic '{}'. Available topics: {}",
          topic, converterFactory.getAvailableTopicPatterns()
      );
      throw new RuntimeException("No converter found for topic: " + topic, e);

    } catch (Exception e) {
      logger.error(
          "Unexpected error while forwarding JSON to topic '{}': {}",
          topic, e.getMessage(), e
      );
      throw new RuntimeException("Failed to forward JSON to Kafka", e);
    }
  }

  /**
   * Forward JSON data to DLQ (Dead Letter Queue) - PHASE 6 PROFESSIONAL ARCHITECTURE
   *
   * Converts JSON to Avro and publishes to DLQ using CentralEventCoordinator.
   * If conversion fails, logs error but doesn't throw (DLQ processing should not fail main flow).
   *
   * Important: Uses sourceTopic to find converter, then sends to dlqTopic.
   * DLQ topics have "dlq.*" prefix which has no converter, so we use the original
   * topic to find the converter and send the Avro event to DLQ topic.
   *
   * Flow:
   * 1. Extract original topic from DLQ topic (remove "dlq." prefix)
   * 2. Find converter using original topic
   * 3. Convert JSON to Avro
   * 4. Publish Avro event to DLQ topic via CentralEventCoordinator (professional)
   * 5. Log results
   *
   * UPGRADE from PHASE 5:
   * - Old: Hybrid approach with EventProducerService fallback for non-SensorDataEvent types
   * - New: Pure professional architecture using ONLY CentralEventCoordinator for all events
   * - Benefit: Consistent error handling, no legacy code paths, unified DLQ publishing
   *
   * @param jsonInput Raw JSON string that failed in main flow
   * @param dlqTopic Target DLQ Kafka topic (e.g., "dlq.smartlock.mqtt.sensorData.v0")
   * @param messageKey Kafka message key (partition key)
   * @return true if successfully sent to DLQ, false otherwise
   */
  public boolean forwardToDLQ(String jsonInput, String dlqTopic, String messageKey) {
    try {
      // Step 0: Extract original topic from DLQ topic
      // DLQ topic format: dlq.{originalTopic}
      // Remove "dlq." prefix to get original topic
      String sourceTopic = dlqTopic.startsWith("dlq.") ? dlqTopic.substring(4) : dlqTopic;

      // Step 1: Convert JSON to Avro using original topic's converter
      SpecificRecord event = converterFactory.convert(sourceTopic, jsonInput);

      // Step 2: Publish to DLQ topic using CentralEventCoordinator
      // CRITICAL: Use dlqTopic parameter, NOT the normal topic
      eventCoordinator.publishEvent(dlqTopic, messageKey, event);

      logger.warn(
          "Message successfully routed to DLQ topic '{}' via CentralEventCoordinator (converted using source topic '{}') with key '{}' - Event type: {}",
          dlqTopic, sourceTopic, messageKey, event.getClass().getSimpleName()
      );

      return true;

    } catch (EventConversionException e) {
      // Conversion failed - log but don't throw (we already have the raw JSON)
      logger.error(
          "Failed to convert message to DLQ topic '{}': {} - Details: {}. Raw JSON will be logged for manual recovery.",
          dlqTopic, e.getMessage(), e.getFailureReason(), e
      );
      // Log the raw JSON for manual recovery
      logger.error("Raw JSON payload for manual recovery (Key: {}): {}", messageKey, jsonInput);
      return false;

    } catch (EventConverterFactory.ConverterNotFoundException e) {
      // No converter found - log but don't throw
      logger.error(
          "No converter found for DLQ topic '{}' with key '{}'. Available topics: {}. Raw JSON will be logged for manual recovery.",
          dlqTopic, messageKey, converterFactory.getAvailableTopicPatterns(), e
      );
      logger.error("Raw JSON payload for manual recovery (Key: {}): {}", messageKey, jsonInput);
      return false;

    } catch (Exception e) {
      // Other errors - log but don't throw
      logger.error(
          "Unexpected error while forwarding to DLQ topic '{}' with key '{}': {}. Raw JSON will be logged for manual recovery.",
          dlqTopic, messageKey, e.getMessage(), e
      );
      logger.error("Raw JSON payload for manual recovery (Key: {}): {}", messageKey, jsonInput);
      return false;
    }
  }

  /**
   * Extract event ID from Avro event for logging
   *
   * Works with any Avro event that has eventId field 
   *
   * @param event Avro SpecificRecord event
   * @return Event ID or "UNKNOWN"
   */
  private String getEventId(SpecificRecord event) {
    try {
      // Use reflection to safely get eventId field
      Object eventIdObj = event.getClass().getMethod("getEventId").invoke(event);
      return eventIdObj != null ? eventIdObj.toString() : "UNKNOWN";
    } catch (Exception e) {
      return "UNKNOWN";
    }
  }
}