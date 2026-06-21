package com.selftech.smartlock.converter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.selftech.kafka.converter.AbstractEventConverter;
import com.selftech.kafka.converter.EventConversionException;
import com.selftech.smartlock.avro.LockBoxEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LockBoxEventConverter extends AbstractEventConverter<LockBoxEvent> {

    private static final String[] TOPIC_PATTERNS = {
        "smartlock.event.boxOperation.v0",
        "smartlock.event.box*",
        "smartlock.event.*"
    };

    @Override
    public LockBoxEvent convert(String jsonInput) throws EventConversionException {
        logConversionStart(jsonInput);

        try {
            JsonNode node = parseJson(jsonInput);
            validateRequiredFields(node, "boxCode", "eventType", "status", "doorOpen", "currentWeight", "timestamp");

            String eventId = extractString(node, "eventId", false, generateEventId());
            String boxCode = extractString(node, "boxCode", true, null);
            String eventType = extractString(node, "eventType", true, null);
            String status = extractString(node, "status", true, null);
            Boolean doorOpen = extractBoolean(node, "doorOpen", true, null);
            Double currentWeight = extractDouble(node, "currentWeight", true, null);

            String previousStatus = extractString(node, "previousStatus", false, null);
            Double previousWeight = extractDouble(node, "previousWeight", false, null);
            Double expectedWeight = extractDouble(node, "expectedWeight", false, null);
            Long deviceCount = extractLong(node, "deviceCount", false, 0L);
            String userId = extractString(node, "userId", false, null);
            String operationType = extractString(node, "operationType", false, null);
            Double temperature = extractDouble(node, "temperature", false, null);
            Double humidity = extractDouble(node, "humidity", false, null);
            String gpsCoordinates = extractString(node, "gpsCoordinates", false, null);
            Double batteryLevel = extractDouble(node, "batteryLevel", false, null);
            String correlationId = extractString(node, "correlationId", false, null);

            List<CharSequence> deviceCodes = extractDeviceCodesArray(node);
            Map<CharSequence, CharSequence> metadata = extractMetadataMap(node);

            java.time.Instant timestamp;
            if (node.has("timestamp") && !node.get("timestamp").isNull()) {
                timestamp = java.time.Instant.ofEpochMilli(normalizeTimestamp(node.get("timestamp")));
            } else {
                logger.warn("Timestamp field missing, using current time. EventId: {}", eventId);
                timestamp = java.time.Instant.now();
            }

            LockBoxEvent event = LockBoxEvent.newBuilder()
                .setEventId(eventId)
                .setBoxCode(boxCode)
                .setEventType(eventType)
                .setPreviousStatus(previousStatus)
                .setStatus(status)
                .setDoorOpen(doorOpen)
                .setCurrentWeight(currentWeight)
                .setPreviousWeight(previousWeight)
                .setExpectedWeight(expectedWeight)
                .setDeviceCount(Math.toIntExact(deviceCount != null ? deviceCount : 0L))
                .setDeviceCodes(deviceCodes)
                .setUserId(userId)
                .setOperationType(operationType)
                .setTemperature(temperature)
                .setHumidity(humidity)
                .setGpsCoordinates(gpsCoordinates)
                .setBatteryLevel(batteryLevel)
                .setTimestamp(timestamp)
                .setCorrelationId(correlationId)
                .setSchemaVersion("1.0")
                .setMetadata(metadata)
                .build();

            logConversionSuccess(eventId);
            return event;

        } catch (EventConversionException e) {
            logConversionFailure("Event conversion exception", e);
            throw e;
        } catch (Exception e) {
            String errorMsg = "Unexpected error during conversion: " + e.getMessage();
            logConversionFailure(errorMsg, e);
            throw new EventConversionException(
                getTargetEventClass().getSimpleName(),
                jsonInput,
                errorMsg,
                e
            );
        }
    }

    private List<CharSequence> extractDeviceCodesArray(JsonNode node) {
        if (node == null || !node.has("deviceCodes")) {
            return null;
        }
        JsonNode codesNode = node.get("deviceCodes");
        if (codesNode.isNull() || !codesNode.isArray()) {
            return null;
        }
        List<CharSequence> codes = new java.util.ArrayList<>();
        codesNode.forEach(codeNode -> codes.add(codeNode.asText()));
        return codes.isEmpty() ? null : codes;
    }

    private Map<CharSequence, CharSequence> extractMetadataMap(JsonNode node) {
        if (node == null || !node.has("metadata")) {
            return null;
        }
        JsonNode metadataNode = node.get("metadata");
        if (metadataNode.isNull() || !metadataNode.isObject()) {
            return null;
        }
        Map<CharSequence, CharSequence> metadata = new HashMap<>();
        metadataNode.fields().forEachRemaining(entry ->
            metadata.put(entry.getKey(), entry.getValue().asText())
        );
        return metadata.isEmpty() ? null : metadata;
    }

    @Override
    public Class<LockBoxEvent> getTargetEventClass() {
        return LockBoxEvent.class;
    }

    @Override
    public String[] getTopicPatterns() {
        return TOPIC_PATTERNS;
    }

    @Override
    public String getConverterName() {
        return "LockBoxEventConverter";
    }
}
