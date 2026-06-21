package com.selftech.smartlock.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.selftech.kafka.converter.AbstractEventConverter;
import com.selftech.kafka.converter.EventConversionException;
import com.selftech.kafka.models.avro.SensorDataEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class SensorDataEventConverter extends AbstractEventConverter<SensorDataEvent> {

    private static final String[] TOPIC_PATTERNS = {
        "smartlock.mqtt.sensorData.v0",
        "smartlock.mqtt.*",
        "smartlock.*.data.v0"
    };

    @Override
    public SensorDataEvent convert(String jsonInput) throws EventConversionException {
        logConversionStart(jsonInput);

        try {
            JsonNode node = parseJson(jsonInput);

            String eventId = extractString(node, "eventId", false, generateEventId());
            Double batteryLevel = extractDouble(node, "batteryLevel", false, null);
            Boolean doorOpen = extractBoolean(node, "doorOpen", false, null);
            Double weight = extractDouble(node, "weight", false, null);
            String lockStatus = extractString(node, "lockStatus", false, null);
            Double temperature = extractDouble(node, "temperature", false, null);
            Double humidity = extractDouble(node, "humidity", false, null);
            String gpsCoordinates = extractString(node, "gpsCoordinates", false, null);
            String deviceStatus = extractString(node, "deviceStatus", false, null);

            long timestamp;
            if (node.has("timestamp") && !node.get("timestamp").isNull()) {
                timestamp = normalizeTimestamp(node.get("timestamp"));
            } else {
                logger.warn("Timestamp field missing, using current time. EventId: {}", eventId);
                timestamp = System.currentTimeMillis();
            }

            SensorDataEvent event = SensorDataEvent.newBuilder()
                .setEventId(eventId)
                .setBatteryLevel(batteryLevel)
                .setDoorOpen(doorOpen)
                .setWeight(weight)
                .setLockStatus(lockStatus)
                .setTemperature(temperature)
                .setHumidity(humidity)
                .setGpsCoordinates(gpsCoordinates)
                .setDeviceStatus(deviceStatus)
                .setTimestamp(java.time.Instant.ofEpochMilli(timestamp))
                .setCorrelationId(null)
                .setEventType("SensorDataReceived")
                .setSource("SMARTLOCK")
                .setSchemaVersion("1.0")
                .setMetadata(null)
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

    @Override
    public Class<SensorDataEvent> getTargetEventClass() {
        return SensorDataEvent.class;
    }

    @Override
    public String[] getTopicPatterns() {
        return TOPIC_PATTERNS;
    }

    @Override
    public String getConverterName() {
        return "SensorDataEventConverter";
    }
}
