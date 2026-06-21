package com.selftech.smartlock.config.kafka;

import com.selftech.kafka.core.serialization.EventTypeRegistry;
import com.selftech.kafka.models.avro.SensorDataEvent;
import com.selftech.kafka.models.avro.SmsEvent;
import com.selftech.smartlock.avro.LockBoxEvent;
import com.selftech.smartlock.avro.LockDeviceEvent;
import com.selftech.smartlock.avro.PaymentEvent;
import com.selftech.smartlock.avro.TransactionEvent;
import com.selftech.smartlock.kafka.events.LockOperationEvent;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;

/**
 * Registers smartlock event types with the shared EventTypeRegistry.
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class EventTypeRegistrarConfig {

    private final EventTypeRegistry eventTypeRegistry;

    @PostConstruct
    public void registerEventTypes() {
        log.info("Registering smartlock event types...");

        eventTypeRegistry.register("LockBoxEvent", LockBoxEvent.class);
        eventTypeRegistry.register("LockDeviceEvent", LockDeviceEvent.class);
        eventTypeRegistry.register("LockOperationEvent", LockOperationEvent.class);
        eventTypeRegistry.register("PaymentEvent", PaymentEvent.class);
        eventTypeRegistry.register("TransactionEvent", TransactionEvent.class);
        eventTypeRegistry.register("SensorDataEvent", SensorDataEvent.class);
        eventTypeRegistry.register("SmsEvent", SmsEvent.class);

        eventTypeRegistry.logStatus();
    }
}
