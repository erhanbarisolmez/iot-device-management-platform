package com.selftech.smartlock.models.dto.enums.mqtt;

public enum MQTT_CHANNEL {
  INPUT_CHANNEL("mqttInputChannel"),
  DEVICE_STATUS_CHANNEL("deviceStatusChannel"),
  DEVICE_EVENT_CHANNEL("deviceEventChannel"),
  SENSOR_DATA_CHANNEL("sensorDataChannel"),
  LOCK_OPERATION_CHANNEL("lockOperationChannel"),
  MESSAGE_CHANNEL("messageChannel");
  private final String name;

  MQTT_CHANNEL(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
