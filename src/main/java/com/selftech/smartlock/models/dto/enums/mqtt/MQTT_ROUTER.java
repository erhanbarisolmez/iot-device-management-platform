package com.selftech.smartlock.models.dto.enums.mqtt;

public enum MQTT_ROUTER {
  SENSOR_DATA("selfpark/**/data"),
  DEVICE_STATUS("selftech/*/status"),
  DEVICE_EVENT("selftech/*/event"),
  MESSAGE_DATA("selfpark/*/message");

  private final String topic;

  MQTT_ROUTER(String topic) {
    this.topic = topic;
  }

  public String getTopic() {
    return topic;
  }

}
