package com.selftech.smartlock.models.dto.request.sensor;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class SensorDataRequest {
  private Double weight;
  private Boolean doorOpen;
  private Double batteryLevel;
}
