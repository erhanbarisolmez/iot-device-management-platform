package com.selftech.smartlock.models.dto.response;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class GetAllLockBoxResponse {
  private String boxCode;
  private String batteryLevel;
  private String currentWeight;
  private String doorOpen;
  private String expectedWeight;
  private String firmwareVersion;
  private String lastCommunication;
  private String lastMaintenanceDate;

  private String lat;
  private String lng;
  private String location;
  private String openingCode;
  private String openingCodeExpiry;
  private String status;
  private String usageCount;


}
