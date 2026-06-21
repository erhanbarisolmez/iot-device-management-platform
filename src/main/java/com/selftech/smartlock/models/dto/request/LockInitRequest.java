package com.selftech.smartlock.models.dto.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LockInitRequest {
  private String deviceId;
  private String plateNumber;
  private Long userId;
}
