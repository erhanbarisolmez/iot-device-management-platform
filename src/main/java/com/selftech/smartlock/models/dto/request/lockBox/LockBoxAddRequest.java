package com.selftech.smartlock.models.dto.request.lockBox;

import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class LockBoxAddRequest {
  private String boxCode;
  private String location;
  private BigDecimal lng;
  private BigDecimal lat;
}
