package com.selftech.smartlock.models.dto.request.mockPayment;

import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MockPaymentEvent {
  private String transactionId;
  private String lockCode;
  private String msisdn;
  private String status; // SUCCESS / FAILED
  private BigDecimal amount;
}

// Payguru test