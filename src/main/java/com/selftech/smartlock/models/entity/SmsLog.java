package com.selftech.smartlock.models.entity;

import java.time.LocalDateTime;

import com.selftech.smartlock.models.dto.enums.SmsLogStatus;
import com.selftech.smartlock.models.dto.enums.SmsProviderStatus;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "sms_logs")
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SmsLog {
  
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "lock_operation_id")
  private DeviceOperation lockOperation;
  
  private String phoneNumber;
  
  @Column(length = 1024)
  private String message;

  @Enumerated(EnumType.STRING)
  private SmsLogStatus status;
  
  @Enumerated(EnumType.STRING)
  private SmsProviderStatus smsProvider;
  
  private String providerMessageId;
  private LocalDateTime sentAt;
  private LocalDateTime deliveredAt;
}
