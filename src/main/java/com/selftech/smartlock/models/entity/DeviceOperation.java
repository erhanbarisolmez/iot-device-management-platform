package com.selftech.smartlock.models.entity;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import com.selftech.selfparkbackendv001.models.user.User;
import com.selftech.smartlock.models.dto.enums.LockStatus;
import com.selftech.smartlock.models.dto.enums.OperationType;
import com.selftech.smartlock.models.dto.enums.PaymentStatus;
import com.fasterxml.jackson.annotation.JsonIgnore;

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
import lombok.ToString;

@Entity
@Table(name = "lock_operations")
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString(exclude = {"device", "vehicle", "user", "lockBox"})
public class DeviceOperation {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "device_id", nullable = false)
  @JsonIgnore
  private LockDevice device;

  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "vehicle_id", nullable = true)
  @JsonIgnore
  private Vehicle vehicle;

  // Operasyonu gerçekleştiren personel (cihazı araca takan vb.)
  @ManyToOne
  @JoinColumn(name = "user_id")
  @JsonIgnore
  private User user;

  private String lockCode;
  private String unlockCode;

  @jakarta.persistence.Column(unique = true)
  private String boxOpeningCode;

  private LocalDateTime lockTime;
  private LocalDateTime unlockTime;
  private LocalDateTime returnTime;

  @Enumerated(EnumType.STRING)
  private LockStatus status;

  @Enumerated(EnumType.STRING)
  private OperationType operationType;
  private LocalDateTime operationTime;
  private String notes;

  private BigDecimal penaltyAmount;
  private BigDecimal lockFee;
  private BigDecimal depositAmount;
  private BigDecimal totalAmount;
  private BigDecimal refundAmount;

  private LocalDateTime depositRefundedAt;

  private LocalDateTime paymentConfirmedAt;

  @Enumerated(EnumType.STRING)
  private PaymentStatus paymentStatus;

  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "lock_box_id")
  @JsonIgnore
  private LockBox lockBox;

  // ESP32'den gelen sensör verileri
  private Double lockWeight; // Kilidin ağırlığı
  private Double boxWeight; // Kutunun ağırlığı
  private Boolean boxDoorOpen; // Kutu kapısı açık mı?

  @Column(name = "payment_id")
  private String paymentId;
}
