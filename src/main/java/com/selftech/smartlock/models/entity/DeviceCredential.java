package com.selftech.smartlock.models.entity;

import java.time.LocalDateTime;

import com.selftech.smartlock.models.dto.enums.SignatureAlgorithm;

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
@Table(name = "device_credentials")
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DeviceCredential {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "device_id", nullable = false)
  private LockDevice device;

  @Column(name = "public_key", nullable =  false, columnDefinition = "TEXT")
  private String publicKey;

  @Enumerated(EnumType.STRING)
  @Column(name = "algorithm", nullable = false)
  private SignatureAlgorithm algorithm;

  @Column(name = "is_active", nullable = false)
  private Boolean isActive = true;

  private LocalDateTime createdAt;
  private LocalDateTime revokedAt;

}