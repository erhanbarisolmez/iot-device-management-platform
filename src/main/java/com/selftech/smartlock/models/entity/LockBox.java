package com.selftech.smartlock.models.entity;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import com.selftech.smartlock.models.dto.enums.BoxStatus;
import com.fasterxml.jackson.annotation.JsonIgnore;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "smart_lock_boxes")
public class LockBox {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(unique = true, nullable = false)
  private String boxCode;

  private String location;
  private Double lat;
  private Double lng;

  @Enumerated(EnumType.STRING)
  @Column(length = 20, nullable = false)
  private BoxStatus status = BoxStatus.EMPTY;

  private LocalDateTime lastMaintenanceDate;
  private Integer usageCount = 0;

  private Double expectedWeight = 1.5; // Beklenen kilit ağırlığı (kg)
  private Double currentWeight = 0.0; // Mevcut ağırlık ölçümü (kg)
  private Boolean doorOpen = false; // Kapı durumu (varsayılan: kapalı)

  // --- Kutu Donanım Bilgileri ---
  private Double batteryLevel; // Kutunun pil seviyesi (%lik olarak)
  private String firmwareVersion; // Kutudaki ESP32'nin yazılım versiyonu
  private LocalDateTime lastCommunication; // Kutunun sunucu ile son iletişim zamanı

  // Personelin kutuyu açmak için kullanacağı geçici kod
  private String openingCode;
  private LocalDateTime openingCodeExpiry;

  // One-to-Many: Bir kutuya birden fazla kilit geçici olarak konulabilir
  @OneToMany(mappedBy = "currentBox", fetch = FetchType.LAZY)
  @JsonIgnore
  private List<LockDevice> devices = new ArrayList<>();

  // Kutuya kilit yerleştirme/çıkarma geçmişi
  @OneToMany(mappedBy = "lockBox", fetch = FetchType.LAZY)
  @JsonIgnore
  private List<DeviceOperation> lockOperations = new ArrayList<>();

}
