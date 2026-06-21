package com.selftech.smartlock.models.entity;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import com.selftech.smartlock.shared.model.User;
import com.selftech.smartlock.models.dto.enums.LockStatus;

import jakarta.persistence.CascadeType;
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
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString(exclude = { "deviceOperations", "credentials", "currentBox", "currentUser" })
@Table(name = "smart_lock_devices")

public class LockDevice {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(unique = true, nullable = false)
  private String deviceCode;

  @Enumerated(EnumType.STRING)
  private LockStatus status;

  private String retrievalCode;
  private LocalDateTime retrievalCodeExpiry;

  // Many-to-One: Bir cihaz bir kutuda bulunabilir
  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "current_box_id")
  @JsonIgnore
  private LockBox currentBox;

  // Operasyon geçmişi
  @OneToMany(mappedBy = "device", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
  @JsonIgnore
  private List<DeviceOperation> deviceOperations = new ArrayList<>();

  // Bir cihaza ait tüm kimlik bilgilerini tutar (anahtar rotasyonu için)
  @OneToMany(mappedBy = "device", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
  @JsonIgnore
  private List<DeviceCredential> credentials = new ArrayList<>();

  // son kullanıcı bilgisi
  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "current_user_id")
  @JsonIgnore
  private User currentUser;
}
