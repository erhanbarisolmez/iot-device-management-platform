package com.selftech.smartlock.models.entity;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import com.selftech.smartlock.shared.model.Plate;
import com.selftech.smartlock.shared.model.User;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Data
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "vehicles")
public class Vehicle {
    
  @Id
  @GeneratedValue(strategy =  GenerationType.IDENTITY)
  private Long id;

  @ManyToOne
  @JoinColumn(name = "plate_id", nullable = false)
  private Plate plate;

  @ManyToOne
  @JoinColumn(name = "user_id", nullable = false)
  private User user;

  private LocalDateTime createdAt;

  @OneToMany(mappedBy = "vehicle", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
  private List<DeviceOperation> lockOperations = new ArrayList<>();

  
}
