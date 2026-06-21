package com.selftech.smartlock.models.entity;

import java.time.LocalDateTime;

import com.selftech.selfparkbackendv001.models.user.User;
import com.selftech.smartlock.models.dto.enums.OperationType;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "box_operations")
public class BoxOperation {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne
    @JoinColumn(name = "box_id", nullable = false)
    private LockBox lockBox;

    @ManyToOne
    @JoinColumn(name = "device_id")
    private LockDevice device;

    // Operasyonu gerçekleştiren kullanıcı/personel
    @ManyToOne
    @JoinColumn(name = "user_id")
    private User user;

    @Enumerated(EnumType.STRING)
    @Column(name = "operation_type")
    private OperationType operationType;

    private LocalDateTime operationTime;
    private String notes;
    private String previousStatus;
    private String newStatus;
}