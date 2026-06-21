package com.selftech.smartlock.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import com.selftech.smartlock.models.entity.DeviceCredential;

public interface DeviceCredentialRepository extends JpaRepository<DeviceCredential, Long> {
    Optional<DeviceCredential> findByDevice_DeviceCodeAndIsActiveTrue(String deviceCode);
}