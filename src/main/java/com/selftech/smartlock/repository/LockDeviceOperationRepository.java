package com.selftech.smartlock.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.selftech.smartlock.models.dto.enums.LockStatus;
import com.selftech.smartlock.models.entity.DeviceOperation;

@Repository
public interface LockDeviceOperationRepository extends JpaRepository<DeviceOperation, Long> {
  Optional<DeviceOperation> findByLockCode(String lockCode);
  Optional<DeviceOperation> findByUnlockCode(String unlockCode);
  Optional<DeviceOperation> findByDevice_DeviceCodeAndStatus(String deviceCode, LockStatus status); //device.deviceCode ile arama yapar.
  Optional<DeviceOperation> findByBoxOpeningCode(String boxOpeningCode);
  // Optional<LockOperation> findByDeviceAndStatus(String deviceId, LockStatus status); //  Device nesnesi verildiğinde çalışır.
}
