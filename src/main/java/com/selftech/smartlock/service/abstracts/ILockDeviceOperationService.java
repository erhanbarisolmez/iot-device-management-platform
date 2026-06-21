package com.selftech.smartlock.service.abstracts;

import java.util.Optional;

import com.selftech.smartlock.models.dto.enums.LockStatus;
import com.selftech.smartlock.models.entity.DeviceOperation;

public interface ILockDeviceOperationService {
  DeviceOperation save(DeviceOperation lockOperation);

  Optional<DeviceOperation> findByLockCode(String lockCode);

  Optional<DeviceOperation> findByUnlockCode(String unlockCode);

  Optional<DeviceOperation> findByDevice_DeviceCodeAndStatus(String deviceCode, LockStatus status);

  Optional<DeviceOperation> findByBoxOpeningCode(String boxOpeningCode);
}
