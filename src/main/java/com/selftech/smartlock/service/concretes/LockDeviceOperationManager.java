package com.selftech.smartlock.service.concretes;
  
import java.util.Optional;

import org.springframework.stereotype.Service;

import com.selftech.smartlock.models.dto.enums.LockStatus;
import com.selftech.smartlock.models.entity.DeviceOperation;
import com.selftech.smartlock.repository.LockDeviceOperationRepository;
import com.selftech.smartlock.service.abstracts.ILockDeviceOperationService;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class LockDeviceOperationManager implements ILockDeviceOperationService {

  private final LockDeviceOperationRepository lockDeviceOperationRepository;

  @Override
  public DeviceOperation save(DeviceOperation lockOperation) {
    return lockDeviceOperationRepository.save(lockOperation);
  }

  @Override
  public Optional<DeviceOperation> findByLockCode(String lockCode) {
    return lockDeviceOperationRepository.findByLockCode(lockCode);
  }

  @Override
  public Optional<DeviceOperation> findByUnlockCode(String unlockCode) {
    return lockDeviceOperationRepository.findByUnlockCode(unlockCode);
  }

  /**
   * Finds a device operation by device code and status.
   */
  @Override
  public Optional<DeviceOperation> findByDevice_DeviceCodeAndStatus(String deviceCode, LockStatus status) {
    return lockDeviceOperationRepository.findByDevice_DeviceCodeAndStatus(deviceCode, status);
  }

  @Override
  public Optional<DeviceOperation> findByBoxOpeningCode(String boxOpeningCode) {
    return lockDeviceOperationRepository.findByBoxOpeningCode(boxOpeningCode);
  }
}
