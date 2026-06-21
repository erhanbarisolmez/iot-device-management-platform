package com.selftech.smartlock.service.abstracts;

import java.util.List;
import java.util.Optional;

import com.selftech.smartlock.shared.model.User;
import com.selftech.smartlock.models.dto.request.lockDevice.LockDeviceRequest;
import com.selftech.smartlock.models.entity.LockDevice;

public interface ILockDeviceService {
    LockDevice getDeviceFromBox(String boxCode);

    List<LockDevice> getAllDevices();

    Optional<LockDevice> findByDeviceCode(String deviceCode);

    LockDevice createDevice(LockDeviceRequest request, User user);

    LockDevice updateLockDevice(Long id, LockDevice deviceDetails, User user);

    void deleteLockDevice(Long id, User user);

    String generateRetrievalCode(String deviceCode, User user);

    boolean validateRetrievalCode(String deviceCode, String code);

    void assignDeviceToBox(String deviceCode, String boxCode, User user);
}