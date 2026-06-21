package com.selftech.smartlock.service.abstracts;

import com.selftech.smartlock.shared.model.User;
import com.selftech.smartlock.models.dto.request.sensor.SensorDataRequest;
import com.selftech.smartlock.models.entity.DeviceOperation;

public interface ILockBoxOperationService {
   DeviceOperation initializeLock(String boxCode, String plateNumber, User user);

   DeviceOperation calculatePenalty(String lockCode);

   DeviceOperation finalizePaymentAndGenerateUnlockCode(String lockCode, String paymentId);

   boolean unlock(String unlockCode);

   void processReturn(String boxCode, String deviceCode, Double weightMeasured, Boolean doorOpen, User user);

   void processDeviceData(String boxCode, SensorDataRequest sensorData);

   DeviceOperation validateAndOpenBoxForCustomer(String boxOpeningCode);

   void processCustomerReturnByCode(String boxOpeningCode);
}
