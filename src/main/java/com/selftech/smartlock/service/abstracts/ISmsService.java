package com.selftech.smartlock.service.abstracts;

import com.selftech.smartlock.models.entity.DeviceOperation;

public interface ISmsService {
    void sendUnlockCodeSms(DeviceOperation lockOperation);

    void sendLockCodeSms(DeviceOperation lockOperation);

    void sendPaymentAmountSms(DeviceOperation lockOperation);

    void sendDepositRefundSms(DeviceOperation lockOperation);
}