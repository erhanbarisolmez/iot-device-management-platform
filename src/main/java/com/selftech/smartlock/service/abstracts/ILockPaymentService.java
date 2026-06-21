package com.selftech.smartlock.service.abstracts;

import com.iyzipay.model.CheckoutFormInitialize;
import com.iyzipay.model.Payment;
import com.selftech.smartlock.models.entity.DeviceOperation;

import jakarta.servlet.http.HttpServletRequest;

public interface ILockPaymentService {
    void initiatePayment(DeviceOperation lockOperation);
    void refundDeposit(DeviceOperation lockOperation);
    boolean processPayment(String lockCode);
    Payment retrievePaymentDetail(String lockCode);
    CheckoutFormInitialize prepareCheckoutForm(DeviceOperation lockOperation, HttpServletRequest request);
    String getIyzicoJsUrl();
}