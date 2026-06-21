package com.selftech.smartlock.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.selftech.smartlock.service.concretes.LockBoxOperationManager;

import lombok.RequiredArgsConstructor;

@RestController("smartLockPaymentController") // Bean adını benzersiz hale getiriyoruz.
@RequestMapping("/api/v0/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final LockBoxOperationManager lockBoxOperationManager;

    // Bu endpoint, Iyzico gibi bir ödeme sağlayıcısından gelen callback'i işlemek için kullanılır.
    @PostMapping("/callback")
    public ResponseEntity<String> handlePaymentCallback(@RequestBody String payload) { // Gerçekte bir DTO kullanılmalı
        // 1. Gelen payload'u doğrula ve içinden ilgili lockCode'u veya işlem ID'sini al.
        // 2. lockBoxOperationManager.finalizePaymentAndGenerateUnlockCode(lockCode) çağır.
        lockBoxOperationManager.finalizePaymentAndGenerateUnlockCode(payload, null);
        // 3. SMS servisi ile açma kodunu müşteriye gönder.
        return ResponseEntity.ok("Payment processed successfully.");
    }

    
}
