package com.selftech.smartlock.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.selftech.smartlock.models.dto.request.sensor.SensorDataRequest;
import com.selftech.smartlock.models.entity.DeviceOperation;
import com.selftech.smartlock.service.abstracts.ILockBoxOperationService;
import com.selftech.smartlock.service.abstracts.ILockPaymentService;

import io.swagger.v3.oas.annotations.parameters.RequestBody;
import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/v0/device-data")
@RequiredArgsConstructor
public class DeviceDataController {
  private final ILockBoxOperationService lockBoxOperationManager;
  private final ILockPaymentService paymentManager;

  // Bu endpoint, planlanan MQTT mimarisine geçildiğinde kaldırılacaktır.
  // Cihaz verileri artık bir MQTT listener tarafından alınacaktır.
  // Geçici olarak kimlik doğrulama adımı kaldırılmıştır.
  @PostMapping("/sensor")
  public ResponseEntity<String> receiveSensorData(@RequestBody SensorDataRequest request) {
    // TODO: Bu endpoint'in içeriği MQTT mimarisine taşınacak.
    return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).body("Bu endpoint kullanımdan kaldırılmıştır. Lütfen MQTT kullanın.");
  }

  @PostMapping("/confirm")
  public ResponseEntity<DeviceOperation> confirmPayment(@RequestParam String lockCode) {
    try {
      boolean paymentSuccess = paymentManager.processPayment(lockCode);
      if (paymentSuccess) {
        DeviceOperation operation = lockBoxOperationManager.finalizePaymentAndGenerateUnlockCode(lockCode, null);
        return ResponseEntity.ok(operation);
      }
    } catch (Exception e) {
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
    }
    return null;
  }

}
