package com.selftech.smartlock.controller;

import java.time.LocalDateTime;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.selftech.smartlock.models.dto.enums.LockStatus;
import com.selftech.smartlock.models.dto.enums.OperationType;
import com.selftech.smartlock.models.dto.enums.PaymentStatus;
import com.selftech.smartlock.models.dto.request.mockPayment.MockPaymentEvent;
import com.selftech.smartlock.models.entity.DeviceOperation;
import com.selftech.smartlock.repository.LockDeviceOperationRepository;
import com.selftech.smartlock.service.concretes.SmsManager;
import com.selftech.smartlock.utils.OTPGenerator;

import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v0/payguru/webhook")
public class PayguruMockWebhookController {
  
  private final LockDeviceOperationRepository lockDeviceOperationRepository;

  private final Logger logger = LoggerFactory.getLogger(PayguruMockWebhookController.class);

  private final SmsManager smsManager;

  @PostMapping("/callback")
  public ResponseEntity<String> mockCallback(@RequestBody MockPaymentEvent event) {
    
    System.out.println("Mock Payguru callback geldi: " + event);

    Optional<DeviceOperation> lockOperOptional = lockDeviceOperationRepository.findByLockCode(event.getLockCode());

    if (lockOperOptional.isEmpty()) {
      logger.warn("Mock Payguru callback için geçersiz lockCode alındı: {}", event.getLockCode());
      return ResponseEntity.badRequest().body("Invalid lockCode");
    }

    DeviceOperation op = lockOperOptional.get();
    op.setOperationTime(LocalDateTime.now());
    op.setPaymentConfirmedAt(LocalDateTime.now());
    op.setOperationType(OperationType.STATUS_CHANGE); // Bu bir durum değişikliği işlemidir.

    if ("SUCCESS".equalsIgnoreCase(event.getStatus())) {
      // Kilit açma ve kutu açma kodlarını oluştur
      String unlockCode = OTPGenerator.generate(6);
      String boxOpeningCode = "C" + OTPGenerator.generate(7);

      op.setUnlockCode(unlockCode);
      op.setBoxOpeningCode(boxOpeningCode);
      op.setPaymentId(event.getTransactionId());
      op.setPaymentStatus(PaymentStatus.SUCCESS);
      op.setStatus(LockStatus.PAYMENT_CONFIRMED);
      op.setNotes("Payguru SMS ile ödeme tamamlandı. Plaka: " + op.getVehicle().getPlate().getPlateNumber());

      lockDeviceOperationRepository.save(op);
      logger.info("Ödeme tamamlandı. LockCode: {}, UnlockCode: {}, BoxOpeningCode: {}, Plaka: {}",
          op.getLockCode(), unlockCode, boxOpeningCode, op.getVehicle().getPlate().getPlateNumber());

      // Ödeme başarılı olduğu için kilit açma kodunu gönder
      smsManager.sendUnlockCodeSms(op);

      return ResponseEntity.ok("Ödeme tamamlandı");
    } else { // FAILED veya diğer durumlar
      op.setPaymentStatus(PaymentStatus.FAILED);
      op.setNotes("Payguru SMS ile ödeme başarısız oldu. Plaka: " + op.getVehicle().getPlate().getPlateNumber());
      lockDeviceOperationRepository.save(op); // Başarısız durumu da kaydet
      logger.warn("Ödeme başarısız oldu. LockCode: {}", op.getLockCode());
    }
    return ResponseEntity.ok("OK");
  }
}
