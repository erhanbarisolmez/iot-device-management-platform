package com.selftech.smartlock.controller;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.selftech.smartlock.models.dto.request.mockPayment.MockPaymentEvent;
import com.selftech.smartlock.models.entity.DeviceOperation;
import com.selftech.smartlock.repository.LockDeviceOperationRepository;
import com.selftech.smartlock.service.concretes.SmsManager;

import io.swagger.v3.oas.annotations.Operation;
import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/sms/webhook")
@RequiredArgsConstructor
public class TwilioSmsWebHookController {

  private final SmsManager smsManager;
  private final LockDeviceOperationRepository lockDeviceOperationRepository;
  private final PayguruMockWebhookController payguruMockWebhookController;
  private static final Logger logger = LoggerFactory.getLogger(TwilioSmsWebHookController.class);

  @Operation(summary = "Twilio Webhook ile yanıt alma.", description = "SMS ile Twilio mesaj göndererek yanıt alır")
  @PostMapping(consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
  public ResponseEntity<String> receiveSms(
      @RequestParam("From") String from,
      @RequestParam("Body") String body,
      @RequestParam("To") String to) {
    
    logger.info("Gelen SMS: '{}' | Kimden: {} | Kime: {}", body, from, to);
    String responseMessage;
    String[] parts = body.trim().toUpperCase().split("\\s+");
    String command = parts.length > 0 ? parts[0] : "";
    String lockCode = parts.length > 1 ? parts[1] : null;
    
    if (lockCode == null) {
        responseMessage = "Geçersiz format. Lütfen 'BILGI <kod>' veya 'ODE <kod>' formatında gönderin.";
        return createTwimlResponse(responseMessage);
    }

    Optional<DeviceOperation> lockOperationOpt = lockDeviceOperationRepository.findByLockCode(lockCode);

    if (lockOperationOpt.isEmpty()) {
        responseMessage = "Geçersiz işlem kodu: " + lockCode;
        return createTwimlResponse(responseMessage);
    }

    DeviceOperation lockOperation = lockOperationOpt.get();

    switch (command) {
        case "BILGI":
            responseMessage = smsManager.paymentMethodMessage(lockOperation);
            break;

        case "ODE":
            // Payguru simülasyonunu tetikle
            MockPaymentEvent mockPaymentEvent = new MockPaymentEvent(
                "mock-trx-" + System.currentTimeMillis(),
                lockCode,
                from,
                "SUCCESS",
                lockOperation.getTotalAmount());

            payguruMockWebhookController.mockCallback(mockPaymentEvent);
            
            // Kullanıcıya ödemenin alındığına dair bir geri bildirim mesajı
            responseMessage = "Mobil ödeme talebiniz alınmıştır. İşlem sonucu kısa süre içinde SMS ile bildirilecektir. LockCode: " + lockCode;
            break;

        default:
            responseMessage = "Anlaşılmayan komut: " + command + ". Lütfen 'BILGI' veya 'ODE' komutlarını kullanın.";
            break;
    }

    return createTwimlResponse(responseMessage);
  }

  private ResponseEntity<String> createTwimlResponse(String message) {
    // Twilio’ya TwiML ile cevap döndür
    String twiml = "<Response><Message>" + message + "</Message></Response>";
    return ResponseEntity.ok()
        .contentType(MediaType.APPLICATION_XML)
        .body(twiml);
  }
}
