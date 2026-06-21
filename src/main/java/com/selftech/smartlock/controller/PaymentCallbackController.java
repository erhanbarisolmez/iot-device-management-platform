package com.selftech.smartlock.controller;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.iyzipay.Options;
import com.iyzipay.model.CheckoutForm;
import com.iyzipay.model.Locale;
import com.iyzipay.request.RetrieveCheckoutFormRequest;
import com.selftech.smartlock.service.concretes.LockBoxOperationManager;

import lombok.RequiredArgsConstructor;

@Controller
@RequiredArgsConstructor
public class PaymentCallbackController {
  
  private static final Logger logger = LoggerFactory.getLogger(PaymentCallbackController.class);

  @Value("${iyzico.api-key}")
  private String apiKey;

  @Value("${iyzico.secret-key}")
  private String secretKey;

  @Value("${iyzico.base-url}")
  private String baseUrl;

  private final LockBoxOperationManager lockBoxOperationManager; // Kilit açma kodunu üretmek için

  @PostMapping("/payment/callback")
  public String handleCallback(@RequestParam("token") String token, Model model) {

    Options options = new Options();
    options.setApiKey(apiKey);
    options.setSecretKey(secretKey);
    options.setBaseUrl(baseUrl);

    try {
      // Ödeme sonucunu doğrula
      RetrieveCheckoutFormRequest request = new RetrieveCheckoutFormRequest();
      request.setLocale(Locale.TR.getValue());
      request.setToken(token);

      CheckoutForm result = CheckoutForm.retrieve(request, options);

      // --- DETAYLI LOGLAMA EKLENDİ ---
      logger.info("Iyzico callback result received: {}", result);

      // Iyzico'dan gelen conversationId, bizim gönderdiğimiz lockCode'dur.
      String lockCode = result.getConversationId();

      // --- GÜVENLİK ÖNLEMİ / WORKAROUND ---
      // Iyzico'dan dönen ana nesnede conversationId null gelebiliyor.
      // Bu durumda, "BASKET_" + lockCode formatında set ettiğimiz basketId'den lockCode'u alıyoruz.
      if (lockCode == null || lockCode.isEmpty()) {
        String basketId = result.getBasketId();
        if (basketId != null && basketId.startsWith("BASKET_")) {
          lockCode = basketId.substring(7); // "BASKET_" (7 karakter) sonrasını al.
          logger.warn("conversationId null geldi. lockCode, basketId'den alındı: {}", lockCode);
        }
      }

      if ("SUCCESS".equals(result.getPaymentStatus()) && lockCode != null && !lockCode.isEmpty()) {
        logger.info("Ödeme başarılı. LockCode: {}, PaymentId: {}", lockCode, result.getPaymentId());
        // Ödeme başarılı
        // Kilit açma kodunu üret ve SMS gönder
        // paymentId'yi de operasyona kaydetmek için servise gönderiyoruz.
        String paymentId = result.getPaymentId();
        lockBoxOperationManager.finalizePaymentAndGenerateUnlockCode(lockCode, paymentId);

        model.addAttribute("paymentId", result.getPaymentId());
        model.addAttribute("lockCode", lockCode);
        model.addAttribute("amount", result.getPaidPrice());
        // Iyzico'dan gelen milisaniye cinsinden zamanı LocalDateTime'a çevir
        LocalDateTime paymentDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(result.getSystemTime()), ZoneId.systemDefault());
        model.addAttribute("paymentDate", paymentDateTime);
        return "payment-success"; // templates/payment-success.html
      } else {
        // Ödeme başarısız
        logger.error("Ödeme başarısız. LockCode: {}, PaymentStatus: {}, Hata: {}, Hata Kodu: {}", lockCode, result.getPaymentStatus(), result.getErrorMessage(), result.getErrorCode());
        model.addAttribute("error", result.getErrorMessage());
        model.addAttribute("lockCode", lockCode);
        model.addAttribute("errorCode", result.getErrorCode());
        return "payment-failed";
      }

    } catch (Exception e) {
      logger.error("Callback işlenirken kritik hata oluştu. Token: {}", token, e);
      model.addAttribute("error", "Ödeme sonucu doğrulanırken bir hata oluştu: " + e.getMessage());
      return "payment-failed"; // templates/payment-failed.html
    }
  }
}
