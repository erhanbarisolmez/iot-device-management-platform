package com.selftech.smartlock.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.iyzipay.model.CheckoutFormInitialize;
import com.selftech.smartlock.models.entity.DeviceOperation;
import com.selftech.smartlock.service.abstracts.ILockPaymentService;
import com.selftech.smartlock.service.concretes.LockBoxOperationManager;

import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;

@Controller
@RequiredArgsConstructor
public class PaymentPageController {

    private final LockBoxOperationManager lockBoxOperationManager;
    private final ILockPaymentService lockPaymentManager;

  @GetMapping({"/pay", "/pay/"}) 
    public String showPaymentPage(@RequestParam String code, Model model, HttpServletRequest request) {
        try {
            // 1. Kilit koduna göre operasyonu ve güncel tutarı al
            DeviceOperation lockOperation = lockBoxOperationManager.calculatePenalty(code);

            // 2. Iyzico için checkout formunu hazırla
            CheckoutFormInitialize checkoutFormData = lockPaymentManager.prepareCheckoutForm(lockOperation, request);

            // 3. View'a gerekli bilgileri gönder
            model.addAttribute("lockOperation", lockOperation); // Tüm operasyon bilgisini gönder
            model.addAttribute("checkoutFormContent", checkoutFormData.getCheckoutFormContent());
            model.addAttribute("iyzicoJsUrl", lockPaymentManager.getIyzicoJsUrl());

            return "payment-page"; // templates/payment-page.html
        } catch (Exception e) {
            model.addAttribute("error", "Ödeme sayfası hazırlanırken bir hata oluştu: " + e.getMessage());
            return "payment-failed"; // templates/payment-failed.html
        }
    }

    @GetMapping("/test")
    public String testPage(Model model){
      model.addAttribute("message", "Thymeleaf test sayfası başarıyla çalışıyor!");
      return "test-page";
    }
}