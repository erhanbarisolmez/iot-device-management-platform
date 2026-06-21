package com.selftech.smartlock.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.iyzipay.model.Payment;
import com.selftech.smartlock.shared.mapper.ModelMapperService;
import com.selftech.smartlock.models.dto.response.PenaltyCalculationResponseDTO;
import com.selftech.smartlock.models.entity.DeviceOperation;
import com.selftech.smartlock.service.abstracts.ILockBoxOperationService;
import com.selftech.smartlock.service.abstracts.ILockPaymentService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v0/customer/operations")
@Tag(name = "Customer Operations", description = "Müşteri tarafından gerçekleştirilen kilit operasyonları")
public class CustomerOperationController {

    private final ILockBoxOperationService lockBoxOperationManager;
    private final ILockPaymentService lockPaymentManager;
    private final ModelMapperService modelMapperService;

    @Operation(summary = "Kilit koduna göre ceza ve ödeme tutarını hesaplar.", description = "Müşteri, aracındaki kilit kodunu girerek ödemesi gereken toplam tutarı öğrenir.")
    @PostMapping("/calculate-fee")
    public ResponseEntity<PenaltyCalculationResponseDTO> calculateFee(@RequestParam String lockCode) {
        DeviceOperation lockOperation = lockBoxOperationManager.calculatePenalty(lockCode);
        
        // DeviceOperation'ı PenaltyCalculationResponseDTO'ya dönüştürüyoruz
        PenaltyCalculationResponseDTO responseDTO = modelMapperService.forResponse().map(lockOperation, PenaltyCalculationResponseDTO.class);
        responseDTO.setDepositRefundMessage("Kilidi kutuya iade ettiğinizde " + lockOperation.getDepositAmount() + " TL depozito hesabınıza iade edilecektir.");

        return ResponseEntity.ok(responseDTO);
    }

    @Operation(summary = "Bir ödemenin durumunu Iyzico'dan sorgular.", description = "lockCode (conversationId) kullanarak bir ödemenin güncel durumunu doğrudan Iyzico'dan alır.")
    @GetMapping("/payment-status")
    public ResponseEntity<Payment> getPaymentStatus(@RequestParam String lockCode) {
        Payment paymentDetails = lockPaymentManager.retrievePaymentDetail(lockCode);
        return ResponseEntity.ok(paymentDetails);
    }

    @Operation(summary = "Ödeme sonrası alınan kilit açma kodunu kullanarak kilidi açar.")
    @PostMapping("/unlock-device")
    public ResponseEntity<String> unlockDevice(@RequestParam String unlockCode) {
        boolean unlocked = lockBoxOperationManager.unlock(unlockCode);
        if (unlocked) {
            return ResponseEntity.ok("Kilit başarıyla açıldı. Cihazı en yakın SelfPark kutusuna iade edebilirsiniz.");
        } else {
            return ResponseEntity.badRequest().body("Geçersiz veya daha önce kullanılmış kilit açma kodu.");
        }
    }

    @Operation(summary = "Müşterinin, iade için kutu açma kodunu kullanmasını sağlar.", description = "Ödeme sonrası SMS ile gelen kutu açma kodu doğrulanır ve kutu açılır.")
    @PostMapping("/open-return-box")
    public ResponseEntity<String> openReturnBox(@RequestParam String boxOpeningCode) {
        // Bu metot kodu doğrular ve kutu açma sinyalini tetikler.
        // İade işleminin tamamlanması (processReturn) kutu sensörlerinden gelecek veri ile olur.
        lockBoxOperationManager.validateAndOpenBoxForCustomer(boxOpeningCode);
        return ResponseEntity.ok("Kod doğrulandı. Kutuyu açabilirsiniz. Kilidi yerleştirdikten sonra kapağı kapatınız.");
    }


    @Operation(summary = "Müşterinin kilidi kutuya iade etme işlemini tamamlar.", description = "Müşteri, SMS ile aldığı kutu açma kodunu kullanarak iade işlemini onaylar ve depozito iadesini başlatır.")
    @PostMapping("/return-device")
    public ResponseEntity<String> returnDevice(@RequestParam String boxOpeningCode) {
        lockBoxOperationManager.processCustomerReturnByCode(boxOpeningCode);
        return ResponseEntity.ok("Cihaz iade işlemi başarıyla alındı. Depozito iadeniz başlatıldı.");
    }
}