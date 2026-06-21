package com.selftech.smartlock.controller;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.selftech.smartlock.shared.model.User;
import com.selftech.smartlock.shared.mapper.ModelMapperService;
import com.selftech.smartlock.models.dto.request.lockDevice.LockDeviceRequest;
import com.selftech.smartlock.models.entity.LockDevice;
import com.selftech.smartlock.service.concretes.LockDeviceManager;
import com.selftech.smartlock.utils.exceptions.SmartLockException;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/v0/lock-device")
@RequiredArgsConstructor
public class LockDeviceController {
    private final LockDeviceManager lockDeviceManager;
    private final ModelMapperService modelMapperService;

    @Operation(summary = "Yeni bir akıllı kilit cihazı oluşturur", description = "Araçlarda cezai işlem için kullanılacak akıllı kilit cihazı oluşturur.", requestBody = @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "Oluşturulacak kilit cihazı bilgileri", required = true, content = @Content(schema = @Schema(implementation = LockDeviceRequest.class))))
    @PostMapping("/add")
    @PreAuthorize("hasAnyRole('ADMIN', 'MANAGER')")
    public ResponseEntity<LockDevice> createDevice(@Valid @RequestBody LockDeviceRequest request,
            @AuthenticationPrincipal User user) {
         String email = user.getEmail();
         System.out.println("email:" + email);
        LockDevice createdDevice = lockDeviceManager.createDevice(request, user);
        return new ResponseEntity<>(createdDevice, HttpStatus.CREATED);
    }

    @Operation(summary = "Tüm akıllı kilit cihazlarını listeler.")
    @GetMapping
    public ResponseEntity<List<LockDevice>> getAllDevices() {
        return ResponseEntity.ok(lockDeviceManager.getAllDevices());
    }

    @Operation(summary = "Verilen koda sahip akıllı kilit cihazını getirir.")
    @GetMapping("/{deviceCode}")
    public ResponseEntity<LockDevice> getDeviceByCode(@PathVariable String deviceCode) {
        LockDevice device = lockDeviceManager.findByDeviceCode(deviceCode)
                .orElseThrow(() -> new SmartLockException("Cihaz bulunamadı: " + deviceCode));
        return ResponseEntity.ok(device);
    }

    @Operation(summary = "Mevcut bir akıllı kilit cihazını günceller.")
    @PutMapping("/{id}")
    @PreAuthorize("hasAnyRole('ADMIN', 'MANAGER')")
    public ResponseEntity<LockDevice> updateDevice(@PathVariable Long id, @RequestBody LockDevice deviceDetails, @AuthenticationPrincipal User user) {
        return ResponseEntity.ok(lockDeviceManager.updateLockDevice(id, deviceDetails, user));
    }

    @Operation(summary = "Bir akıllı kilit cihazını siler.")
    @DeleteMapping("/{id}")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<Void> deleteDevice(@PathVariable Long id, @AuthenticationPrincipal User user) {
        lockDeviceManager.deleteLockDevice(id, user);
        return ResponseEntity.noContent().build();
    }

    @Operation(summary = "Personnel, cihaz geri alma kodu üretir.")
    @PostMapping("/{deviceCode}/generate-retrieval-code")
    @PreAuthorize("hasAnyRole('ADMIN', 'MANAGER')")
    public ResponseEntity<String> generateRetrievalCode(
            @PathVariable String deviceCode,
            @AuthenticationPrincipal User user) {
        String code = lockDeviceManager.generateRetrievalCode(deviceCode, user);
        return ResponseEntity.ok(code);
    }

    @Operation(summary = "Cihaz geri alma kodunu doğrular.")
    @PostMapping("/{deviceCode}/validate-retrieval-code/{code}")
    public ResponseEntity<Boolean> validateRetrievalCode(
            @PathVariable String deviceCode,
            @PathVariable String code) { // 'code' artık URL'den alınacak.
        boolean isValid = lockDeviceManager.validateRetrievalCode(deviceCode, code);
        return ResponseEntity.ok(isValid);
    }

    @Operation(summary = "Bir akıllı kilit cihazını bir kutuya atar.", description = "Personelin, belirtilen cihazı belirtilen kutuya yerleştirmesini ve durumunu 'Kullanılabilir' olarak ayarlamasını sağlar.")
    @PostMapping("/assign-to-box")
    @PreAuthorize("hasAnyRole('ADMIN', 'MANAGER')")
    public ResponseEntity<String> assignDeviceToBox(
            @RequestParam String deviceCode,
            @RequestParam String boxCode,
            @AuthenticationPrincipal User user) {
        lockDeviceManager.assignDeviceToBox(deviceCode, boxCode, user);
        return ResponseEntity.ok("Cihaz " + deviceCode + ", " + boxCode + " kutusuna başarıyla atandı.");
    }

}
