package com.selftech.smartlock.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.selftech.selfparkbackendv001.models.user.User;
import com.selftech.smartlock.models.entity.DeviceOperation;
import com.selftech.smartlock.service.concretes.LockBoxOperationManager;
import com.selftech.smartlock.utils.exceptions.SmartLockException;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v0/personnel/operations")
@Tag(name = "Personnel Operations", description = "Personel (Admin/Manager) tarafından gerçekleştirilen kilit ve kutu operasyonları")
@Slf4j
public class PersonnelOperationController {

  private final LockBoxOperationManager lockBoxOperationManager;

  @Operation(summary = "Bir araca kilit takma işlemini başlatır.", description = "Personel, kutudan bir cihaz alarak belirtilen plakalı araca kilit takar.")
  @PostMapping("/lock-vehicle")
  public ResponseEntity<DeviceOperation> initializeLock(
      @RequestParam String boxCode,
      @RequestParam String plateNumber,
      @AuthenticationPrincipal User personnel) {
    try {
      DeviceOperation lockOperation = lockBoxOperationManager.initializeLock(boxCode, plateNumber, personnel);
      log.info("boxCode: {}, plateNumber: {}, personnel: {} ", boxCode, plateNumber, personnel);
      log.info("lockOperation: {}", lockOperation);
      return ResponseEntity.ok(lockOperation);
    } catch (SmartLockException e) {
      // Uygulamaya özgü hataları (örn: "Kutuda cihaz yok") 400 Bad Request olarak
      // döndürmek daha anlamlıdır.
      log.info("lock vehicle error: {}", e);
      return ResponseEntity.badRequest().body(null); // Veya bir hata nesnesi dönebilirsiniz.
    }
  }

  @Operation(summary = "Personelin bir cihazı kutuya iade etme işlemini yönetir.", description = "Bu işlem depozito iadesi tetiklemez, sadece cihazın durumunu 'Kullanılabilir' yapar.")
  @PostMapping("/return-device-to-box")
  public ResponseEntity<String> returnDeviceToBox(
      @RequestParam String boxCode,
      @RequestParam String deviceCode,
      @AuthenticationPrincipal User personnel) {
    // Personel işlemi olduğu için 'user' parametresi dolu gönderilir.
    lockBoxOperationManager.processReturn(boxCode, deviceCode, null, null, personnel);
    return ResponseEntity.ok("Cihaz personel tarafından kutuya başarıyla iade edildi.");
  }
}
