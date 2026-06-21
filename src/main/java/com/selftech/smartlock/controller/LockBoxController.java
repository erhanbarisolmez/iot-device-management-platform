package com.selftech.smartlock.controller;

import java.util.List;

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
import org.springframework.web.bind.annotation.RestController;

import com.selftech.smartlock.shared.model.User;
import com.selftech.smartlock.shared.mapper.ModelMapperService;
import com.selftech.smartlock.models.dto.request.lockBox.LockBoxAddRequest;
import com.selftech.smartlock.models.dto.request.lockBox.LockBoxUpdateRequest;
import com.selftech.smartlock.models.dto.request.lockBox.OpenBoxRequest;
import com.selftech.smartlock.models.dto.response.GetAllLockBoxResponse;
import com.selftech.smartlock.models.entity.LockBox;
import com.selftech.smartlock.service.concretes.LockBoxManager;

import io.swagger.v3.oas.annotations.Operation;
import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/v0/lock-box")
@RequiredArgsConstructor
public class LockBoxController {
    private final LockBoxManager lockBoxManager; // Bu zaten doğru, arayüze bağımlı.
    private final ModelMapperService modelMapperService;

    @Operation(summary = "Akıllı kilitlerin saklandığı kutuları listeler.")
    @GetMapping
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<List<GetAllLockBoxResponse>> getAllBoxes() {
        List<LockBox> boxes = lockBoxManager.getAllReturnBoxes();
        List<GetAllLockBoxResponse> response = boxes.stream()
            .map(box -> modelMapperService.forResponse().map(box, GetAllLockBoxResponse.class))
            .toList();
        return ResponseEntity.ok(response);
    }

    @Operation(summary = "Yeni iade kutusu oluşturur", description = "Sisteme SmartLock cihazlarının bırakılacağı yeni bir iade kutusu ekler.")
    @PostMapping("/add")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<LockBox> createBox(@RequestBody LockBoxAddRequest returnBoxRequest, @AuthenticationPrincipal User user) { // @RequestBody Spring'e
                                                                                                // aittir.
        LockBox returnBox = modelMapperService.forRequest().map(returnBoxRequest, LockBox.class);
        return ResponseEntity.ok(lockBoxManager.createReturnBox(returnBox, user)); // Değişiklik yok
    }

    @Operation(summary = "İade kutusunu günceller", description = "İade edileceği kutularını bilgilerini günceller.")
    @PutMapping("/update")
    @PreAuthorize("hasAnyRole('ADMIN', 'MANAGER')") // Multiple roles
    public ResponseEntity<LockBox> updatedBox(@RequestBody LockBoxUpdateRequest dto, @AuthenticationPrincipal User user) {
        LockBox returnBoxes = modelMapperService.forUpdate().map(dto, LockBox.class);
        return ResponseEntity.ok(lockBoxManager.updateReturnBox(returnBoxes, user)); // Değişiklik yok
    }

    @Operation(summary = "LockBox id ile ilgili kutuyu siler", description = "Cihaz id'si ile siler")
    @DeleteMapping("/delete/{id}")
    @PreAuthorize("hasAuthority('admin:delete')") // Permission bazlı
    public void deleteBox(@PathVariable Long id, @AuthenticationPrincipal User user) {
        lockBoxManager.deleteReturnBox(id, user); // Değişiklik yok
    }

    @Operation(summary = "Personel için kutu açma kodu üretir.", description = "Belirtilen kutu kodu için 6 haneli geçici bir açma kodu oluşturur ve döndürür.")
    @GetMapping("/{boxCode}/generate-opening-code")
    @PreAuthorize("hasAnyRole('ADMIN', 'MANAGER')")
    public ResponseEntity<String> generateOpeningCode(@PathVariable String boxCode, @AuthenticationPrincipal User user) {
        System.out.println("WHO" + user.getEmail());
        String openingCode = lockBoxManager.generateOpeningCode(boxCode, user);
        return ResponseEntity.ok(openingCode);
    }

    @Operation(summary = "Kutu açma kodunu doğrulayarak kutuyu açar.", description = "Sağlanan kod geçerliyse ve süresi dolmamışsa kutuyu açmak için sinyal gönderir.")
    @PostMapping("/{boxCode}/open")
    @PreAuthorize("hasAnyRole('ADMIN', 'MANAGER')")
    public ResponseEntity<String> openBox(@PathVariable String boxCode, @RequestBody OpenBoxRequest request, @AuthenticationPrincipal User user) {
        boolean opened = lockBoxManager.openBoxWithCode(boxCode, request.getCode(), user);
        if (opened) {
            // Başarılı olursa, kod kullanıldığı için geçersiz kılınır.
            return ResponseEntity.ok("Kutu açma sinyali başarıyla gönderildi.");
        } else {
            // Başarısız olursa, istemciye nedenini bildirmek daha iyi olabilir.
            return ResponseEntity.badRequest().body("Geçersiz veya süresi dolmuş kod.");
        }
    }
}
