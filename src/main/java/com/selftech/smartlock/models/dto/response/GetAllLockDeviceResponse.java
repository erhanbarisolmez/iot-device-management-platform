package com.selftech.smartlock.models.dto.response;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Akıllı kilit cihazlarının listelenmesi için response DTO.
 * Circular reference problemini önlemek için ilişkili entity'leri içermez.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class GetAllLockDeviceResponse {
    private Long id;
    private String deviceCode;
    private String status;
    private String retrievalCode;
    private LocalDateTime retrievalCodeExpiry;
    private String currentBoxCode;  // Sadece kutu kodu, full entity değil
    private String currentUserEmail;  // Sadece kullanıcı email, full entity değil
}
