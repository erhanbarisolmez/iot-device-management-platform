package com.selftech.smartlock.models.dto.request.lockDevice;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LockDeviceRequest {
    private String deviceCode;
    private String publicKey;

}
