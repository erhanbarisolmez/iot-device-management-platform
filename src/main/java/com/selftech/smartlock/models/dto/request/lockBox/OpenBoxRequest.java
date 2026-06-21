package com.selftech.smartlock.models.dto.request.lockBox;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OpenBoxRequest {
    private String code;
}