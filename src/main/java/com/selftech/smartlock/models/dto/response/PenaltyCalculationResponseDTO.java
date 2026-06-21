package com.selftech.smartlock.models.dto.response;

import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class PenaltyCalculationResponseDTO {
    private String lockCode;
    private BigDecimal penaltyAmount;
    private BigDecimal lockFee;
    private BigDecimal depositAmount;
    private BigDecimal totalAmount;
    private String depositRefundMessage; // Müşteriye depozito iadesi hakkında bilgi
}