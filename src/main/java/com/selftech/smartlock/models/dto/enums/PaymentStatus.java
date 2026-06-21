package com.selftech.smartlock.models.dto.enums;

public enum PaymentStatus {
    PENDING,    // Ödeme bekleniyor
    PROCESSING, // Ödeme işlemde
    SUCCESS,    // Ödeme başarılı
    FAILED,     // Ödeme başarısız
    REFUNDED,   // Ödeme iade edildi
    PARTIALLY_REFUNDED // Kısmi iade
}
