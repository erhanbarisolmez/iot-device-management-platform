package com.selftech.smartlock.models.dto.enums;

public enum LockStatus {
    AVAILABLE,          // Kilit kullanıma hazır
    IN_USE,             // Kilit kullanımda
    MAINTENANCE,        // Bakımda
    OUT_OF_SERVICE,     // Hizmet dışı
    LOCKED,             // Kilitli (araçta)
    UNLOCKED,           // Kilidi açılmış (araçtan çıkarılmış)
    RETURNED,           // Kutuya iade edilmiş
    AWAITING_PAYMENT,   // Ödeme bekleniyor
    PAYMENT_CONFIRMED   // Ödeme onaylandı
}
