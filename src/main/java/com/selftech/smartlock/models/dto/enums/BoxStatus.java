package com.selftech.smartlock.models.dto.enums;

public enum BoxStatus {
    EMPTY,          // Kutu boş
    OCCUPIED,       // Kutu dolu (içinde kilit var)
    MAINTENANCE,    // Bakımda
    OUT_OF_SERVICE, // Hizmet dışı
    LOW_BATTERY,    // Pil seviyesi düşük
    SENSOR_ERROR    // Sensör hatası
}
