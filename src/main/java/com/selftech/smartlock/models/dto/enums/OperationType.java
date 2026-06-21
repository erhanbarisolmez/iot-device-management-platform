package com.selftech.smartlock.models.dto.enums;

public enum OperationType {
  LOCK_DEVICE, // Cihaz kilitleme
  UNLOCK_DEVICE, // Cihaz kilidi açma
  RETURN_TO_BOX, // Kutuya iade
  TAKE_FROM_BOX, // Kutudan alma
  MAINTENANCE_START, // Bakım başlangıcı
  MAINTENANCE_END, // Bakım bitişi
  STATUS_CHANGE, // Durum değişikliği
  GENERATE_OPENING_CODE, // Personel için kutu açma kodu üretme
  CREATE_DEVICE // Yeni cihaz oluşturma
}
