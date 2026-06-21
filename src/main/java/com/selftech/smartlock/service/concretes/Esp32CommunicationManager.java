package com.selftech.smartlock.service.concretes;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.selftech.smartlock.models.dto.enums.DeviceStatus;
import com.selftech.smartlock.utils.exceptions.SmartLockException;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class Esp32CommunicationManager {
  private final RestTemplate restTemplate;

  public void sendUnlockSignal(String deviceId, String unlockCode) {
    // Burada ESP32'ye kilit açma sinyali gönderme mantığı olacak.
    // Bu bir HTTP isteği, MQTT mesajı veya başka bir iletişim protokolü olabilir.
    System.out.println("ESP32 cihazına kilit açma sinyali gönderildi: " + deviceId);

    String url = "http://" + deviceId + ".local/unlock?Code" + unlockCode;

    try {
      restTemplate.postForEntity(url, null, String.class);
    } catch (Exception e) {
      throw new SmartLockException("Cihaz ile iletişim kurulamadı" + e.getMessage()); 
    }

  }

  public void sendLockSignal(String deviceId) {
    // Burada ESP32'ye kilit kapatma sinyali gönderme mantığı olacak.
    System.out.println("ESP32 cihazına kilit kapatma sinyali gönderildi: " + deviceId);
  }

  public DeviceStatus getDeviceStatus(String deviceId){
    // ESO32'den cihaz durumunu sorgula
    String url = "http://" + deviceId + ".local/status";
    try {
      ResponseEntity<DeviceStatus> response = restTemplate.getForEntity(url, DeviceStatus.class);
      return response.getBody();

    } catch (Exception e) {
      throw new SmartLockException("Cihaz durumu alınmadı:" + e.getMessage());
    }
  }

}
