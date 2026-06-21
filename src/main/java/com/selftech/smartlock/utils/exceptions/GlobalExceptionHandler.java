package com.selftech.smartlock.utils.exceptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import com.selftech.smartlock.models.dto.response.ApiResponse;

@RestControllerAdvice
public class GlobalExceptionHandler {

  private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);
  
  @ExceptionHandler(SmartLockException.class)
  public ResponseEntity<ApiResponse> handleSmartLockException(SmartLockException ex) {
    return ResponseEntity.badRequest().body(ApiResponse.error(ex.getMessage()));
  }

  @ExceptionHandler(SmsSendingException.class)
  public ResponseEntity<ApiResponse> handleSmsSendingException(SmsSendingException ex) {
    // Hatayı sunucu loglarına detaylı olarak yazıyoruz.
    logger.error("SMS gönderim hatası global handler tarafından yakalandı: ", ex);
    // İstemciye genel bir hata mesajı dönüyoruz.
    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ApiResponse.error("SMS gönderimi sırasında bir sunucu hatası oluştu."));
  }
}
