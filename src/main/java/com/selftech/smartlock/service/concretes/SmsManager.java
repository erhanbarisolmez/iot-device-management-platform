package com.selftech.smartlock.service.concretes;

import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import com.selftech.smartlock.config.TwilioConfig;
import com.selftech.smartlock.event.kafka.publisher.SmsEventPublisherService;
import com.selftech.smartlock.models.dto.enums.SmsLogStatus;
import com.selftech.smartlock.models.dto.enums.SmsProviderStatus;
import com.selftech.smartlock.models.entity.DeviceOperation;
import com.selftech.smartlock.models.entity.SmsLog;
import com.selftech.smartlock.repository.SmsLogRepository;
import com.selftech.smartlock.service.abstracts.ISmsService;
import com.selftech.smartlock.utils.exceptions.SmsSendingException;
import com.twilio.http.TwilioRestClient;
import com.twilio.rest.api.v2010.account.Message;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class SmsManager implements ISmsService {

  private final SmsLogRepository smsLogRepository;
  private final TwilioConfig twilioConfig;
  private final TwilioRestClient twilioRestClient;
  private final SmsEventPublisherService smsEventPublisher; // Yeni publisher'ı enjekte et
  private static final Logger logger = LoggerFactory.getLogger(SmsManager.class);
  @Value("${spring.profiles.active:dev}")
  private String activeProfile;
  @Value("${app.base-url:https://local.selfpark.com}")
  private String appBaseUrl;

  private void sendSms(DeviceOperation lockOperation, String message) {

    // Bu, geliştirme sırasında Twilio limitlerine takılmayı önler.
    if ("dev".equals(activeProfile)) {
      logger.info("--- DEV MODE: SMS GÖNDERİMİ ATLANIYOR ---");
      logger.info("Alıcı: {}", lockOperation.getVehicle().getUser().getPhone());
      logger.info("Mesaj: {}", message);
      logger.info("-----------------------------------------");
      // Simülasyon için başarılı bir SMS logu oluşturup Kafka olayını tetikleyelim
      SmsLog simulatedSmsLog = saveSmsLog(lockOperation, message, "SIMULATED_SID_" + System.currentTimeMillis(),
          SmsLogStatus.SENT);
      smsEventPublisher.publishSmsStatusEvent(simulatedSmsLog);
      return; // Gerçek gönderim yapmadan metodu sonlandır.
    }

    SmsLog smsLog = null;
    try {

      Message twilioMessage = Message.creator(
          new com.twilio.type.PhoneNumber(lockOperation.getVehicle().getUser().getPhone()),
          new com.twilio.type.PhoneNumber(twilioConfig.getPhoneNumber()),
          message)
          .create(twilioRestClient);

      String messageSid = twilioMessage.getSid();

      logger.info("'{}' numarasına SMS gönderiliyor: '{}'. MessageSid: {}",
          lockOperation.getVehicle().getUser().getPhone(), message, messageSid);
      // --- Simülasyon Sonu ---

      smsLog = saveSmsLog(lockOperation, message, messageSid, SmsLogStatus.SENT);

    } catch (Exception e) {
      logger.error("'{}' numarasına SMS gönderimi başarısız oldu.",
          lockOperation.getVehicle().getUser().getPhone(), e);

      smsLog = saveSmsLog(lockOperation, message, null, SmsLogStatus.FAILED);
      throw new SmsSendingException("SMS gönderimi sırasında bir hata oluştu: " + e.getMessage(), e);
    } finally {
      // Her durumda (başarılı veya başarısız) Kafka olayını yayınla
      if (smsLog != null) {
        smsEventPublisher.publishSmsStatusEvent(smsLog);
      }
    }
  }

  private SmsLog saveSmsLog(DeviceOperation lockOperation, String message, String providerMessageId,
      SmsLogStatus status) {
    SmsLog smsLog = new SmsLog();
    smsLog.setLockOperation(lockOperation);
    smsLog.setPhoneNumber(lockOperation.getVehicle().getUser().getPhone());
    smsLog.setMessage(message);
    smsLog.setSmsProvider(SmsProviderStatus.TWILIO);
    smsLog.setSentAt(LocalDateTime.now());
    smsLog.setStatus(status);
    smsLog.setProviderMessageId(providerMessageId);
    return smsLogRepository.save(smsLog);
  }

  @Override
  public void sendUnlockCodeSms(DeviceOperation lockOperation) {
    String message = "SelfPark ödemeniz alinmistir. Kilit acma kodunuz: " + lockOperation.getUnlockCode()
        + ". Kilidi iade edip depozitonuzu geri almak icin kutu acma kodunuz: "
        + lockOperation.getBoxOpeningCode();
    logger.info(message);
    sendSms(lockOperation, message);
  }

  @Override
  public void sendLockCodeSms(DeviceOperation lockOperation) {

    String message = "Aracınıza SelfPark kilit takıldı. Kod: " + lockOperation.getLockCode()
        + ". Detay/ödeme için \"BILGI\" yazın.";

    // Detaylı bilgi ve ödeme için "BILGI" yazıp mesajı yanıtlayabilirsiniz.

    logger.info(message);
    sendSms(lockOperation, message);
  }

  @Override
  public void sendPaymentAmountSms(DeviceOperation lockOperation) {
    // Ödeme linkine lockCode yerine DeviceOperation'ın ID'sini veya başka bir
    // benzersiz referansı göndermek daha güvenli olabilir.
    // Mevcut isteğin adresinden (örn: http://localhost:8080) temel URL'yi alıp
    // linki oluşturuyoruz.
    String message = paymentMethodMessage(lockOperation);
    logger.info(message);
    sendSms(lockOperation, message);
  }

  public String paymentMethodMessage(DeviceOperation lockOperation) {
    // Konfigürasyondan base URL'i al (Nginx proxy için)
    String webLink = appBaseUrl + "/pay?code=" + lockOperation.getLockCode();
    String appLink = appBaseUrl + "/pay/deeplink?code=" + lockOperation.getLockCode();

    // SMS karakter limitlerini aşmamak için mesaj kısaltıldı.
    String message = "SelfPark Borc: " + lockOperation.getTotalAmount() + " TL.\n"
        + "Web'den ode: " + webLink + "\n"
        + "Uygulamadan ode: " + appLink + "\n"
        + "SMS ile odeme icin 'ODE " + lockOperation.getLockCode() + "' yazip yanitlayin.";

    logger.info(message);
    return message;
  }

  @Override
  public void sendDepositRefundSms(DeviceOperation lockOperation) {
    String message = "SelfPark akıllı kilidi başarıyla iade ettiniz. " + lockOperation.getDepositAmount()
        + " TL depozito iadeniz en kısa sürede gerçekleştirilecektir.";
    logger.info(message);
    sendSms(lockOperation, message);
  }

}
