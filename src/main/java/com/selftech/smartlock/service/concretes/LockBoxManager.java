package com.selftech.smartlock.service.concretes;

import java.time.LocalDateTime;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.selftech.selfparkbackendv001.models.user.User;
import com.selftech.smartlock.event.kafka.publisher.LockBoxEventPublisherService;
import com.selftech.smartlock.models.dto.enums.OperationType;
import com.selftech.smartlock.models.entity.BoxOperation;
import com.selftech.smartlock.models.entity.LockBox;
import com.selftech.smartlock.repository.LockBoxOperationRepository;
import com.selftech.smartlock.repository.LockBoxRepository;
import com.selftech.smartlock.service.abstracts.ILockBoxService;
import com.selftech.smartlock.utils.OTPGenerator;
import com.selftech.smartlock.utils.exceptions.SmartLockException;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class LockBoxManager implements ILockBoxService {

  private static final Logger logger = LoggerFactory.getLogger(LockBoxManager.class);

  private final Esp32CommunicationManager esp32CommunicationManager; // ESP32 servisini enjekte et
  private final LockBoxRepository lockBoxRepository;
  private final LockBoxOperationRepository boxOperationRepository;
  private final LockBoxEventPublisherService lockBoxEventPublisher;

  @Override
  @Transactional
  public LockBox createReturnBox(LockBox lockBox, User user) {
    if (lockBoxRepository.existsByBoxCode(lockBox.getBoxCode())) {
      throw new IllegalArgumentException("BoxCode zaten mevcut: " + lockBox.getBoxCode());
    }
    LockBox savedBox = lockBoxRepository.save(lockBox);

    // Oluşturma işlemini logla
    BoxOperation boxOperation = new BoxOperation();
    boxOperation.setLockBox(savedBox);
    boxOperation.setUser(user);
    boxOperation.setOperationType(OperationType.CREATE_DEVICE); // Genel bir tip kullanılabilir
    boxOperation.setOperationTime(LocalDateTime.now());
    boxOperation.setNotes("Yeni kutu sisteme eklendi.");
    boxOperation.setPreviousStatus(null);
    boxOperation.setNewStatus(savedBox.getStatus().name());
    boxOperationRepository.save(boxOperation);

    // Kafka'ya box created event'i outbox pattern ile gönder
    // Aynı transaction içinde outbox'a kaydedilir (reliable delivery)
    publishBoxCreatedEventReliable(savedBox);

    return savedBox;
  }

  @Override
  @Transactional
  public LockBox updateReturnBox(LockBox lockBox, User user) {
    if (lockBox.getId() == null) {
      throw new IllegalArgumentException("ID ZORUNLU");
    }
    LockBox existingBox = lockBoxRepository.findById(lockBox.getId())
        .orElseThrow(() -> new SmartLockException("Kutu bulunamadı: " + lockBox.getId()));

    String previousStatus = existingBox.getStatus().name();

    // Gelen bilgilerle güncelle
    existingBox.setBoxCode(lockBox.getBoxCode());
    existingBox.setLocation(lockBox.getLocation());
    existingBox.setStatus(lockBox.getStatus());
    LockBox updatedBox = lockBoxRepository.save(existingBox);

    // Güncelleme işlemini logla
    BoxOperation boxOperation = new BoxOperation();
    boxOperation.setLockBox(updatedBox);
    boxOperation.setUser(user);
    boxOperation.setOperationType(OperationType.STATUS_CHANGE);
    boxOperation.setOperationTime(LocalDateTime.now());
    boxOperation.setNotes("Kutu bilgileri güncellendi.");
    boxOperation.setPreviousStatus(previousStatus);
    boxOperation.setNewStatus(updatedBox.getStatus().name());
    boxOperationRepository.save(boxOperation);

    // Kafka'ya box status changed event'i outbox pattern ile gönder
    // Aynı transaction içinde outbox'a kaydedilir (reliable delivery)
    publishBoxStatusChangedEventReliable(updatedBox, previousStatus);

    return updatedBox;
  }

  @Override
  public List<LockBox> getAllReturnBoxes() {
    return lockBoxRepository.findAll();
  }

  @Override
  public void deleteReturnBox(Long id, User user) {
    // Silme işlemi yerine durumu değiştirmek daha güvenli olabilir.
    // Bu örnekte direkt silme yapılıyor.
    lockBoxRepository.deleteById(id);
  }

  // ID veya boxCode ile arama gibi diğer metotlar buraya eklenebilir.
  @Override
  @Transactional
  /**
   * Personel için bir kutuyu açmak üzere 6 haneli bir kod üretir ve kaydeder.
   *
   * @param boxCode Kod üretilecek kutunun kodu.
   * @param user    İşlemi yapan personel (User).
   * @return Üretilen 6 haneli açma kodu.
   */
  public String generateOpeningCode(String boxCode, User user) {
    LockBox lockBox = lockBoxRepository.findByBoxCode(boxCode)
        .orElseThrow(() -> new SmartLockException("Kutu bulunamadı: " + boxCode));

    String openingCode = OTPGenerator.generate(6);
    lockBox.setOpeningCode(openingCode);
    lockBox.setOpeningCodeExpiry(LocalDateTime.now().plusMinutes(10));
    lockBoxRepository.save(lockBox);

    // İşlemi BoxOperation olarak kaydet
    BoxOperation boxOperation = new BoxOperation();
    boxOperation.setLockBox(lockBox);
    boxOperation.setUser(user);
    boxOperation.setOperationType(OperationType.GENERATE_OPENING_CODE);
    boxOperation.setOperationTime(LocalDateTime.now());
    boxOperation.setNotes("Personel için kutu açma kodu üretildi.");
    boxOperation.setPreviousStatus(lockBox.getStatus().name());
    boxOperation.setNewStatus(lockBox.getStatus().name());
    boxOperationRepository.save(boxOperation);

    // Kafka'ya box operation event'i outbox pattern ile gönder
    // Aynı transaction içinde outbox'a kaydedilir (reliable delivery)
    publishBoxOperationEventReliable(boxOperation);

    return openingCode;
  }

  @Transactional
  public boolean openBoxWithCode(String boxCode, String code, User user) {
    LockBox lockBox = lockBoxRepository.findByBoxCode(boxCode)
        .orElseThrow(() -> new SmartLockException("Kutu bulunamadı: " + boxCode));

    boolean isValid = lockBox.getOpeningCode() != null &&
        lockBox.getOpeningCode().equals(code) &&
        lockBox.getOpeningCodeExpiry() != null &&
        lockBox.getOpeningCodeExpiry().isAfter(LocalDateTime.now());

    if (isValid) {
      // 1. ESP32'ye açma sinyali gönder
      // esp32CommunicationManager.sendUnlockSignal(lockBox.getBoxCode(), code);
      // Not: ESP32'nin IP adresi veya kimliği `boxCode` olmayabilir.
      // LockBox entity'sinde `deviceId` gibi bir alan tutmak daha doğru olabilir.
      // Şimdilik boxCode kullandığımızı varsayalım.
      logger.info("Açma sinyali gönderiliyor: {}", lockBox.getBoxCode());

      // 2. Kodun tekrar kullanılmasını engellemek için null yap
      lockBox.setOpeningCode(null);
      lockBox.setOpeningCodeExpiry(null);
      lockBoxRepository.save(lockBox);

      // 3. Başarılı açma işlemini logla
      BoxOperation boxOperation = new BoxOperation();
      boxOperation.setLockBox(lockBox);
      boxOperation.setUser(user);
      boxOperation.setOperationType(OperationType.TAKE_FROM_BOX); // Veya yeni bir OperationType: OPEN_BY_STAFF
      boxOperation.setOperationTime(LocalDateTime.now());
      boxOperation.setNotes("Personel tarafından kutu başarıyla açıldı.");
      boxOperation.setPreviousStatus(lockBox.getStatus().name());
      boxOperation.setNewStatus(lockBox.getStatus().name()); // Durum değişmiyorsa aynı kalabilir.
      boxOperationRepository.save(boxOperation);

      // Kafka'ya box opened event'i outbox pattern ile gönder
      // Aynı transaction içinde outbox'a kaydedilir (reliable delivery)
      publishBoxOpenedEventReliable(lockBox);
    }
    return isValid;
  }

  @Override
  public boolean validateOpeningCode(String boxCode, String code) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'validateOpeningCode'");
  }

  /**
   * Reliable event publishing using Outbox Pattern for box created
   *
   * Phase 4 Enhancement: Transactional Outbox Pattern
   * - Published within the same transaction as LockBox save
   * - Guarantees atomic consistency: box + event both persisted or both rolled back
   * - OutboxPoller handles async Kafka publishing (no thread blocking)
   * - Supports replay and audit trail
   *
   * @param box The lock box that was saved to database
   */
  private void publishBoxCreatedEventReliable(LockBox box) {
    try {
      lockBoxEventPublisher.publishBoxCreatedEventReliable(box);
      logger.debug("Box created event published reliably to outbox - BoxId: {}", box.getId());
    } catch (Exception e) {
      logger.error("Failed to publish box created event to outbox - BoxId: {}", box.getId(), e);
      throw new RuntimeException("Event publishing failed - transaction will rollback", e);
    }
  }

  /**
   * Reliable event publishing using Outbox Pattern for box status changed
   *
   * Phase 4 Enhancement: Transactional Outbox Pattern
   * - Published within the same transaction as LockBox update
   * - Guarantees atomic consistency: box + event both persisted or both rolled back
   * - OutboxPoller handles async Kafka publishing (no thread blocking)
   * - Supports replay and audit trail
   *
   * @param box The lock box that was updated
   * @param previousStatus The previous status of the box
   */
  private void publishBoxStatusChangedEventReliable(LockBox box, String previousStatus) {
    try {
      lockBoxEventPublisher.publishBoxStatusChangedEventReliable(box, previousStatus);
      logger.debug("Box status changed event published reliably to outbox - BoxId: {}", box.getId());
    } catch (Exception e) {
      logger.error("Failed to publish box status changed event to outbox - BoxId: {}", box.getId(), e);
      throw new RuntimeException("Event publishing failed - transaction will rollback", e);
    }
  }

  /**
   * Reliable event publishing using Outbox Pattern for box opened (security-critical)
   *
   * Phase 4 Enhancement: Transactional Outbox Pattern
   * - Published within the same transaction as box open operation
   * - Guarantees atomic consistency: box + event both persisted or both rolled back
   * - OutboxPoller handles async Kafka publishing (no thread blocking)
   * - Supports replay and audit trail
   *
   * @param box The lock box that was opened
   */
  private void publishBoxOpenedEventReliable(LockBox box) {
    try {
      lockBoxEventPublisher.publishBoxOpenedEventReliable(box);
      logger.debug("Box opened event published reliably to outbox - BoxId: {}", box.getId());
    } catch (Exception e) {
      logger.error("Failed to publish box opened event to outbox - BoxId: {}", box.getId(), e);
      throw new RuntimeException("Event publishing failed - transaction will rollback", e);
    }
  }

  /**
   * Reliable event publishing using Outbox Pattern for box operation
   *
   * Phase 4 Enhancement: Transactional Outbox Pattern
   * - Published within the same transaction as BoxOperation save
   * - Guarantees atomic consistency: operation + event both persisted or both rolled back
   * - OutboxPoller handles async Kafka publishing (no thread blocking)
   * - Supports replay and audit trail
   *
   * @param boxOp The box operation that was saved
   */
  private void publishBoxOperationEventReliable(BoxOperation boxOp) {
    try {
      lockBoxEventPublisher.publishBoxOperationEventReliable(boxOp);
      logger.debug("Box operation event published reliably to outbox - OperationId: {}", boxOp.getId());
    } catch (Exception e) {
      logger.error("Failed to publish box operation event to outbox - BoxId: {}", boxOp.getLockBox().getId(), e);
      throw new RuntimeException("Event publishing failed - transaction will rollback", e);
    }
  }

}
