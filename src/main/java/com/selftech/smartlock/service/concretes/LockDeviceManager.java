package com.selftech.smartlock.service.concretes;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.selftech.smartlock.shared.model.User;
import com.selftech.smartlock.shared.mapper.ModelMapperService;
import com.selftech.smartlock.event.kafka.publisher.LockDeviceEventPublisherService;
import com.selftech.smartlock.models.dto.enums.LockStatus;
import com.selftech.smartlock.models.dto.enums.OperationType;
import com.selftech.smartlock.models.dto.enums.SignatureAlgorithm;
import com.selftech.smartlock.models.dto.request.lockDevice.LockDeviceRequest;
import com.selftech.smartlock.models.entity.DeviceCredential;
import com.selftech.smartlock.models.entity.DeviceOperation;
import com.selftech.smartlock.models.entity.LockBox;
import com.selftech.smartlock.models.entity.LockDevice;
import com.selftech.smartlock.repository.DeviceCredentialRepository;
import com.selftech.smartlock.repository.LockBoxRepository;
import com.selftech.smartlock.repository.LockDeviceOperationRepository;
import com.selftech.smartlock.repository.LockDeviceRepository;
import com.selftech.smartlock.service.abstracts.ILockDeviceService;
import com.selftech.smartlock.utils.OTPGenerator;
import com.selftech.smartlock.utils.exceptions.SmartLockException;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class LockDeviceManager implements ILockDeviceService {
  private final LockDeviceRepository lockDeviceRepository;
  private final LockBoxRepository lockBoxRepository;
  private final LockDeviceOperationRepository lockDeviceOperationRepository;
  private final DeviceCredentialRepository deviceCredentialRepository;
  private final ModelMapperService modelMapperService;
  private final LockDeviceEventPublisherService lockDeviceEventPublisher;

  @Override
  public LockDevice getDeviceFromBox(String boxCode) {
    LockBox returnBox = lockBoxRepository.findByBoxCode(boxCode)
        .orElseThrow(() -> new SmartLockException("Kutu bulunamadı: " + boxCode));

    if (returnBox.getDevices().isEmpty()) {
      throw new SmartLockException("Kutuda cihaz bulunamadı: " + boxCode);
    }
    // Kutudaki ilk cihazı döndürür.
    return returnBox.getDevices().get(0);
  }

  @Override
  public List<LockDevice> getAllDevices() {
    return lockDeviceRepository.findAll();
  }

  @Override
  public LockDevice createDevice(LockDeviceRequest request, User user) {
    if (lockDeviceRepository.findByDeviceCode(request.getDeviceCode()).isPresent()) {
      throw new SmartLockException("Bu cihaz kodu zaten mevcut: " + request.getDeviceCode());
    }

    LockDevice device = modelMapperService.forRequest().map(request, LockDevice.class);

    // 2. Cihaz durumunu ayarla
    device.setStatus(LockStatus.AVAILABLE); // Yeni cihazın başlangıç durumu
    device.setCredentials(new ArrayList<>());

    LockDevice savedDevice = lockDeviceRepository.save(device);

    // 3. Cihazın kimlik bilgisini (public key) oluştur ve ilişkilendir.
    // request DTO'sunda publicKey alanı olduğunu varsayıyoruz.
    if (request.getPublicKey() != null && !request.getPublicKey().isEmpty()) {
      DeviceCredential credential = DeviceCredential.builder()
          .device(savedDevice)
          .publicKey(request.getPublicKey())
          .algorithm(SignatureAlgorithm.ECDSA_SHA256) // Varsayılan algoritma
          .isActive(true)
          .createdAt(LocalDateTime.now())
          .build();
      deviceCredentialRepository.save(credential);
    }

    // 3. Cihaz oluşturma işlemini logla
    DeviceOperation deviceOperation = DeviceOperation.builder()
        .device(savedDevice)
        .user(user) // İşlemi yapan personel
        .vehicle(null) // Bu işlem bir araçla ilişkili değil
        .operationType(OperationType.CREATE_DEVICE)
        .operationTime(LocalDateTime.now())
        .notes("Yeni cihaz sisteme eklendi.")
        .build();
    lockDeviceOperationRepository.save(deviceOperation);

    // Kafka'ya device created event'i async şekilde gönder
    publishDeviceCreatedEventAsync(savedDevice);

    return savedDevice;
  }

  @Override
  public Optional<LockDevice> findByDeviceCode(String deviceCode) {
    return lockDeviceRepository.findByDeviceCode(deviceCode);
  }

  @Override
  public LockDevice updateLockDevice(Long id, LockDevice deviceDetails, User user) {
    LockDevice existingDevice = lockDeviceRepository.findById(id)
        .orElseThrow(() -> new SmartLockException("ID'si " + id + " olan cihaz bulunamadı."));

    String previousStatus = existingDevice.getStatus().name();

    // Gelen bilgilerle mevcut cihazı güncelle
    existingDevice.setDeviceCode(deviceDetails.getDeviceCode());
    existingDevice.setStatus(deviceDetails.getStatus());
    LockDevice updatedDevice = lockDeviceRepository.save(existingDevice);

    // Güncelleme işlemini logla
    DeviceOperation deviceOperation = DeviceOperation.builder()
        .device(updatedDevice)
        .user(user)
        .operationType(OperationType.STATUS_CHANGE)
        .operationTime(LocalDateTime.now())
        .notes("Cihaz bilgileri personel tarafından güncellendi.")
        .build();
    lockDeviceOperationRepository.save(deviceOperation);

    // Kafka'ya device status changed event'i async şekilde gönder
    publishDeviceStatusChangedEventAsync(updatedDevice, previousStatus);

    return updatedDevice;
  }

  @Override
  public void deleteLockDevice(Long id, User user) {
    LockDevice device = lockDeviceRepository.findById(id)
        .orElseThrow(() -> new SmartLockException("ID'si " + id + " olan cihaz bulunamadı."));

    String previousStatus = device.getStatus().name();

    // Silme işlemi yerine cihazın durumunu 'Hizmet Dışı' olarak işaretlemek daha güvenli bir yaklaşımdır.
    device.setStatus(LockStatus.OUT_OF_SERVICE);
    lockDeviceRepository.save(device);

    // Silme (durum değiştirme) işlemini logla
    DeviceOperation deviceOperation = DeviceOperation.builder()
        .device(device)
        .user(user)
        .operationType(OperationType.STATUS_CHANGE)
        .operationTime(LocalDateTime.now())
        .notes("Cihaz personel tarafından hizmet dışı bırakıldı (silindi).")
        .build();
    lockDeviceOperationRepository.save(deviceOperation);

    // Kafka'ya device status changed event'i async şekilde gönder
    publishDeviceStatusChangedEventAsync(device, previousStatus);

    lockDeviceRepository.deleteById(id);
  }

  /**
   * Personel kilidi geri almak isterse, tekrar açması için 6 haneli kod üretir ve
   * kaydeder.
   *
   * @param deviceCode Kod üretilecek kilidin kodu.
   * @param user       İşlemi yapan personel (User).
   * @return Üretilen 6 haneli açma kodu.
   */
  @Override
  public String generateRetrievalCode(String deviceCode, User user) {
    LockDevice lockDevice = lockDeviceRepository.findByDeviceCode(deviceCode)
        .orElseThrow(() -> new SmartLockException("Cihaz bulunamadı: " + deviceCode));

    String retrievalCode = OTPGenerator.generate(6);
    lockDevice.setRetrievalCode(retrievalCode);
    lockDevice.setRetrievalCodeExpiry(LocalDateTime.now().plusMinutes(10));
    lockDeviceRepository.save(lockDevice);

    // İşlemi DeviceOperation olarak kaydet
    DeviceOperation deviceOperation = DeviceOperation.builder()
        .device(lockDevice)
        .user(user)
        .operationType(OperationType.GENERATE_OPENING_CODE)
        .vehicle(null)
        .build();

    lockDeviceOperationRepository.save(deviceOperation);

    // Kafka'ya device operation event'i async şekilde gönder
    publishDeviceOperationEventAsync(deviceOperation);

    return retrievalCode;
  }

  @Override
  public boolean validateRetrievalCode(String deviceCode, String code) {
    LockDevice lockDevice = lockDeviceRepository.findByDeviceCode(deviceCode)
        .orElseThrow(() -> new SmartLockException("Cihaz bulunamadı: " + deviceCode));

    return lockDevice.getRetrievalCode() != null &&
        lockDevice.getRetrievalCode().equals(code) &&
        lockDevice.getRetrievalCodeExpiry() != null &&
        lockDevice.getRetrievalCodeExpiry().isAfter(LocalDateTime.now());
  }

  @Override
  public void assignDeviceToBox(String deviceCode, String boxCode, User user) {
    LockDevice lockDevice = lockDeviceRepository.findByDeviceCode(deviceCode)
        .orElseThrow(() -> new SmartLockException("Cihaz bulunamadı: " + deviceCode));

    LockBox lockBox = lockBoxRepository.findByBoxCode(boxCode)
        .orElseThrow(() -> new SmartLockException("Kutu bulunamadı: " + boxCode));

    // Cihazın durumunu ve ilişkili kutuyu güncelle
    lockDevice.setCurrentBox(lockBox);
    lockDevice.setStatus(LockStatus.AVAILABLE); // Kutudaki cihaz kullanıma hazırdır.
    lockDevice.setCurrentUser(null); // Cihaz artık bir kullanıcıya zimmetli değil.
    lockDeviceRepository.save(lockDevice);

    // Atama işlemini logla
    DeviceOperation deviceOperation = DeviceOperation.builder()
        .device(lockDevice)
        .user(user) // İşlemi yapan personel
        .lockBox(lockBox)
        .operationType(OperationType.RETURN_TO_BOX) // Mevcut operasyon tipi bu senaryo için uygun.
        .operationTime(LocalDateTime.now())
        .notes("Personel tarafından " + lockBox.getBoxCode() + " kutusuna atandı.")
        .build();

    lockDeviceOperationRepository.save(deviceOperation);

    // Kafka'ya device operation event'i async şekilde gönder
    publishDeviceOperationEventAsync(deviceOperation);
  }

  // Cihaz kaydı, güncellemesi gibi diğer metotlar buraya eklenebilir.

  /**
   * Async wrapper method untuk device created event publishing
   * API response'u engellememesi için background thread'de çalışır
   */
  @Async("taskExecutor")
  private void publishDeviceCreatedEventAsync(LockDevice device) {
    try {
      lockDeviceEventPublisher.publishDeviceCreatedEvent(device);
    } catch (Exception e) {
      // Log aber don't throw - async publish failure doesn't affect API response
    }
  }

  /**
   * Async wrapper method untuk device status changed event publishing
   */
  @Async("taskExecutor")
  private void publishDeviceStatusChangedEventAsync(LockDevice device, String previousStatus) {
    try {
      lockDeviceEventPublisher.publishDeviceStatusChangedEvent(device, previousStatus);
    } catch (Exception e) {
      // Log aber don't throw - async publish failure doesn't affect API response
    }
  }

  /**
   * Async wrapper method untuk device operation event publishing
   */
  @Async("taskExecutor")
  private void publishDeviceOperationEventAsync(DeviceOperation deviceOperation) {
    try {
      lockDeviceEventPublisher.publishDeviceOperationEvent(deviceOperation);
    } catch (Exception e) {
      // Log aber don't throw - async publish failure doesn't affect API response
    }
  }

}
