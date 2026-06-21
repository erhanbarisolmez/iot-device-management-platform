package com.selftech.smartlock.service.concretes;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.selftech.selfparkbackendv001.models.user.Role;
import com.selftech.selfparkbackendv001.models.user.User;
import com.selftech.smartlock.event.kafka.publisher.LockDeviceEventPublisherService;
import com.selftech.smartlock.models.dto.enums.BoxStatus;
import com.selftech.smartlock.models.dto.enums.LockStatus;
import com.selftech.smartlock.models.dto.enums.OperationType;
import com.selftech.smartlock.models.dto.enums.PaymentStatus;
import com.selftech.smartlock.models.dto.request.sensor.SensorDataRequest;
import com.selftech.smartlock.models.entity.DeviceOperation;
import com.selftech.smartlock.models.entity.LockBox;
import com.selftech.smartlock.models.entity.LockDevice;
import com.selftech.smartlock.models.entity.Vehicle;
import com.selftech.smartlock.repository.LockBoxRepository;
import com.selftech.smartlock.repository.LockDeviceRepository;
import com.selftech.smartlock.repository.VehicleRepository;
import com.selftech.smartlock.service.abstracts.ILockBoxOperationService;
import com.selftech.smartlock.service.abstracts.ILockDeviceOperationService;
import com.selftech.smartlock.service.abstracts.ILockPaymentService;
import com.selftech.smartlock.service.abstracts.ISmsService;
import com.selftech.smartlock.service.abstracts.IVehicleService;
import com.selftech.smartlock.utils.OTPGenerator;
import com.selftech.smartlock.utils.exceptions.SmartLockException;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class LockBoxOperationManager implements ILockBoxOperationService {

    // Business Constants - Pricing
    private static final BigDecimal DEFAULT_LOCK_FEE = BigDecimal.valueOf(50.0);
    private static final BigDecimal DEFAULT_DEPOSIT_AMOUNT = BigDecimal.valueOf(100.0);
    private static final BigDecimal HOURLY_PENALTY_RATE = BigDecimal.valueOf(25.0);
    private static final long MINIMUM_BILLING_HOURS = 1L;

    // OTP/Code Configuration
    private static final int LOCK_CODE_LENGTH = 6;
    private static final int UNLOCK_CODE_LENGTH = 6;
    private static final int BOX_OPENING_CODE_LENGTH = 7;
    private static final String BOX_OPENING_CODE_PREFIX = "C";

    // Sensor Thresholds
    private static final double WEIGHT_THRESHOLD_KG = 0.5;

    // Dependencies
    private final ILockDeviceOperationService lockDeviceOperationService;
    private final VehicleRepository vehicleRepository;
    private final LockDeviceRepository lockDeviceRepository;
    private final LockBoxRepository lockBoxRepository;
    private final IVehicleService vehicleService;
    private final ISmsService smsService;
    private final ILockPaymentService paymentService;
    private final LockDeviceEventPublisherService lockDeviceEventPublisher;
    private static final Logger logger = LoggerFactory.getLogger(LockBoxOperationManager.class);

    @Override
    @Transactional
    public DeviceOperation initializeLock(String boxCode, String plateNumber, User user) {

        LockBox lockBox = lockBoxRepository.findByBoxCode(boxCode)
            .orElseThrow(() -> new SmartLockException("Kutu bulunamadı: " + boxCode));

        if (lockBox.getDevices().isEmpty()) {
            throw new SmartLockException("Kutuda cihaz bulunamadı: " + boxCode);
        }

        LockDevice device = lockBox.getDevices().get(0);

        if (!device.getStatus().equals(LockStatus.AVAILABLE)) {
            throw new SmartLockException("Cihaz '" + boxCode + "' şu anda müsait değil. Durumu: " + device.getStatus());
        }

        Vehicle vehicle = vehicleRepository.findByPlate_PlateNumber(plateNumber)
            .orElseGet(() -> vehicleService.createVehicleForPlate(plateNumber));

        DeviceOperation lockOperation = createLockOperation(device, vehicle, user);

        String previousStatus = device.getStatus().name();
        device.setStatus(LockStatus.IN_USE);
        device.setCurrentUser(vehicle.getUser());
        device.setCurrentBox(null);
        lockDeviceRepository.save(device);

        // Kafka'ya device status changed event'i async şekilde gönder
        publishDeviceStatusChangedEventAsync(device, previousStatus);

        return lockOperation;
    }

    private DeviceOperation createLockOperation(LockDevice device, Vehicle vehicle, User user) {
        String lockCode = OTPGenerator.generate(LOCK_CODE_LENGTH);

        DeviceOperation lockOperation = DeviceOperation.builder()
            .device(device)
            .vehicle(vehicle)
            .user(user)
            .lockCode(lockCode)
            .lockTime(LocalDateTime.now())
            .status(LockStatus.LOCKED)
            .paymentStatus(PaymentStatus.PENDING)
            .lockFee(DEFAULT_LOCK_FEE)
            .depositAmount(DEFAULT_DEPOSIT_AMOUNT)
            .totalAmount(DEFAULT_LOCK_FEE.add(DEFAULT_DEPOSIT_AMOUNT))
            .operationType(OperationType.LOCK_DEVICE)
            .operationTime(LocalDateTime.now())
            .notes("Kilit operasyonu başlatıldı - Plaka: " + vehicle.getPlate().getPlateNumber())
            .build();

        // ÖNCE operasyonu kaydet, böylece bir ID'si olur.
        DeviceOperation savedOperation = lockDeviceOperationService.save(lockOperation);

        // LockCode'u müşteriye gönder
        // Artık kaydedilmiş ve ID'si olan nesneyi SMS servisine gönderiyoruz.
        smsService.sendLockCodeSms(savedOperation);

        return savedOperation;
    }

    @Override
    @Transactional
    public DeviceOperation calculatePenalty(String lockCode) {
        DeviceOperation lockOperation = lockDeviceOperationService.findByLockCode(lockCode)
            .orElseThrow(() -> new SmartLockException("Kilit operasyonu bulunamadı: " + lockCode));

        // SMS gönderilip gönderilmeyeceğini kontrol etmek için önceki tutarı saklayalım.
        BigDecimal previousTotalAmount = BigDecimal.ZERO; // lockOperation.getTotalAmount()

        // Kötüye kullanımı önlemek için, ücret HER ZAMAN yeniden hesaplanır.
        long hours = Duration.between(lockOperation.getLockTime(), LocalDateTime.now()).toHours();
        // Minimum billing hours
        hours = Math.max(MINIMUM_BILLING_HOURS, hours);

        BigDecimal penalty = HOURLY_PENALTY_RATE.multiply(BigDecimal.valueOf(hours));
        
        BigDecimal newTotalAmount = lockOperation.getLockFee()
            .add(lockOperation.getDepositAmount())
            .add(penalty);

        // Tutarın değişip değişmediğini kontrol et. (previousTotalAmount null olabilir)
        boolean amountChanged = previousTotalAmount == null || newTotalAmount.compareTo(previousTotalAmount) != 0;

        lockOperation.setPenaltyAmount(penalty);
        lockOperation.setTotalAmount(newTotalAmount);
        lockOperation.setStatus(LockStatus.AWAITING_PAYMENT);

        DeviceOperation savedOperation = lockDeviceOperationService.save(lockOperation);
        
        // Müşteriye ilk defa hesaplama yapıldığında VEYA tutar değiştiğinde ödeme bilgisi gönder.
        if (amountChanged) {
            smsService.sendPaymentAmountSms(savedOperation);
        }

        return savedOperation;
    }

    @Override
    @Transactional
    public DeviceOperation finalizePaymentAndGenerateUnlockCode(String lockCode, String paymentId) {
        DeviceOperation lockOperation = lockDeviceOperationService.findByLockCode(lockCode)
            .orElseThrow(() -> new SmartLockException("Kilit operasyonu bulunamadı: " + lockCode));

        // Ödeme işlemini gerçekleştir
        // boolean paymentSuccess = paymentService.processPayment(lockCode); // Bu metot artık gereksiz, doğrulama callback'te yapılıyor.
        
        // if (!paymentSuccess) {
        //     throw new SmartLockException("Ödeme işlemi başarısız: " + lockCode);
        // }

        String unlockCode = OTPGenerator.generate(UNLOCK_CODE_LENGTH);
        String boxOpeningCode = BOX_OPENING_CODE_PREFIX + OTPGenerator.generate(BOX_OPENING_CODE_LENGTH);
        lockOperation.setPaymentId(paymentId); // Iyzico'dan gelen paymentId'yi kaydediyoruz.
        lockOperation.setUnlockCode(unlockCode);
        lockOperation.setBoxOpeningCode(boxOpeningCode);
        lockOperation.setPaymentStatus(PaymentStatus.SUCCESS);
        lockOperation.setStatus(LockStatus.PAYMENT_CONFIRMED);
        lockOperation.setPaymentConfirmedAt(LocalDateTime.now());

        DeviceOperation savedOperation = lockDeviceOperationService.save(lockOperation);

        // Müşteriye hem kilit açma hem de kutu açma kodunu gönder
        smsService.sendUnlockCodeSms(savedOperation);

        return savedOperation;
    }

    @Override
    @Transactional
    public boolean unlock(String unlockCode) {
        // Kilit açma koduna göre operasyonu bul. Bulamazsa hata fırlat.
        DeviceOperation lockOperation = lockDeviceOperationService.findByUnlockCode(unlockCode)
            .orElseThrow(() -> new SmartLockException("Geçersiz kilit açma kodu: " + unlockCode));

        // Güvenlik kontrolü: Bu kod daha önce kullanılmış mı?
        if (lockOperation.getStatus() == LockStatus.UNLOCKED || lockOperation.getStatus() == LockStatus.RETURNED) {
            // İsteğe bağlı: Tekrar denemeleri loglayabiliriz.
            // throw new SmartLockException("Bu kilit açma kodu daha önce kullanılmış.");
            return false; // Kodu geçersiz kılmak yerine sessizce başarısız ol.
        }

        // Operasyon durumunu güncelle
        lockOperation.setUnlockTime(LocalDateTime.now());
        lockOperation.setStatus(LockStatus.UNLOCKED);

        // Cihaz durumunu güncelle
        LockDevice device = lockOperation.getDevice();
        String previousStatus = device.getStatus().name();
        device.setStatus(LockStatus.UNLOCKED);
        device.setCurrentUser(null);
        lockDeviceRepository.save(device);

        lockDeviceOperationService.save(lockOperation);

        // Kafka'ya device status changed event'i async sekilde gonder
        publishDeviceStatusChangedEventAsync(device, previousStatus);

        return true;
    }

    @Override
    @Transactional
    public void processReturn(String boxCode, String deviceCode, Double weightMeasured, Boolean doorOpen, User user) {
        LockBox box = lockBoxRepository.findByBoxCode(boxCode)
            .orElseThrow(() -> new SmartLockException("Kutu bulunamadı: " + boxCode));

        LockDevice device = lockDeviceRepository.findByDeviceCode(deviceCode)
            .orElseThrow(() -> new SmartLockException("Cihaz bulunamadı: " + deviceCode));

        // Eğer işlemi yapan bir personel ise (ADMIN veya MANAGER), sadece cihazı kutuya yerleştir ve durumu güncelle.
        if (user != null && (user.getRole() == Role.ADMIN || user.getRole() == Role.MANAGER)) {
            processPersonnelReturn(box, device, user);
        } else {
            // Eğer işlemi yapan müşteri ise (veya sistem tarafından tetikleniyorsa), depozito iadesi yap.
            processCustomerReturn(box, device, weightMeasured, doorOpen);
        }
    }

    /**
     * Müşterinin, SMS ile aldığı kutu açma kodunu kullanarak cihazı iade etmesini sağlar.
     * Bu metot, kodun geçerliliğini kontrol eder, iade işlemini tamamlar ve depozito iadesini başlatır.
     * Başarılı işlem sonrası kod, tekrar kullanılmaması için geçersiz kılınır (null yapılır).
     * @param boxOpeningCode Müşteriye SMS ile gönderilen kod.
     */
    @Transactional
    public void processCustomerReturnByCode(String boxOpeningCode) {
        // 1. Koda göre operasyonu bul
        DeviceOperation lockOperation = lockDeviceOperationService.findByBoxOpeningCode(boxOpeningCode)
            .orElseThrow(() -> new SmartLockException("Geçersiz veya daha önce kullanılmış kutu açma kodu."));

        // 2. Cihazın durumunu kontrol et (kilidi açılmış olmalı)
        LockDevice device = lockOperation.getDevice();
        if (device.getStatus() != LockStatus.UNLOCKED) {
            throw new SmartLockException("Cihaz iade edilmeden önce kilidinin açılmış olması gerekir. Mevcut durum: " + device.getStatus());
        }

        // 3. İade işlemini gerçekleştir
        // Not: Bu akışta fiziksel kutu (LockBox) bilgisi müşteri tarafından sağlanmıyor.
        // Bu nedenle lockOperation.setLockBox(box) adımını atlıyoruz veya varsayımsal bir kutu atıyoruz.
        device.setStatus(LockStatus.RETURNED); // Cihazın durumu 'İade Edildi' olarak güncellenir.
        lockDeviceRepository.save(device);

        lockOperation.setReturnTime(LocalDateTime.now());
        lockOperation.setStatus(LockStatus.RETURNED);
        lockOperation.setDepositRefundedAt(LocalDateTime.now()); // İade işleminin başladığı zaman
        
        // 4. Kodu geçersiz kıl
        lockOperation.setBoxOpeningCode(null);
        lockDeviceOperationService.save(lockOperation);

        // 5. Depozito iadesini başlat ve müşteriye SMS gönder
        paymentService.refundDeposit(lockOperation);
        smsService.sendDepositRefundSms(lockOperation);
    }

    /**
     * Müşteri tarafından yapılan iade işlemini yönetir. Depozito iadesi başlatır.
     */
    private void processCustomerReturn(LockBox box, LockDevice device, Double weightMeasured, Boolean doorOpen) {
        String deviceCode = device.getDeviceCode();
        DeviceOperation lockOperation = lockDeviceOperationService.findByDevice_DeviceCodeAndStatus(deviceCode, LockStatus.UNLOCKED)
            .orElseThrow(() -> new SmartLockException("Cihaz için kilidi açılmış operasyon bulunamadı."));

        // Kilidi kutuya yerleştir
        String previousStatus = device.getStatus().name();
        device.setCurrentBox(box);
        device.setStatus(LockStatus.RETURNED);
        lockDeviceRepository.save(device);

        // Operasyonu güncelle
        lockOperation.setReturnTime(LocalDateTime.now());
        lockOperation.setStatus(LockStatus.RETURNED);
        lockOperation.setLockBox(box);
        lockOperation.setBoxWeight(weightMeasured);
        lockOperation.setBoxDoorOpen(doorOpen);
        lockOperation.setDepositRefundedAt(LocalDateTime.now());
        lockDeviceOperationService.save(lockOperation);

        // Teminat iadesi
        paymentService.refundDeposit(lockOperation);

        // Müşteriye iade bilgisi gönder
        smsService.sendDepositRefundSms(lockOperation);

        // Kafka'ya device status changed event'i async sekilde gonder
        publishDeviceStatusChangedEventAsync(device, previousStatus);

        logger.info(deviceCode + " cihazı kutuya iade edildi.");
        logger.info(lockOperation.toString() + " operasyonu güncellendi.");
    }

    /**
     * Personel tarafından yapılan iade işlemini yönetir. Depozito iadesi yapmaz.
     * Cihazın durumunu doğrudan 'AVAILABLE' olarak ayarlar.
     */
    private void processPersonnelReturn(LockBox box, LockDevice device, User personnel) {
        String previousStatus = device.getStatus().name();
        device.setCurrentBox(box);
        device.setStatus(LockStatus.AVAILABLE); // Personel iade ettiğinde cihaz tekrar kullanıma hazır olur.
        device.setCurrentUser(null);
        lockDeviceRepository.save(device);

        // Bu işlem için de bir log kaydı oluşturmak faydalı olacaktır.
        DeviceOperation operationLog = DeviceOperation.builder()
            .device(device)
            .user(personnel)
            .operationType(OperationType.RETURN_TO_BOX)
            .notes("Personel tarafından kutuya iade edildi.")
            .build();
        lockDeviceOperationService.save(operationLog);

        // Kafka'ya device status changed event'i async sekilde gonder
        publishDeviceStatusChangedEventAsync(device, previousStatus);

        logger.info(operationLog.toString() + " operasyonu kaydedildi.");
    }

    /**
     * Müşterinin tek kullanımlık kutu açma kodunu doğrular ve kutuyu açar.
     * @param boxOpeningCode Müşteriye SMS ile gönderilen kod.
     * @return Kod geçerliyse ilgili DeviceOperation nesnesini döndürür.
     */
    public DeviceOperation validateAndOpenBoxForCustomer(String boxOpeningCode) {
        DeviceOperation lockOperation = lockDeviceOperationService.findByBoxOpeningCode(boxOpeningCode)
            .orElseThrow(() -> new SmartLockException("Geçersiz kutu açma kodu."));

        // Bu kodun ait olduğu cihazın durumu UNLOCKED olmalı.
        if (lockOperation.getDevice().getStatus() != LockStatus.UNLOCKED) {
            throw new SmartLockException("Bu kodun kullanılabilmesi için önce cihaz kilidinin açılması gerekir.");
        }

        // TODO: Burada kutu açma sinyali gönderilebilir.
        logger.info("Sending box opening signal for customer - boxOpeningCode: {}", boxOpeningCode);

        return lockOperation;
    }

    @Override
    @Transactional
    public void processDeviceData(String boxCode, SensorDataRequest sensorData) {
        // 1. Gelen `boxCode` ile ilgili kutuyu bul.
        LockBox box = lockBoxRepository.findByBoxCode(boxCode)
            .orElseThrow(() -> new SmartLockException("Kutu bulunamadı: " + boxCode));

        // 2. Kutunun sensör ve durum bilgilerini güncelle.
        box.setLastCommunication(LocalDateTime.now());
        box.setBatteryLevel(sensorData.getBatteryLevel());
        box.setCurrentWeight(sensorData.getWeight());
        box.setDoorOpen(sensorData.getDoorOpen());

        // Kutunun içinde bir cihaz varsa, o cihazın da pil seviyesini güncelle.
        // Bu, sahadaki cihazların pil durumunu izlemek için önemlidir.
        if (!box.getDevices().isEmpty()) {
            LockDevice deviceInBox = box.getDevices().get(0); // Kutuda sadece bir cihaz olduğunu varsayıyoruz.
            // Cihazın pil seviyesini güncelleme mantığı buraya eklenebilir.
            // deviceInBox.setBatteryLevel(sensorData.getBatteryLevel());
            // lockDeviceRepository.save(deviceInBox);
        }
        // 3. Ağırlık ve kapı durumuna göre kutunun doluluk durumunu (status) belirle.
        // Sensördeki küçük oynamaları tolere etmek için bir eşik değeri kullanıyoruz.
        boolean isConsideredEmpty = sensorData.getWeight() < WEIGHT_THRESHOLD_KG;

        // Senaryo 1: Kapı açık. Durum ne olursa olsun, kutu şu an için 'EMPTY' kabul edilebilir
        // çünkü işlem tamamlanmamıştır.
        if (sensorData.getDoorOpen()) {
            box.setStatus(BoxStatus.EMPTY);
            logger.info("Kutu kapısı açık. Durum EMPTY olarak ayarlandı. BoxCode: {}", boxCode);
        // Senaryo 2: Kapı kapalı ve ağırlık eşik değerinin altında. Kutu boş.
        } else if (isConsideredEmpty) {
            box.setStatus(BoxStatus.EMPTY);
            logger.info("Kapı kapalı ancak ağırlık yetersiz ({} kg). Durum EMPTY olarak ayarlandı. BoxCode: {}", sensorData.getWeight(), boxCode);
        // Senaryo 3: Kapı kapalı ve ağırlık eşik değerinin üzerinde. Kutu dolu.
        } else {
            box.setStatus(BoxStatus.OCCUPIED);
            logger.info("Kapı kapalı ve yeterli ağırlık ({} kg) tespit edildi. Durum OCCUPIED olarak ayarlandı. BoxCode: {}", sensorData.getWeight(), boxCode);
        }

        // 4. Güncellenmiş kutu bilgilerini veritabanına kaydet.
        lockBoxRepository.save(box);
        logger.info("Kutu bilgileri güncellendi ve kaydedildi: boxCode={}, status={}", box.getBoxCode(), box.getStatus());
    }

    /**
     * Async wrapper method for device status changed event publishing
     * Runs in background thread so doesn't block API response
     */
    @Async("taskExecutor")
    private void publishDeviceStatusChangedEventAsync(LockDevice device, String previousStatus) {
        try {
            lockDeviceEventPublisher.publishDeviceStatusChangedEvent(device, previousStatus);
        } catch (Exception e) {
            // Log aber don't throw - async publish failure doesn't affect API response
        }
    }
}