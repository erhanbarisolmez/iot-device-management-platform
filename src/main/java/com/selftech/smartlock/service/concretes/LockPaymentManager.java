package com.selftech.smartlock.service.concretes;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.iyzipay.Options;
import com.iyzipay.model.BasketItem;
import com.iyzipay.model.BasketItemType;
import com.iyzipay.model.Buyer;
import com.iyzipay.model.CheckoutFormInitialize;
import com.iyzipay.model.Currency;
import com.iyzipay.model.Locale;
import com.iyzipay.model.Payment;
import com.iyzipay.model.Refund;
import com.iyzipay.request.CreateCheckoutFormInitializeRequest;
import com.iyzipay.request.CreateRefundRequest;
import com.iyzipay.request.RetrievePaymentRequest;
import com.selftech.smartlock.event.kafka.publisher.PaymentEventPublisherService;
import com.selftech.smartlock.models.dto.enums.PaymentStatus;
import com.selftech.smartlock.models.entity.DeviceOperation;
import com.selftech.smartlock.repository.LockDeviceOperationRepository;
import com.selftech.smartlock.service.abstracts.ILockPaymentService;

import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class LockPaymentManager implements ILockPaymentService {

  private static final Logger logger = LoggerFactory.getLogger(LockPaymentManager.class);

  @Value("${iyzico.api-key}")
  private String apiKey;

  @Value("${iyzico.secret-key}")
  private String secretKey;

  @Value("${iyzico.base-url}")
  private String baseUrl;

  @Value("${app.callback-url}")
  private String appCallbackUrl;

  private final LockDeviceOperationRepository lockDeviceOperationRepository;
  private final PaymentEventPublisherService paymentEventPublisher;
  // private final PaymentManager paymentManager; // Bu bağımlılığı kaldırıyoruz.

  // Iyzico entegrasyonu için gerekli konfigürasyon ve client buraya enjekte
  // edilecek.

  @Override
  public void initiatePayment(DeviceOperation lockOperation) {
    // 1. lockOperation'daki totalAmount bilgisi ile Iyzico'ya ödeme isteği oluştur.
    // 2. Iyzico'dan dönen ödeme formunu (checkout form) veya linkini hazırla.
    // 3. Bu bilgiyi müşteriye sunulmak üzere (örneğin, controller'a) döndür.
    System.out.println(lockOperation.getTotalAmount() + " TL için ödeme başlatılıyor...");

    lockOperation.setRefundAmount(lockOperation.getDepositAmount());
    lockOperation.setPaymentStatus(PaymentStatus.REFUNDED);

    // Kafka'ya payment initiated event'i async şekilde gönder
    publishPaymentInitiatedEventAsync(lockOperation);
  }

  @Override
  public void refundDeposit(DeviceOperation lockOperation) {
    if (lockOperation.getPaymentId() == null || lockOperation.getPaymentId().isEmpty()) {
      logger.error("İade işlemi yapılamadı. Operasyon için paymentId bulunamadı. LockCode: {}",
          lockOperation.getLockCode());
      return;
    }

    // İade edilecek toplam tutarı hesapla: Depozito + Kilit Ücreti
    BigDecimal totalRefundAmount = lockOperation.getDepositAmount().add(lockOperation.getLockFee());

    logger.info("{} TL tutarında iade işlemi başlatılıyor. (Depozito: {}, Kilit Ücreti: {}). PaymentId: {}",
        totalRefundAmount, lockOperation.getDepositAmount(), lockOperation.getLockFee(), lockOperation.getPaymentId());

    CreateRefundRequest request = new CreateRefundRequest();
    request.setLocale(Locale.TR.getValue());
    request.setConversationId("REFUND_" + lockOperation.getLockCode());
    request.setPaymentTransactionId(lockOperation.getPaymentId()); // Iyzico'da iade için paymentId yerine
                                                                   // paymentTransactionId kullanılır.
    request.setPrice(totalRefundAmount);
    request.setIp("127.0.0.1"); // Sunucu IP'si veya sabit bir IP

    try {
      Refund refund = Refund.create(request, getOptions());

      if ("success".equalsIgnoreCase(refund.getStatus())) {
        logger.info("İade başarılı. PaymentId: {}, Refund TransactionId: {}", lockOperation.getPaymentId(),
            refund.getPaymentTransactionId());
        // Burada operasyonun iade durumunu güncelleyebilirsiniz.

        // Kafka'ya refund processed event'i async şekilde gönder
        publishRefundProcessedEventAsync(lockOperation);
      } else {
        logger.error("Iyzico iade işlemi başarısız oldu. Hata: {}", refund.getErrorMessage());
      }
    } catch (Exception e) {
      logger.error("Iyzico iade işlemi sırasında kritik bir hata oluştu.", e);
    }
  }

  @Override
  public boolean processPayment(String lockCode) {
    // Bu metot, ödemenin gerçekten yapıldığını doğrulamak için kullanılabilir.
    // Şimdilik her zaman başarılı varsayıyoruz. Asıl doğrulama callback'te olacak.
    return true;
  }

  public CheckoutFormInitialize prepareCheckoutForm(DeviceOperation lockOperation,
      HttpServletRequest httpServletRequest) {
    Options options = getOptions();

    CreateCheckoutFormInitializeRequest iyzicoRequest = new CreateCheckoutFormInitializeRequest();
    iyzicoRequest.setLocale(Locale.TR.getValue());
    iyzicoRequest.setConversationId(lockOperation.getLockCode()); // lockCode'u conversationId olarak kullanıyoruz.
    iyzicoRequest.setBasketId("BASKET_" + lockOperation.getLockCode());
    iyzicoRequest.setPaymentGroup(com.iyzipay.model.PaymentGroup.PRODUCT.name());
    iyzicoRequest.setCallbackUrl(getCallbackUrl(httpServletRequest));

    // Taksit seçeneklerini belirle (Responsive form için zorunlu)
    List<Integer> enabledInstallments = new ArrayList<>();
    enabledInstallments.add(1); // Sadece tek çekim
    iyzicoRequest.setEnabledInstallments(enabledInstallments);

    // Buyer bilgileri
    Buyer buyer = new Buyer();
    buyer.setId("USER_" + lockOperation.getVehicle().getUser().getId());
    buyer.setName(lockOperation.getVehicle().getUser().getFirstName());
    buyer.setSurname(lockOperation.getVehicle().getUser().getLastName());
    buyer.setEmail(lockOperation.getVehicle().getUser().getEmail());
    buyer.setGsmNumber(lockOperation.getVehicle().getUser().getPhone());
    buyer.setIdentityNumber("11111111111"); // Gerekli alan
    buyer.setRegistrationAddress("SelfPark");
    buyer.setCity("Istanbul");
    buyer.setCountry("Turkey");
    buyer.setIp(httpServletRequest.getRemoteAddr());
    iyzicoRequest.setBuyer(buyer);

    // Adres Bilgileri (ZORUNLU)
    com.iyzipay.model.Address billingAddress = new com.iyzipay.model.Address();
    billingAddress.setContactName(
        lockOperation.getVehicle().getUser().getFirstName() + " " + lockOperation.getVehicle().getUser().getLastName());
    billingAddress.setCity("Istanbul");
    billingAddress.setCountry("Turkey");
    billingAddress.setAddress("SelfPark Adres");
    iyzicoRequest.setBillingAddress(billingAddress);
    iyzicoRequest.setShippingAddress(billingAddress); // Kargo adresi fatura ile aynı

    // Basket items
    List<BasketItem> basketItems = new ArrayList<>();

    if (lockOperation.getPenaltyAmount().compareTo(BigDecimal.ZERO) > 0) {
      basketItems.add(createBasketItem("Ceza", lockOperation.getPenaltyAmount()));
    }
    if (lockOperation.getLockFee().compareTo(BigDecimal.ZERO) > 0) {
      basketItems.add(createBasketItem("Kilit Ücreti", lockOperation.getLockFee()));
    }
    if (lockOperation.getDepositAmount().compareTo(BigDecimal.ZERO) > 0) {
      basketItems.add(createBasketItem("Depozito", lockOperation.getDepositAmount()));
    }

    iyzicoRequest.setBasketItems(basketItems);

    // ÖNEMLİ: Sepet tutarını, sepet kalemlerinin toplamından hesapla.
    // Bu, tutarsızlıkları önler ve Iyzico'nun formu doğru oluşturmasını sağlar.
    BigDecimal totalBasketPrice = basketItems.stream()
        .map(BasketItem::getPrice)
        .reduce(BigDecimal.ZERO, BigDecimal::add);
    iyzicoRequest.setPrice(totalBasketPrice);
    iyzicoRequest.setPaidPrice(totalBasketPrice);
    iyzicoRequest.setCurrency(Currency.TRY.name());

    // Iyzico'yu initialize et
    CheckoutFormInitialize checkoutFormInitialize = CheckoutFormInitialize.create(iyzicoRequest, options);

    return checkoutFormInitialize;
  }

  /**
   * Verilen lockCode (conversationId) kullanarak Iyzico'dan ödeme detaylarını
   * sorgular.
   * Bu metot, callback'e alternatif olarak veya manuel doğrulama için
   * kullanılabilir.
   * 
   * @param lockCode Sorgulanacak işleme ait lockCode (Iyzico'daki
   *                 conversationId).
   * @return Iyzico'dan dönen Payment nesnesi.
   */
  public Payment retrievePaymentDetail(String lockCode) {
    logger.info("Iyzico'dan ödeme detayı sorgulanıyor. ConversationId (lockCode): {}", lockCode);

    RetrievePaymentRequest request = new RetrievePaymentRequest();
    request.setLocale(Locale.TR.getValue());
    // HATA DÜZELTMESİ: Iyzico, ödeme detayı sorgularken 'paymentConversationId'
    // alanını bekler.
    // 'conversationId' bu istek için geçerli değildir.
    request.setPaymentConversationId(lockCode);

    try {
      Payment payment = Payment.retrieve(request, getOptions());
      logger.info("Ödeme detayı başarıyla alındı. Status: {}, PaymentId: {}", payment.getStatus(),
          payment.getPaymentId());
      return payment;
    } catch (Exception e) {
      logger.error("Iyzico ödeme detayı sorgulanırken hata oluştu. ConversationId: {}", lockCode, e);
      throw new RuntimeException("Iyzico API ile iletişim kurulamadı: " + e.getMessage(), e);
    }
  }

  private BasketItem createBasketItem(String name, BigDecimal price) {
    BasketItem item = new BasketItem();
    item.setId(name.toUpperCase());
    item.setName(name);
    item.setCategory1("Park");
    item.setItemType(BasketItemType.VIRTUAL.name());
    item.setPrice(price);
    return item;
  }

  public Options getOptions() {
    Options options = new Options();
    options.setApiKey(apiKey);
    options.setSecretKey(secretKey);
    options.setBaseUrl(baseUrl);
    return options;
  }

  public String getIyzicoJsUrl() {
    return baseUrl + "/v1/checkoutform/iyzipay-checkout.js";
  }

  private String getCallbackUrl(HttpServletRequest request) {
    // Callback URL'ini application.properties dosyasından alıyoruz.
    return appCallbackUrl;
  }

  /**
   * Async wrapper method untuk payment initiated event publishing
   * API response'u engellememesi için background thread'de çalışır
   */
  @Async("taskExecutor")
  private void publishPaymentInitiatedEventAsync(DeviceOperation operation) {
    try {
      paymentEventPublisher.publishPaymentInitiatedEvent(operation);
    } catch (Exception e) {
      // Log aber don't throw - async publish failure doesn't affect API response
    }
  }

  /**
   * Async wrapper method untuk payment completed event publishing
   */
  @Async("taskExecutor")
  private void publishPaymentCompletedEventAsync(DeviceOperation operation) {
    try {
      paymentEventPublisher.publishPaymentCompletedEvent(operation);
    } catch (Exception e) {
      // Log aber don't throw - async publish failure doesn't affect API response
    }
  }

  /**
   * Async wrapper method untuk payment failed event publishing
   */
  @Async("taskExecutor")
  private void publishPaymentFailedEventAsync(DeviceOperation operation, String failureReason) {
    try {
      paymentEventPublisher.publishPaymentFailedEvent(operation, failureReason);
    } catch (Exception e) {
      // Log aber don't throw - async publish failure doesn't affect API response
    }
  }

  /**
   * Async wrapper method untuk refund processed event publishing
   */
  @Async("taskExecutor")
  private void publishRefundProcessedEventAsync(DeviceOperation operation) {
    try {
      paymentEventPublisher.publishRefundProcessedEvent(operation);
    } catch (Exception e) {
      // Log aber don't throw - async publish failure doesn't affect API response
    }
  }
}
