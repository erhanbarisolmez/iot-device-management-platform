package com.selftech.smartlock.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.twilio.Twilio;
import com.twilio.http.TwilioRestClient;

@Configuration
public class TwilioConfig {
  
  @Value("${twilio.account.sid}")
  private String accountSid;

  @Value("${twilio.auth.token}")
  private String authToken;

  @Value("${twilio.trial.number}")
  private String trialNumber;

  public String getPhoneNumber(){
    return trialNumber; 
  }

  @Bean
  public TwilioRestClient twilioRestClient() {
    // Twilio'yu, kimlik bilgileri @Value ile enjekte edildikten sonra burada başlatıyoruz.
    // Bu, @PostConstruct kullanmaktan daha güvenli ve sıralama sorunlarını çözen bir yaklaşımdır.
    Twilio.init(accountSid, authToken);
    return new TwilioRestClient.Builder(accountSid, authToken).build();
  }


}
