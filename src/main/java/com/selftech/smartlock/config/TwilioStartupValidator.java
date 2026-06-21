package com.selftech.smartlock.config;

import org.springframework.stereotype.Component;

import com.twilio.http.TwilioRestClient;
import com.twilio.rest.api.v2010.Account;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class TwilioStartupValidator {

  private final TwilioRestClient twilioRestClient;

  @PostConstruct
  public void validateTwilioCredentials() {
    try {
      // Gerçek API çağrısı → Auth kesin doğrulanır
      twilioRestClient.getAccountSid();

      Account account = Account.fetcher().fetch(twilioRestClient);

      log.info("✅ Twilio credentials validated successfully", account.getSid());

    } catch (Exception e) {
      log.error("❌ Twilio credential validation FAILED", e);
      throw new IllegalStateException(
          "Twilio credentials are invalid. Application startup aborted.", e);
    }
  }
}
