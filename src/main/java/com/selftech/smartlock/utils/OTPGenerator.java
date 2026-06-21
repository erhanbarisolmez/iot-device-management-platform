package com.selftech.smartlock.utils;

import java.security.SecureRandom;

public class OTPGenerator {

  private static final SecureRandom random = new SecureRandom();

  /**
   * Belirtilen uzunlukta kriptografik olarak güvenli bir OTP oluşturur.
   * @param length OTP'nin uzunluğu.
   * @return Oluşturulan OTP string'i.
   */
  public static String generate(int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(random.nextInt(10));
    }
    return sb.toString();
  }
}
