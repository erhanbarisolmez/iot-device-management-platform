package com.selftech.smartlock.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;

@Controller
@RequestMapping("/pay/deeplink")
@RequiredArgsConstructor
public class DeepLinkController {
  private static final Logger logger = LoggerFactory.getLogger(DeepLinkController.class);

  @Value("${app.store.ios-url:https://apps.apple.com/app/idYOUR_APP_ID}")
  private String iosAppStoreLink;

  @Value("${app.store.android-url:https://play.google.com/store/apps/details?id=com.your.package}")
  private String androidPlayStoreLink;

  @GetMapping
  public String handleDeepLink(
      @RequestParam String code,
      HttpServletRequest request,
      Model model) {

    String userAgent = request.getHeader("User-Agent");
    model.addAttribute("code", code);
    
    logger.info("Deep link isteği alındı. Code: {}, User-Agent: {}", code, userAgent);
    // Flutter deep link
    String flutterDeepLink = "selfpark://payment?code=" + code;
    model.addAttribute("flutterDeepLink", flutterDeepLink);

    // Platform linkleri IOS ve Android için
    model.addAttribute("playStoreUrl", androidPlayStoreLink);
    model.addAttribute("appStoreUrl", iosAppStoreLink);
    // Platform tespiti
    if (isAndroid(userAgent)) {
      model.addAttribute("platform", "android");
    } else if (isIOS(userAgent)) {
      model.addAttribute("platform", "ios");
    }

    return "app-deeplink";
  }

  private boolean isAndroid(String userAgent) {
    return userAgent != null && userAgent.toLowerCase().contains("android");
  }

  private boolean isIOS(String userAgent) {
    return userAgent != null && (userAgent.contains("iPhone") || userAgent.contains("iPad") || userAgent.contains("iPod"));
  }

}
