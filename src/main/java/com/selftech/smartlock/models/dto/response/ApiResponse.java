package com.selftech.smartlock.models.dto.response;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ApiResponse {
  private boolean success;
  private String message;

  public static ApiResponse success(String msg) {
    return new ApiResponse(true, msg);
  }

  public static ApiResponse error(String msg) {
    return new ApiResponse(false, msg);
  }

}
