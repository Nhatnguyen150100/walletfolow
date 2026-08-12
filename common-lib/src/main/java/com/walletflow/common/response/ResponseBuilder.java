package com.walletflow.common.response;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/**
 * Tiện ích tạo ResponseEntity chuẩn hoá.
 */
public final class ResponseBuilder {

  private ResponseBuilder() {
  }

  public static <T> ResponseEntity<BaseResponse<T>> success(String message, T data) {
    return build(HttpStatus.OK, true, null, message, data);
  }

  public static ResponseEntity<BaseResponse<Void>> success(String message) {
    return build(HttpStatus.OK, true, null, message, null);
  }

  public static <T> ResponseEntity<BaseResponse<T>> created(String message, T data) {
    return build(HttpStatus.CREATED, true, null, message, data);
  }

  public static <T> ResponseEntity<BaseResponse<T>> error(HttpStatus status, String errorCode, String message) {
    return build(status, false, errorCode, message, null);
  }

  public static <T> ResponseEntity<BaseResponse<T>> build(
      HttpStatus status, boolean success, String errorCode, String message, T data) {
    BaseResponse<T> body = BaseResponse.<T>builder()
        .success(success)
        .status(status.value())
        .errorCode(errorCode)
        .message(message)
        .data(data)
        .build();
    return ResponseEntity.status(status).body(body);
  }
}
