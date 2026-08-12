package com.walletflow.common.exception;

import lombok.Getter;
import org.springframework.http.HttpStatus;

/**
 * Exception nghiệp vụ cơ sở, gắn sẵn mã HTTP + mã lỗi nghiệp vụ ổn định
 * (errorCode) để GlobalExceptionHandler và client dùng lại.
 */
@Getter
public class AppException extends RuntimeException {
  private final HttpStatus status;
  private final String errorCode;

  public AppException(HttpStatus status, String errorCode, String message) {
    super(message);
    this.status = status;
    this.errorCode = errorCode;
  }
}
