package com.walletflow.common.exception;

import org.springframework.http.HttpStatus;

public class BadRequestException extends AppException {
  public BadRequestException(String message) {
    super(HttpStatus.BAD_REQUEST, "BAD_REQUEST", message);
  }

  public BadRequestException(String errorCode, String message) {
    super(HttpStatus.BAD_REQUEST, errorCode, message);
  }
}
