package com.walletflow.common.exception;

import org.springframework.http.HttpStatus;

public class ConflictException extends AppException {
  public ConflictException(String message) {
    super(HttpStatus.CONFLICT, "CONFLICT", message);
  }

  public ConflictException(String errorCode, String message) {
    super(HttpStatus.CONFLICT, errorCode, message);
  }
}
