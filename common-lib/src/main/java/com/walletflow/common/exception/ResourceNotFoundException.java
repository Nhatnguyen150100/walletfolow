package com.walletflow.common.exception;

import org.springframework.http.HttpStatus;

public class ResourceNotFoundException extends AppException {
  public ResourceNotFoundException(String message) {
    super(HttpStatus.NOT_FOUND, "NOT_FOUND", message);
  }
}
