package com.walletflow.common.exception;

import org.springframework.http.HttpStatus;

/**
 * Số dư không đủ để thực hiện giao dịch. Trả 422 Unprocessable Entity với mã ổn định
 * INSUFFICIENT_FUNDS để client phân biệt với lỗi validate thông thường (400).
 */
public class InsufficientFundsException extends AppException {
  public InsufficientFundsException(String message) {
    super(HttpStatus.UNPROCESSABLE_ENTITY, "INSUFFICIENT_FUNDS", message);
  }
}
