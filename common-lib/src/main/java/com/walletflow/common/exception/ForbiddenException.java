package com.walletflow.common.exception;

import org.springframework.http.HttpStatus;

/**
 * Đã biết anh là ai, nhưng anh không được làm việc này (khác {@link UnauthorizedException}: ở đó ta
 * chưa biết anh là ai).
 *
 * <p>Chỉ dùng cho quyền theo VAI. Quyền sở hữu ("hàng này không phải của anh") vẫn phải trả
 * {@link ResourceNotFoundException} như cũ — phân biệt "không tồn tại" với "của người khác" là cho
 * người ngoài một máy dò id (CLAUDE.md).
 */
public class ForbiddenException extends AppException {
  public ForbiddenException(String message) {
    super(HttpStatus.FORBIDDEN, "FORBIDDEN", message);
  }

  public ForbiddenException(String errorCode, String message) {
    super(HttpStatus.FORBIDDEN, errorCode, message);
  }
}
