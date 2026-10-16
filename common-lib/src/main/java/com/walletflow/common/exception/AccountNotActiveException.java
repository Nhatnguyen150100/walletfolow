package com.walletflow.common.exception;

import org.springframework.http.HttpStatus;

/**
 * Tài khoản đang ở trạng thái không cho phép thao tác này (ví bị đóng băng mà bị ghi nợ, ví đã
 * đóng). Là TỪ CHỐI NGHIỆP VỤ — thử lại không thay đổi kết quả — nên kế thừa {@link AppException}
 * để ledger phát {@code LedgerRejectedEvent} thay vì retry rồi đẩy vào DLT.
 */
public class AccountNotActiveException extends AppException {
  public AccountNotActiveException(String errorCode, String message) {
    super(HttpStatus.UNPROCESSABLE_ENTITY, errorCode, message);
  }
}
