package com.walletflow.common.exception;

import org.springframework.http.HttpStatus;

/**
 * Danh tính không hợp lệ hoặc đã hết hiệu lực (401).
 *
 * <p>Tách khỏi {@link BadRequestException} vì 400 và 401 nói hai chuyện khác hẳn nhau với client:
 * 400 là "yêu cầu của bạn sai, sửa rồi gửi lại", còn 401 là "hãy đăng nhập lại". Client sẽ xử lý
 * sai nếu ta trả 400 cho một refresh token hết hạn — nó sẽ báo lỗi thay vì chuyển sang màn đăng nhập.
 */
public class UnauthorizedException extends AppException {

  public UnauthorizedException(String message) {
    super(HttpStatus.UNAUTHORIZED, "UNAUTHORIZED", message);
  }

  public UnauthorizedException(String errorCode, String message) {
    super(HttpStatus.UNAUTHORIZED, errorCode, message);
  }
}
