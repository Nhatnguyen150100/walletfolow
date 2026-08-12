package com.walletflow.common.exception;

import com.walletflow.common.response.BaseResponse;
import com.walletflow.common.response.ResponseBuilder;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.stream.Collectors;

/**
 * Bộ bắt lỗi toàn cục dùng chung. Mỗi service chỉ cần component-scan com.walletflow.common là có.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

  @ExceptionHandler(AppException.class)
  public ResponseEntity<BaseResponse<Void>> handleAppException(AppException ex) {
    return ResponseBuilder.error(ex.getStatus(), ex.getErrorCode(), ex.getMessage());
  }

  @ExceptionHandler(MethodArgumentNotValidException.class)
  public ResponseEntity<BaseResponse<Void>> handleValidation(MethodArgumentNotValidException ex) {
    String msg = ex.getBindingResult().getFieldErrors().stream()
        .map(FieldError::getDefaultMessage)
        .collect(Collectors.joining("; "));
    return ResponseBuilder.error(HttpStatus.BAD_REQUEST, "VALIDATION_ERROR", msg);
  }

  @ExceptionHandler(IllegalArgumentException.class)
  public ResponseEntity<BaseResponse<Void>> handleIllegalArgument(IllegalArgumentException ex) {
    return ResponseBuilder.error(HttpStatus.BAD_REQUEST, "BAD_REQUEST", ex.getMessage());
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<BaseResponse<Void>> handleGeneric(Exception ex) {
    return ResponseBuilder.error(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR",
        "Lỗi hệ thống: " + ex.getMessage());
  }
}
