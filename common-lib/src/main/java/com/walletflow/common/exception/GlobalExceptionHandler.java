package com.walletflow.common.exception;

import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * Mô hình lỗi THỐNG NHẤT toàn hệ thống: RFC 7807 / 9457 ({@code application/problem+json}).
 *
 * <pre>{@code
 * { "type": "https://walletflow.dev/problems/insufficient-funds",
 *   "title": "Unprocessable Entity", "status": 422,
 *   "detail": "Không đủ số dư", "instance": "/api/v1/transactions/transfer",
 *   "errorCode": "INSUFFICIENT_FUNDS" }
 * }</pre>
 *
 * <p>{@code errorCode} là trường MỞ RỘNG (RFC 7807 cho phép) và là thứ client nên rẽ nhánh theo — ổn
 * định, không bị dịch, không phải parse câu chữ. Giữ đúng tên field của định dạng cũ nên client đang
 * đọc {@code errorCode} không vỡ.
 *
 * <p>Lỗi 500 KHÔNG trả nội dung exception về client: thông điệp kỹ thuật (tên bảng, câu SQL, đường
 * dẫn class) là thông tin cho kẻ tấn công, không phải cho người dùng. Chi tiết đi vào log, kèm
 * request id để tra.
 *
 * <p>Mỗi service chỉ cần component-scan {@code com.walletflow.common} là có.
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

  static final String TYPE_BASE = "https://walletflow.dev/problems/";

  @ExceptionHandler(AppException.class)
  public ResponseEntity<ProblemDetail> handleAppException(AppException ex, HttpServletRequest request) {
    return problem(ex.getStatus(), ex.getErrorCode(), ex.getMessage(), request);
  }

  @ExceptionHandler(MethodArgumentNotValidException.class)
  public ResponseEntity<ProblemDetail> handleValidation(
      MethodArgumentNotValidException ex, HttpServletRequest request) {
    List<Map<String, String>> fields = ex.getBindingResult().getFieldErrors().stream()
        .map(GlobalExceptionHandler::describe)
        .toList();
    ResponseEntity<ProblemDetail> response =
        problem(HttpStatus.BAD_REQUEST, "VALIDATION_ERROR", "Dữ liệu không hợp lệ", request);
    // Danh sách lỗi theo từng field: client hiển thị đúng chỗ thay vì một câu gộp
    response.getBody().setProperty("errors", fields);
    return response;
  }

  /** Body không đọc được (JSON hỏng, giá trị enum lạ như currency "XYZ") — lỗi phía client, không phải 500. */
  @ExceptionHandler(HttpMessageNotReadableException.class)
  public ResponseEntity<ProblemDetail> handleUnreadable(
      HttpMessageNotReadableException ex, HttpServletRequest request) {
    return problem(HttpStatus.BAD_REQUEST, "MALFORMED_REQUEST", "Body không hợp lệ", request);
  }

  @ExceptionHandler(MissingRequestHeaderException.class)
  public ResponseEntity<ProblemDetail> handleMissingHeader(
      MissingRequestHeaderException ex, HttpServletRequest request) {
    return problem(HttpStatus.BAD_REQUEST, "MISSING_HEADER",
        "Thiếu header " + ex.getHeaderName(), request);
  }

  @ExceptionHandler(MethodArgumentTypeMismatchException.class)
  public ResponseEntity<ProblemDetail> handleTypeMismatch(
      MethodArgumentTypeMismatchException ex, HttpServletRequest request) {
    return problem(HttpStatus.BAD_REQUEST, "BAD_REQUEST",
        "Giá trị không hợp lệ cho '" + ex.getName() + "'", request);
  }

  @ExceptionHandler(IllegalArgumentException.class)
  public ResponseEntity<ProblemDetail> handleIllegalArgument(
      IllegalArgumentException ex, HttpServletRequest request) {
    return problem(HttpStatus.BAD_REQUEST, "BAD_REQUEST", ex.getMessage(), request);
  }

  /**
   * Tràn số khi tính tiền ({@code Math.addExact}/{@code subtractExact}/{@code negateExact}). Là LỖI
   * DỮ LIỆU VÀO chứ không phải lỗi hệ thống, nên trả 400 với mã ổn định thay vì 500 (plan §6, §14).
   */
  @ExceptionHandler(ArithmeticException.class)
  public ResponseEntity<ProblemDetail> handleArithmetic(ArithmeticException ex, HttpServletRequest request) {
    return problem(HttpStatus.BAD_REQUEST, "AMOUNT_OVERFLOW", "Số tiền vượt quá miền giá trị", request);
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<ProblemDetail> handleGeneric(Exception ex, HttpServletRequest request) {
    log.error("Lỗi không lường trước ở {} {}", request.getMethod(), request.getRequestURI(), ex);
    return problem(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR",
        "Lỗi hệ thống, vui lòng thử lại sau", request);
  }

  public static ResponseEntity<ProblemDetail> problem(
      HttpStatus status, String errorCode, String detail, HttpServletRequest request) {
    ProblemDetail body = ProblemDetail.forStatusAndDetail(status, detail);
    body.setType(URI.create(TYPE_BASE + errorCode.toLowerCase().replace('_', '-')));
    body.setTitle(status.getReasonPhrase());
    body.setInstance(URI.create(request.getRequestURI()));
    body.setProperty("errorCode", errorCode);
    return ResponseEntity.status(status).contentType(MediaType.APPLICATION_PROBLEM_JSON).body(body);
  }

  private static Map<String, String> describe(FieldError error) {
    return Map.of(
        "field", error.getField(),
        "message", error.getDefaultMessage() == null ? "không hợp lệ" : error.getDefaultMessage());
  }
}
