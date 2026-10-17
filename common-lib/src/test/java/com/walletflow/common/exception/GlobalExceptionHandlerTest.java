package com.walletflow.common.exception;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;

import static org.assertj.core.api.Assertions.assertThat;

class GlobalExceptionHandlerTest {

  private final GlobalExceptionHandler handler = new GlobalExceptionHandler();
  private final MockHttpServletRequest request =
      new MockHttpServletRequest("POST", "/api/v1/transactions/transfer");

  @Test
  @DisplayName("Lỗi nghiệp vụ: RFC 7807 với errorCode ổn định, type suy từ mã, instance là path")
  void loi_nghiep_vu_theo_rfc7807() {
    ResponseEntity<ProblemDetail> response =
        handler.handleAppException(new InsufficientFundsException("Không đủ số dư"), request);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
    assertThat(response.getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
    ProblemDetail body = response.getBody();
    assertThat(body.getProperties()).containsEntry("errorCode", "INSUFFICIENT_FUNDS");
    assertThat(body.getType()).hasToString("https://walletflow.dev/problems/insufficient-funds");
    assertThat(body.getInstance()).hasToString("/api/v1/transactions/transfer");
    assertThat(body.getDetail()).isEqualTo("Không đủ số dư");
  }

  @Test
  @DisplayName("Lỗi 500: KHÔNG trả thông điệp kỹ thuật về client")
  void loi_he_thong_khong_lo_chi_tiet() {
    ResponseEntity<ProblemDetail> response = handler.handleGeneric(
        new IllegalStateException("relation \"idempotency_keys\" violates constraint pk_xyz"), request);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
    assertThat(response.getBody().getDetail())
        .as("tên bảng/ràng buộc là thông tin cho kẻ tấn công, không phải cho người dùng")
        .doesNotContain("idempotency_keys");
    assertThat(response.getBody().getProperties()).containsEntry("errorCode", "INTERNAL_ERROR");
  }
}
