package com.walletflow.common.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;

/**
 * Định dạng response chuẩn dùng chung cho TẤT CẢ các service.
 * {@code errorCode} là mã lỗi nghiệp vụ ổn định (vd INSUFFICIENT_FUNDS) để client xử lý theo mã,
 * không phải parse chuỗi message.
 */
@Data
@Builder
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BaseResponse<T> {
  private boolean success;
  private int status;
  private String errorCode;
  private String message;
  private T data;
  @Builder.Default
  private Instant timestamp = Instant.now();
}
