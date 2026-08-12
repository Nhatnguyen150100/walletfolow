package com.walletflow.common.security;

/**
 * Tên các header định danh người dùng mà API Gateway gắn vào request SAU KHI đã xác thực
 * JWT thành công, rồi chuyển tiếp xuống các service nội bộ (mô hình "Gateway-centric":
 * Gateway verify 1 lần, service nội bộ tin tưởng header này).
 */
public final class AuthHeaders {

  private AuthHeaders() {
  }

  public static final String USER_ID = "X-User-Id";
  public static final String USER_EMAIL = "X-User-Email";
  public static final String USER_ROLE = "X-User-Role";
}
