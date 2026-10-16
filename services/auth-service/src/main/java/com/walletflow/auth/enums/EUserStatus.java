package com.walletflow.auth.enums;

public enum EUserStatus {
  /** Dùng bình thường. */
  ACTIVE,
  /** Khoá tạm thời do đăng nhập sai nhiều lần — tự mở khi hết {@code lockedUntil}. */
  LOCKED,
  /** Vô hiệu hoá bởi quản trị; không tự mở. */
  DISABLED
}
