package com.walletflow.auth.dto;

import java.util.UUID;

/**
 * @param accessToken          token ngắn hạn, gửi kèm mỗi request ở header {@code Authorization}
 * @param refreshToken         token dài hạn, CHỈ dùng để gọi {@code /auth/refresh}. Giá trị này
 *                             xuất hiện đúng một lần ở đây — server chỉ lưu hash của nó.
 * @param expiresInSeconds     hạn còn lại của access token, để client chủ động refresh trước khi hết
 */
public record AuthResponse(
    UUID userId,
    String email,
    String fullName,
    String role,
    String accessToken,
    String refreshToken,
    long expiresInSeconds,
    String tokenType) {

  public static AuthResponse of(UUID userId, String email, String fullName, String role,
      String accessToken, String refreshToken, long expiresInSeconds) {
    return new AuthResponse(userId, email, fullName, role,
        accessToken, refreshToken, expiresInSeconds, "Bearer");
  }
}
