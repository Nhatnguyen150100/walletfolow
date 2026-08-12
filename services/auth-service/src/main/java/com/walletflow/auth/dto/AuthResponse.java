package com.walletflow.auth.dto;

import java.util.UUID;

public record AuthResponse(
    UUID userId,
    String email,
    String fullName,
    String role,
    String accessToken) {
}
