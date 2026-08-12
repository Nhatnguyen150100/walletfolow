package com.walletflow.auth.controller;

import com.walletflow.auth.dto.AuthResponse;
import com.walletflow.auth.dto.LoginRequest;
import com.walletflow.auth.dto.RegisterRequest;
import com.walletflow.auth.service.AuthService;
import com.walletflow.common.response.BaseResponse;
import com.walletflow.common.response.ResponseBuilder;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/auth")
@RequiredArgsConstructor
@Tag(name = "Auth", description = "Đăng ký, đăng nhập, cấp JWT")
public class AuthController {

  private final AuthService authService;

  @PostMapping("/register")
  @Operation(summary = "Đăng ký tài khoản mới")
  public ResponseEntity<BaseResponse<AuthResponse>> register(@Valid @RequestBody RegisterRequest req) {
    return ResponseBuilder.created("Đăng ký thành công", authService.register(req));
  }

  @PostMapping("/login")
  @Operation(summary = "Đăng nhập, nhận về access token")
  public ResponseEntity<BaseResponse<AuthResponse>> login(@Valid @RequestBody LoginRequest req) {
    return ResponseBuilder.success("Đăng nhập thành công", authService.login(req));
  }
}
