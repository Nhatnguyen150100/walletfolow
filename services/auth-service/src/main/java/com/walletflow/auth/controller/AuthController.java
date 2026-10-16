package com.walletflow.auth.controller;

import com.walletflow.auth.dto.AuthResponse;
import com.walletflow.auth.dto.LoginRequest;
import com.walletflow.auth.dto.RefreshRequest;
import com.walletflow.auth.dto.RegisterRequest;
import com.walletflow.auth.service.AuthService;
import com.walletflow.common.response.BaseResponse;
import com.walletflow.common.response.ResponseBuilder;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/auth")
@RequiredArgsConstructor
@Tag(name = "Auth", description = "Đăng ký, đăng nhập, xoay token")
public class AuthController {

  private final AuthService authService;

  @PostMapping("/register")
  @Operation(summary = "Đăng ký tài khoản mới")
  public ResponseEntity<BaseResponse<AuthResponse>> register(
      @Valid @RequestBody RegisterRequest req, HttpServletRequest http) {
    return ResponseBuilder.created("Đăng ký thành công",
        authService.register(req, clientIp(http), userAgent(http)));
  }

  @PostMapping("/login")
  @Operation(summary = "Đăng nhập, nhận access token + refresh token")
  public ResponseEntity<BaseResponse<AuthResponse>> login(
      @Valid @RequestBody LoginRequest req, HttpServletRequest http) {
    return ResponseBuilder.success("Đăng nhập thành công",
        authService.login(req, clientIp(http), userAgent(http)));
  }

  @PostMapping("/refresh")
  @Operation(summary = "Đổi refresh token lấy cặp token mới (token cũ bị thu hồi)")
  public ResponseEntity<BaseResponse<AuthResponse>> refresh(
      @Valid @RequestBody RefreshRequest req, HttpServletRequest http) {
    return ResponseBuilder.success("Làm mới token thành công",
        authService.refresh(req.refreshToken(), clientIp(http), userAgent(http)));
  }

  @PostMapping("/logout")
  @Operation(summary = "Đăng xuất — thu hồi toàn bộ chuỗi token của phiên này")
  public ResponseEntity<BaseResponse<Void>> logout(@Valid @RequestBody RefreshRequest req) {
    authService.logout(req.refreshToken());
    return ResponseBuilder.success("Đã đăng xuất");
  }

  /**
   * IP của client.
   *
   * <p>Ưu tiên {@code X-Forwarded-For} vì service luôn nằm sau gateway, nên
   * {@code getRemoteAddr()} chỉ cho ra IP của gateway — vô dụng cho audit.
   *
   * <p><b>Lưu ý bảo mật:</b> header này do client gửi nên giả mạo được. Nó CHỈ dùng để ghi audit,
   * tuyệt đối không được dùng làm căn cứ phân quyền hay allowlist. Khi triển khai thật, gateway
   * phải ghi đè header này chứ không chuyển tiếp giá trị client gửi vào.
   */
  private static String clientIp(HttpServletRequest http) {
    String forwarded = http.getHeader("X-Forwarded-For");
    if (forwarded != null && !forwarded.isBlank()) {
      return forwarded.split(",")[0].trim();
    }
    return http.getRemoteAddr();
  }

  private static String userAgent(HttpServletRequest http) {
    return http.getHeader(HttpHeaders.USER_AGENT);
  }
}
