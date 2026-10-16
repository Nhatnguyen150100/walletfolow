package com.walletflow.common.tenant;

import com.walletflow.common.security.AuthHeaders;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * BIÊN VÀO của luồng HTTP: đặt {@link TenantContext} từ header {@code X-Tenant-Id} do gateway gắn.
 * Là bản đối xứng của việc Kafka listener đặt tenant từ {@code envelope.tenantId}.
 *
 * <p><b>Thiếu header thì cho qua — KHÔNG đặt tenant mặc định.</b> Request không có tenant hợp lệ
 * với path công khai (actuator, đăng nhập). Nếu nó chạm tới dữ liệu nghiệp vụ, {@code
 * TenantContext.require()} sẽ nổ ở đó. Chặn ở đây sẽ phải liệt kê path công khai lần thứ hai (lần
 * đầu ở gateway), và hai danh sách sẽ lệch nhau; đoán tenant ở đây là đúng kiểu rò rỉ chéo tenant mà
 * {@code TenantContext} sinh ra để chặn.
 *
 * <p><b>Header sai định dạng thì từ chối 400.</b> Gateway không bao giờ gửi giá trị như vậy, nên nó
 * chỉ có thể đến từ một đường vào không đi qua gateway — thứ cần lộ ra ngay chứ không bỏ qua.
 *
 * <p>Dùng {@code set} + {@code clear} trong {@code finally} thay vì {@link TenantContext#runWith}:
 * {@code chain.doFilter} ném checked exception mà {@code Runnable} không cho phép. Thread của
 * servlet container được tái sử dụng, nên {@code finally} ở đây là bắt buộc.
 */
public class TenantFilter extends OncePerRequestFilter {

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {

    String header = request.getHeader(AuthHeaders.TENANT_ID);
    if (header == null || header.isBlank()) {
      chain.doFilter(request, response);
      return;
    }

    UUID tenantId;
    try {
      tenantId = UUID.fromString(header.trim());
    } catch (IllegalArgumentException malformed) {
      reject(response);
      return;
    }

    TenantContext.set(tenantId);
    try {
      chain.doFilter(request, response);
    } finally {
      TenantContext.clear();
    }
  }

  private static void reject(HttpServletResponse response) throws IOException {
    response.setStatus(HttpStatus.BAD_REQUEST.value());
    response.setContentType(MediaType.APPLICATION_JSON_VALUE);
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    response.getWriter().write("{\"success\":false,\"status\":400,\"errorCode\":\"INVALID_TENANT\","
        + "\"message\":\"Header " + AuthHeaders.TENANT_ID + " không phải UUID hợp lệ\"}");
  }
}
