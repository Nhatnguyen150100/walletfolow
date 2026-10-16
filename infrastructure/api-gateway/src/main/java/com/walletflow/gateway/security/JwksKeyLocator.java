package com.walletflow.gateway.security;

import io.jsonwebtoken.security.Jwk;
import io.jsonwebtoken.security.JwkSet;
import io.jsonwebtoken.security.Jwks;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.security.Key;
import java.security.PublicKey;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Giữ bản sao PUBLIC key của auth-service, lấy qua JWKS, để gateway verify chữ ký token.
 *
 * <p><b>Bộ nhớ đệm là bắt buộc, không phải tối ưu hoá:</b> gateway chạy trên WebFlux, nên mọi thao
 * tác chặn luồng trong filter sẽ chiếm giữ event loop và kéo tụt toàn bộ throughput. Gọi HTTP lấy
 * JWKS cho từng request là điều không được phép làm. Thay vào đó khoá nằm sẵn trong bộ nhớ, còn
 * việc làm mới chạy nền theo lịch — hàm {@link #find} chỉ là một lần tra map, không chặn.
 *
 * <p><b>Xoay khoá:</b> khi auth-service đổi khoá, token mới mang {@code kid} lạ. Lần tra đầu tiên
 * sẽ trượt, và ta kích hoạt làm mới NGAY (không chờ hết chu kỳ) rồi từ chối request đó. Client thử
 * lại sau một nhịp là đi qua. Đổi lấy vài request 401 trong vài giây, ta có được khả năng xoay khoá
 * mà không cần deploy đồng thời auth-service và gateway — thứ mà secret HS256 chia sẻ không bao giờ
 * làm được.
 */
@Slf4j
@Component
public class JwksKeyLocator {

  private final WebClient webClient;
  private final String jwksUri;

  /** volatile + thay nguyên map: người đọc không bao giờ thấy map đang được sửa dở. */
  private volatile Map<String, PublicKey> keysByKid = Map.of();

  /** Chặn hàng loạt request cùng kích hoạt làm mới khi gặp một kid lạ. */
  private final AtomicBoolean refreshing = new AtomicBoolean(false);

  public JwksKeyLocator(
      WebClient.Builder webClientBuilder,
      @Value("${jwt.jwks-uri}") String jwksUri) {
    this.jwksUri = jwksUri;
    this.webClient = webClientBuilder.build();
  }

  /**
   * @return public key ứng với {@code kid}, hoặc {@code null} nếu chưa biết khoá đó
   *         (đồng thời kích hoạt làm mới nền)
   */
  public PublicKey find(String kid) {
    if (kid == null) {
      return null;
    }
    PublicKey key = keysByKid.get(kid);
    if (key == null) {
      log.info("JWKS: chưa biết kid={} — làm mới bộ khoá", kid);
      refreshAsync();
    }
    return key;
  }

  public boolean isReady() {
    return !keysByKid.isEmpty();
  }

  @EventListener(ApplicationReadyEvent.class)
  public void loadOnStartup() {
    refreshAsync();
  }

  @Scheduled(fixedDelayString = "${jwt.jwks-refresh-ms:300000}")
  public void scheduledRefresh() {
    refreshAsync();
  }

  private void refreshAsync() {
    if (!refreshing.compareAndSet(false, true)) {
      return;   // đã có một lần làm mới đang chạy
    }
    webClient.get()
        .uri(jwksUri)
        .retrieve()
        .bodyToMono(String.class)
        .timeout(Duration.ofSeconds(5))
        .doFinally(signal -> refreshing.set(false))
        .subscribe(this::apply, error ->
            // Không ném ra ngoài: giữ nguyên bộ khoá cũ. auth-service tạm thời không truy cập được
            // KHÔNG được phép làm gateway từ chối những token vẫn còn hợp lệ.
            log.error("JWKS: không lấy được bộ khoá từ {} — giữ nguyên {} khoá đang có",
                jwksUri, keysByKid.size(), error));
  }

  private void apply(String json) {
    try {
      JwkSet jwkSet = Jwks.setParser().build().parse(json);
      Map<String, PublicKey> parsed = new HashMap<>();

      for (Jwk<?> jwk : jwkSet.getKeys()) {
        Key key = jwk.toKey();
        if (key instanceof PublicKey publicKey && jwk.getId() != null) {
          parsed.put(jwk.getId(), publicKey);
        } else {
          // JWKS chứa private key là lỗi cấu hình nghiêm trọng ở phía phát hành — kêu to.
          log.error("JWKS: bỏ qua một khoá không dùng được (kid={}, kiểu={})",
              jwk.getId(), key == null ? "null" : key.getClass().getSimpleName());
        }
      }

      if (parsed.isEmpty()) {
        log.error("JWKS: {} không trả về khoá công khai nào — giữ nguyên bộ khoá cũ", jwksUri);
        return;
      }

      keysByKid = Map.copyOf(parsed);
      log.info("JWKS: đã nạp {} khoá từ {} (kid: {})", parsed.size(), jwksUri, parsed.keySet());
    } catch (Exception e) {
      log.error("JWKS: không parse được phản hồi từ {}", jwksUri, e);
    }
  }
}
