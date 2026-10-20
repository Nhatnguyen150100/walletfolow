package com.walletflow.bank.repositories;

import com.walletflow.bank.entity.WebhookEvent;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

public interface WebhookEventRepository extends Repository<WebhookEvent, UUID> {

    /**
     * Ghi lại một webhook đã qua kiểm chữ ký. Webhook trùng (provider phát lại y hệt) không đụng gì —
     * Postgres bắt câu INSERT thứ hai chờ ở unique key tới khi câu thứ nhất commit, nên không bao giờ có
     * hai hàng và không lần nào vỡ unique key thành lỗi 500 trả về cho provider.
     *
     * <p>{@code tenant_id} để NULL: webhook tới từ Internet, tenant chỉ suy ra được sau khi tra
     * {@code provider_ref} — việc của worker 2.3b (xem V4).
     *
     * @return 1 nếu vừa ghi, 0 nếu đã nhận webhook này rồi
     */
    @Modifying
    @Query(value = """
            INSERT INTO webhook_events (id, provider, provider_event_id, provider_ref, raw_payload, received_at)
            VALUES (:id, :provider, :providerEventId, :providerRef, :rawPayload, now())
            ON CONFLICT (provider, provider_event_id) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("id") UUID id,
                       @Param("provider") String provider,
                       @Param("providerEventId") String providerEventId,
                       @Param("providerRef") String providerRef,
                       @Param("rawPayload") String rawPayload);

    /**
     * Số webhook đã nhận mà chưa ai xử lý — tồn đọng của worker (2.3b). Phải dao động quanh 0; nó lớn và
     * không tụt nghĩa là ta đang nói "đã ghi nhận" với provider cho những sự kiện không ai đọc được
     * (alert {@code BankWebhookBacklog}).
     */
    @Query(value = "SELECT count(*) FROM webhook_events WHERE processed_at IS NULL", nativeQuery = true)
    long countUnprocessed();

    /**
     * Lô webhook chưa xử lý đã tới hạn thử, cũ trước. {@code SKIP LOCKED}: nhiều instance chạy cùng lúc
     * mỗi cái lấy một lô khác nhau.
     *
     * <p>Chỉ hàng của provider đang chạy: tra ra lần thu rồi cũng phải hỏi đúng nơi đã tạo phiên
     * ({@code ChargeStatusService.requireSameOrigin}), nên hàng của provider cũ có lấy ra cũng chỉ nổ.
     * Bị bỏ lại, chúng hiện ở gauge tồn đọng — thấy được, chứ không biến mất im lặng.
     */
    @Query(value = """
            SELECT * FROM webhook_events
             WHERE processed_at IS NULL AND provider = :provider AND next_attempt_at <= now()
             ORDER BY next_attempt_at
             LIMIT :limit
               FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<WebhookEvent> lockDueUnprocessed(@Param("provider") String provider, @Param("limit") int limit);

    /**
     * Lùi lần thử kế tiếp — gọi NGAY khi lấy lô, trước khi gọi provider. Provider lỗi (hay cả process
     * chết) thì hàng đó vẫn đã được lùi, nên không bị thử lại liên tục mỗi lượt quét và không chặn các
     * webhook tới sau nó.
     *
     * <p>Giãn theo cấp số {@code base × 2^số_lần_đã_thử}, trần {@code maxSeconds} — cùng khuôn với
     * {@code deferChecks} của lần thu. Vế phải của SET đọc {@code attempts} CŨ.
     */
    @Modifying
    @Query(value = """
            UPDATE webhook_events
               SET next_attempt_at = now() + make_interval(secs => LEAST(:maxSeconds,
                                          :baseSeconds * power(2, LEAST(attempts, 30)))),
                   attempts = attempts + 1
             WHERE id IN (:ids) AND processed_at IS NULL
            """, nativeQuery = true)
    int deferAttempts(@Param("ids") Collection<UUID> ids,
                      @Param("baseSeconds") long baseSeconds,
                      @Param("maxSeconds") long maxSeconds);

    /**
     * Đóng một webhook đã xử lý xong. Gọi SAU khi đã hỏi provider — đóng trước là tự nhận "đã xử lý" cho
     * một việc có thể chưa xảy ra.
     *
     * <p>{@code tenant_id} chỉ được ghi ở đây, bằng giá trị TRA ĐƯỢC từ {@code provider_ref}, không bao
     * giờ bằng thứ gì bên ngoài gửi (ADR-018 §6). NULL khi không tra ra.
     *
     * @return 1 nếu vừa đóng, 0 nếu luồng khác đã đóng trước
     */
    @Modifying
    @Query(value = """
            UPDATE webhook_events
               SET processed_at = now(), tenant_id = :tenantId, outcome = :outcome
             WHERE id = :id AND processed_at IS NULL
            """, nativeQuery = true)
    int markProcessed(@Param("id") UUID id,
                      @Param("tenantId") UUID tenantId,
                      @Param("outcome") String outcome);

    /**
     * Hàng đã dùng hết hạn mức thử MÀ VẪN chưa xử lý được: đóng lại với {@code ABANDONED}, thôi thử.
     *
     * <p>Gọi SAU khi đã chạy xong cả lô và chỉ chạm hàng còn {@code processed_at IS NULL}, nên webhook
     * vừa xử lý xong ở chính lượt chót KHÔNG bị đánh nhầm là bỏ cuộc. Không có trần thì một hàng không
     * bao giờ tra ra được sẽ ở lại tồn đọng mãi và dìm chết tín hiệu của gauge.
     *
     * @return số hàng vừa hết lượt; caller cộng vào counter {@code walletflow.bank.webhook.abandoned}
     */
    @Modifying
    @Query(value = """
            UPDATE webhook_events
               SET processed_at = now(), outcome = :outcome
             WHERE id IN (:ids) AND processed_at IS NULL AND attempts >= :maxAttempts
            """, nativeQuery = true)
    int abandonExhausted(@Param("ids") Collection<UUID> ids,
                         @Param("maxAttempts") int maxAttempts,
                         @Param("outcome") String outcome);
}
