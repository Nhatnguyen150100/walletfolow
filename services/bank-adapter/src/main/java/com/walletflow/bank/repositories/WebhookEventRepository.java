package com.walletflow.bank.repositories;

import com.walletflow.bank.entity.WebhookEvent;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

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
     * Số webhook đã nhận mà chưa ai xử lý. Trong 2.3a worker chưa tồn tại nên con số này chỉ tăng — đó
     * chính là lý do phải đo: nếu nó lớn mà 2.3b chưa lên thì ta đang nói "đã ghi nhận" với provider cho
     * những sự kiện không ai đọc.
     */
    @Query(value = "SELECT count(*) FROM webhook_events WHERE processed_at IS NULL", nativeQuery = true)
    long countUnprocessed();
}
