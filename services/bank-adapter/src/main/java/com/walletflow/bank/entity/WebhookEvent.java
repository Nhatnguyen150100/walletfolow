package com.walletflow.bank.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * Một webhook/IPN ĐÃ QUA KIỂM CHỮ KÝ mà cổng thanh toán gửi tới (ADR-018). Chỉ đọc: hàng được chèn một
 * lần bằng {@code INSERT … ON CONFLICT DO NOTHING} và chỉ còn một thao tác ghi duy nhất là đánh dấu đã
 * xử lý, bằng câu UPDATE có điều kiện trong {@code WebhookEventRepository}.
 *
 * <p>Webhook là <b>gợi ý</b>, không phải sự thật (ADR-016 §1). Hàng này không bao giờ là căn cứ ghi tiền;
 * nó chỉ kích hoạt việc adapter tự hỏi lại provider bằng {@code getStatus}.
 */
@Entity
@Getter
@NoArgsConstructor
@Table(name = "webhook_events")
public class WebhookEvent {

    @Id
    private UUID id;

    @Column(nullable = false, length = 32)
    private String provider;

    /** SHA-256 của chuỗi đã ký — khoá chống phát lại, xem V4. */
    @Column(name = "provider_event_id", nullable = false, length = 64)
    private String providerEventId;

    @Column(name = "provider_ref", length = 128)
    private String providerRef;

    /** NULL cho tới khi worker (2.3b) tra được qua {@code provider_ref}; xem V4. */
    @Column(name = "tenant_id")
    private UUID tenantId;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    @Column(name = "processed_at")
    private Instant processedAt;

    // raw_payload cố ý không map: nó chỉ là bằng chứng đối soát, đọc bằng SQL khi điều tra. Kéo cả
    // payload vào mọi lần đọc entity là tốn vô ích, và dễ khiến ai đó log nguyên nó ra.
}
