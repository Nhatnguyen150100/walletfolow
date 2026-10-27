package com.walletflow.bank.dto;

import java.time.Instant;
import java.util.UUID;

/**
 * Projection cho truy vấn ngữ cảnh người trực (roadmap 2.5 PR3).
 *
 * <p>Phải là projection chứ không phải {@code ProviderTransaction}: {@code raw_response} cố ý KHÔNG
 * map trong entity (xem javadoc của entity), và việc map nó vào entity chỉ để phục vụ một đường đọc
 * sẽ kéo một cột JSONB nặng vào MỌI lần tải hàng ở đường ghi nóng.
 *
 * <p>{@code operation}/{@code currency}/{@code status} là {@code String}: truy vấn chạy native nên
 * không có Hibernate convert enum, và ép kiểu đúng chỗ ở service thì lỗi dữ liệu nổ ra ở một nơi
 * nhìn thấy được.
 */
public interface ProviderOperationRow {

    UUID getId();

    String getOperation();

    String getProvider();

    String getProviderRef();

    long getAmountMinor();

    String getCurrency();

    String getStatus();

    String getFailureCode();

    Instant getCreatedAt();

    Instant getUpdatedAt();

    UUID getRefundOf();

    /** JSON thô của provider dưới dạng text — CHƯA che, service phải mask trước khi trả ra ngoài. */
    String getRawResponse();
}
