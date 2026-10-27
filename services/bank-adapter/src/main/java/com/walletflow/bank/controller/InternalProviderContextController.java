package com.walletflow.bank.controller;

import com.walletflow.bank.services.ProviderContextService;
import com.walletflow.common.bank.ProviderContext;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/**
 * API NỘI BỘ tra ngữ cảnh provider theo lệnh tiền (roadmap 2.5 PR3) — transaction-service gọi để
 * ghép vào màn hình của người trực.
 *
 * <p>Không có route nào ở gateway trỏ tới {@code /internal/**}, chỉ service trong mạng nội bộ gọi
 * được. Chỉ ĐỌC: mọi thao tác với provider vẫn chỉ khởi động bằng Kafka command, để giữ idempotency
 * và outbox.
 *
 * <p>Tenant lấy từ {@code X-Tenant-Id} qua {@code TenantFilter} — tin header vì tin mạng (GAP-B5,
 * Phase 4 thay bằng mTLS). Lưu ý đây là ranh giới DUY NHẤT bảo vệ dữ liệu của khách này khỏi màn
 * hình của khách khác trên đường đi ra, nên thiếu header thì {@code TenantContext.require()} phải
 * nổ chứ không được đoán.
 */
@RestController
@RequestMapping("/internal/provider-transactions")
@RequiredArgsConstructor
public class InternalProviderContextController {

    private final ProviderContextService providerContextService;

    @GetMapping
    public ProviderContext context(@RequestParam UUID transactionId) {
        return providerContextService.contextOf(transactionId);
    }
}
