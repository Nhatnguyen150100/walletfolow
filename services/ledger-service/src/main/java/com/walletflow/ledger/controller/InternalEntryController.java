package com.walletflow.ledger.controller;

import com.walletflow.common.ledger.TransactionEntries;
import com.walletflow.ledger.services.EntryLookupService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/**
 * API NỘI BỘ tra bút toán theo lệnh tiền (roadmap 2.5 PR3) — transaction-service gọi để ghép ngữ
 * cảnh cho người trực.
 *
 * <p>Cùng mô hình với {@link InternalAccountController}: không có route nào ở gateway trỏ tới đây,
 * chỉ ĐỌC, tenant lấy từ {@code X-Tenant-Id} qua {@code TenantFilter}, và tin header vì tin mạng
 * (GAP-B5 — Phase 4 thay bằng mTLS).
 */
@RestController
@RequestMapping("/internal/entries")
@RequiredArgsConstructor
public class InternalEntryController {

    private final EntryLookupService entryLookupService;

    @GetMapping
    public TransactionEntries entries(@RequestParam UUID transactionId) {
        return entryLookupService.entriesOf(transactionId);
    }
}
