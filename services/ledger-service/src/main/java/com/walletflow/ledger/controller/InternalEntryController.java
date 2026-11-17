package com.walletflow.ledger.controller;

import com.walletflow.common.ledger.ExternalMovement;
import com.walletflow.common.ledger.ExternalMovementsRequest;
import com.walletflow.common.ledger.TopupEntryRef;
import com.walletflow.common.ledger.TransactionEntries;
import com.walletflow.ledger.services.EntryLookupService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
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

    /**
     * Tiền ròng qua {@code EXTERNAL_BANK} theo LÔ lệnh tiền — bank-adapter gọi để đối soát với
     * {@code provider_transactions} (roadmap 3.1 PR2a, ADR-024). {@code POST} tuy chỉ đọc: xem
     * {@link ExternalMovementsRequest}.
     */
    @PostMapping("/external-movements")
    public List<ExternalMovement> externalMovements(@RequestBody ExternalMovementsRequest request) {
        return entryLookupService.externalMovementsOf(request);
    }

    /**
     * Trang bút toán {@code TOPUP} XUYÊN tenant — đường quét chiều ngược của cùng phép đối soát.
     * Không cần {@code X-Tenant-Id}; mỗi phần tử mang tenant của chính nó.
     *
     * <p>{@code before} khai {@code required = false} dù nó BẮT BUỘC: thiếu tham số bắt buộc thì Spring
     * ném {@code MissingServletRequestParameterException}, mà {@code GlobalExceptionHandler} không có
     * handler riêng cho nó nên rơi xuống 500. Để service từ chối thì người gọi nhận đúng 400.
     */
    @GetMapping("/topups")
    public List<TopupEntryRef> topups(@RequestParam(required = false) Instant afterPostedAt,
                                      @RequestParam(required = false) UUID afterId,
                                      @RequestParam(required = false) Instant before,
                                      @RequestParam(defaultValue = "500") int limit) {
        return entryLookupService.topupsAfter(afterPostedAt, afterId, before, limit);
    }
}
