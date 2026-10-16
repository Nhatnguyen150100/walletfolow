package com.walletflow.ledger.controller;

import com.walletflow.common.ledger.AccountStatement;
import com.walletflow.common.money.ECurrency;
import com.walletflow.ledger.services.AccountService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * API NỘI BỘ của ledger — không có route nào ở gateway trỏ tới đây (discovery locator đã tắt), chỉ
 * service khác trong mạng nội bộ gọi được.
 *
 * <p>Chỉ ĐỌC. Mọi thay đổi sổ cái vẫn đi qua Kafka command, để có idempotency, outbox và DLT; mở
 * một đường ghi qua HTTP là mở một đường vòng qua cả ba.
 *
 * <p>Tenant lấy từ header {@code X-Tenant-Id} qua {@code TenantFilter}. Trả thẳng record hợp đồng
 * ({@link AccountStatement}) chứ không bọc {@code BaseResponse}: người gọi là máy, và lỗi đã được
 * mô tả bằng HTTP status.
 *
 * <p>⚠️ Cùng mô hình tin cậy với mọi service nội bộ hiện tại: tin header vì tin mạng (GAP-B5). Phase 4
 * thay bằng mTLS.
 */
@RestController
@RequestMapping("/internal/accounts")
@RequiredArgsConstructor
public class InternalAccountController {

    private final AccountService accountService;

    @GetMapping("/{ref}")
    public AccountStatement statement(@PathVariable String ref, @RequestParam ECurrency currency) {
        return accountService.statementOf(ref, currency);
    }
}
