package com.walletflow.ledger.controller;

import com.walletflow.common.ledger.AccountBalance;
import com.walletflow.common.ledger.AccountBalancesRequest;
import com.walletflow.common.ledger.AccountStatement;
import com.walletflow.common.money.ECurrency;
import com.walletflow.ledger.services.AccountService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

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

    /**
     * Số dư theo LÔ — đường đọc của đối soát tầng 2 (roadmap 3.1, ADR-023 §3).
     *
     * <p>{@code POST} cho một thao tác chỉ ĐỌC và không đổi gì ở sổ cái: danh sách vài trăm ref
     * không nhét vừa query string một cách an toàn. Lý lẽ đầy đủ ở {@link AccountBalancesRequest}.
     * Việc này KHÔNG mở một đường ghi — xem ghi chú đầu class.
     */
    @PostMapping("/balances")
    public List<AccountBalance> balances(@RequestBody AccountBalancesRequest request) {
        return accountService.balancesOf(request);
    }
}
