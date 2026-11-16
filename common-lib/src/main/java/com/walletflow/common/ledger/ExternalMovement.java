package com.walletflow.common.ledger;

import com.walletflow.common.money.ECurrency;

import java.util.UUID;

/**
 * Tiền RÒNG một lệnh tiền đã đưa vào hệ thống từ bên ngoài, theo sổ cái — hợp đồng của
 * {@code POST /internal/entries/external-movements} (roadmap 3.1 PR2a, ADR-024).
 *
 * <p>{@code netInMinor = -SUM(postings vào EXTERNAL_BANK)} qua MỌI bút toán của lệnh đó. Một lần nạp
 * ghi {@code EXTERNAL_BANK -x | ví +x} nên ra {@code +x}; một {@code REVERSAL} đảo lại nên lệnh nạp bị
 * đảo ra {@code 0}. Định nghĩa không phụ thuộc loại bút toán: bất cứ thứ gì chạm {@code EXTERNAL_BANK}
 * đều là tiền đi qua cửa ngoài, và đó đúng là thứ phải khớp với những gì provider thật sự thu/hoàn.
 *
 * <p>Lệnh không có vế nào ở {@code EXTERNAL_BANK} thì KHÔNG có mặt trong câu trả lời — người gọi hiểu
 * là 0. Lệnh có vế nhưng tổng ròng bằng 0 (nạp rồi đảo) thì CÓ mặt với {@code 0}.
 *
 * @param currency {@code EXTERNAL_BANK} là một tài khoản cho mỗi {@code (tenant, currency)}, nên tiền
 *                 ròng tính riêng theo từng loại tiền
 */
public record ExternalMovement(
    UUID transactionId,
    ECurrency currency,
    long netInMinor
) {
}
