package com.walletflow.common.event;

import com.walletflow.common.money.ECurrency;

import java.util.UUID;

/**
 * Lệnh hoàn tiền cho MỘT lần thu đã thành công của một giao dịch nạp tiền. Topic
 * {@code txn.command.bank-refund.v1}, producer transaction-service (bù trừ saga top-up, 2.4b), consumer
 * bank-adapter.
 *
 * <p>Chỉ hoàn TRỌN số đã thu: {@code walletId}/{@code amountMinor}/{@code currency} phải khớp đúng lần thu của
 * {@code transactionId}. Khác đi là bug phía producer → DLT — hoàn theo con số của lệnh là trả người dùng số
 * tiền họ chưa từng trả.
 *
 * <p>Idempotent theo {@code (tenant, transactionId)} như lệnh thu: gửi lại bao nhiêu lần cũng chỉ có MỘT lần
 * hoàn ở provider, và lần nào cũng nhận lại câu trả lời hiện tại ({@code bank.event.completed.v1} /
 * {@code bank.event.failed.v1} với {@code operation = REFUND}).
 *
 * @param transactionId giao dịch nạp tiền cần hoàn — khoá idempotency
 * @param walletId      ví của lần nạp; cũng là partition key (cùng partition với lệnh thu)
 * @param amountMinor   số tiền hoàn theo đơn vị nhỏ nhất (ADR-005) — bằng đúng số đã thu
 * @param currency      loại tiền
 * @param reasonCode    vì sao phải hoàn (mã từ chối của ledger, {@code BANK_RESULT_CONFLICT}...) — để đối soát,
 *                      không ảnh hưởng cách hoàn
 */
public record BankRefundCommand(
    UUID transactionId,
    UUID walletId,
    long amountMinor,
    ECurrency currency,
    String reasonCode
) {
}
