package com.walletflow.ledger.dto;

/**
 * Số dư của một tài khoản trong lượt hỏi theo lô — nguồn của
 * {@code POST /internal/accounts/balances} (ADR-023 §3).
 *
 * <p>Interface projection, cùng lý do như {@link ImbalanceRow}: truy vấn là native vì nó cần
 * {@code LEFT JOIN} để tài khoản chưa có posting nào vẫn trả về 0 thay vì biến mất.
 */
public interface BalanceRow {

    String getAccountRef();

    long getBalanceMinor();
}
