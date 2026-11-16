package com.walletflow.common.ledger;

/**
 * Số dư THẬT của một tài khoản sổ cái — hợp đồng của API nội bộ
 * {@code POST /internal/accounts/balances} (ledger → wallet-service), dùng cho đối soát tầng 2
 * (roadmap 3.1, ADR-023).
 *
 * <p>Vì sao cần bên cạnh {@link AccountStatement}: sao kê mang MỌI dòng của một tài khoản. Đối soát
 * hỏi đúng một câu — "số dư bao nhiêu" — cho hàng nghìn ví mỗi đêm. Tải toàn bộ lịch sử để rồi cộng
 * lại là trả giá bằng băng thông và bộ nhớ cho thứ Postgres cộng sẵn được.
 *
 * <p>Số dư ở đây là giá trị SUY RA ({@code SUM(postings)}), không phải một cột được cập nhật — cùng
 * định nghĩa với {@code PostingRepository.balanceOf}. Ledger không có "số dư bị sai".
 *
 * @param accountRef mã ngoài của tài khoản = id ví bên wallet-service
 */
public record AccountBalance(
    String accountRef,
    long balanceMinor
) {
}
