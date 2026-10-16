package com.walletflow.common.ledger;

import com.walletflow.common.event.EAccountStatus;
import com.walletflow.common.money.ECurrency;

import java.util.List;
import java.util.UUID;

/**
 * Sao kê đầy đủ của MỘT tài khoản sổ cái — hợp đồng của API nội bộ
 * {@code GET /internal/accounts/{ref}} (ledger → wallet-service).
 *
 * <p>Dùng để dựng lại read model từ nguồn sự thật (roadmap 1.1). Số dư cố tình KHÔNG có trường
 * riêng: nó là {@link #balanceMinor()} = tổng các dòng. Một trường số dư tách rời có thể lệch với
 * chính danh sách dòng nó đi kèm (hai truy vấn, hai thời điểm), và người rebuild sẽ không biết tin
 * cái nào.
 *
 * @param lines MỌI bút toán chạm tài khoản, mỗi bút toán một dòng, theo thứ tự ghi sổ
 */
public record AccountStatement(
    UUID accountId,
    String accountRef,
    ECurrency currency,
    EAccountStatus status,
    int statusVersion,
    List<StatementLine> lines
) {

  public long balanceMinor() {
    long total = 0;
    for (StatementLine line : lines) {
      total = Math.addExact(total, line.amountMinor());
    }
    return total;
  }
}
