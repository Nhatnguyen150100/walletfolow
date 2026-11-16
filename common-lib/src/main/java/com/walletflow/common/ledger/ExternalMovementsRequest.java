package com.walletflow.common.ledger;

import java.util.List;
import java.util.UUID;

/**
 * Hỏi tiền ròng đi qua {@code EXTERNAL_BANK} của NHIỀU lệnh tiền trong một lượt — thân của
 * {@code POST /internal/entries/external-movements}, dùng cho đối soát bank-adapter ↔ sổ cái
 * (roadmap 3.1 PR2a, ADR-024).
 *
 * <p>POST tuy chỉ ĐỌC, cùng lý lẽ với {@link AccountBalancesRequest}: vài trăm UUID không nhét vừa
 * query string một cách an toàn. Ràng buộc cưỡng chế ở ledger với {@code errorCode} ổn định.
 *
 * @param transactionIds trần {@link #MAX_IDS} là hàng rào bộ nhớ phía ledger; người gọi tự chia lô
 */
public record ExternalMovementsRequest(
    List<UUID> transactionIds
) {

  /** Trần số lệnh mỗi lượt — cùng con số với {@link AccountBalancesRequest#MAX_REFS}. */
  public static final int MAX_IDS = 500;
}
