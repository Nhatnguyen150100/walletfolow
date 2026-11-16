package com.walletflow.common.ledger;

import com.walletflow.common.money.ECurrency;

import java.util.List;

/**
 * Hỏi số dư của NHIỀU tài khoản cùng loại tiền trong một lượt — thân của
 * {@code POST /internal/accounts/balances} (ADR-023 §3).
 *
 * <p>Là POST tuy chỉ ĐỌC và không đổi gì ở ledger: danh sách ref dài hàng trăm phần tử không nhét
 * vừa query string một cách an toàn (giới hạn độ dài URL khác nhau tuỳ proxy, và vượt giới hạn thì
 * hỏng ở tầng mạng chứ không báo lỗi tử tế). Đường ghi sổ vẫn chỉ có Kafka command — xem ghi chú ở
 * {@code InternalAccountController}.
 *
 * <p>Không có annotation Jakarta Validation ở đây: common-lib không có nó trên classpath, và các
 * record hợp đồng khác trong gói này ({@link AccountStatement}) cũng vậy. Ràng buộc được cưỡng chế
 * ở {@code AccountService.balancesOf} với {@code errorCode} ổn định.
 *
 * @param currency mọi ref trong một lượt phải cùng loại tiền: tài khoản được định danh bởi
 *                 {@code (ref, currency)}, nên một danh sách trộn loại tiền sẽ mơ hồ
 * @param refs     trần {@link #MAX_REFS} là hàng rào bộ nhớ ở phía ledger, không phải con số nghiệp
 *                 vụ; người gọi tự chia lô
 */
public record AccountBalancesRequest(
    ECurrency currency,
    List<String> refs
) {

  /** Trần số ref mỗi lượt — hằng ở đây để người gọi chia lô theo đúng con số ledger cưỡng chế. */
  public static final int MAX_REFS = 500;
}
