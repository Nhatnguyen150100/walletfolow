package com.walletflow.wallet.dto;

import java.util.List;

/**
 * Một trang lịch sử. {@code nextCursor = null} nghĩa là hết. Cursor là chuỗi mờ (opaque): client
 * chỉ được gửi lại nguyên văn, không được tự tính — để server đổi cách mã hoá mà không vỡ client.
 */
public record EntryPage(List<EntryView> items, String nextCursor) {
}
