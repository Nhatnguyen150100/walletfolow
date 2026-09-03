package com.walletflow.ledger.dto;

import java.util.List;

/**
 * Mô tả một bút toán ĐÃ NẰM TRONG SỔ, dùng để dựng {@link LedgerPostedEvent}.
 *
 * <p>Đọc lại từ DB thay vì dùng lại {@code command.legs()} là có chủ đích: event phải nói về những
 * gì THỰC SỰ đã được ghi vào sổ. Điều này quan trọng ở đường phát lại (lệnh trùng): lúc đó bút toán
 * đã được ghi từ một lần chạy trước, và sổ cái — không phải lệnh vừa nhận — mới là nguồn sự thật.
 *
 * @param primaryWalletRef ví người dùng "chủ động" của bút toán, ưu tiên ví bị ghi nợ. Dùng làm
 *                         partition key để mọi message của cùng một ví vào cùng partition (plan §11,
 *                         bài toán P10). {@code null} nếu bút toán chỉ gồm tài khoản hệ thống.
 */
public record JournalView(
        List<Leg> legs,
        String primaryWalletRef
) {
}
