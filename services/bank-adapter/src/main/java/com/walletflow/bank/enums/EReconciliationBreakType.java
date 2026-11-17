package com.walletflow.bank.enums;

/**
 * Các loại lệch mà đối soát bank-adapter ↔ sổ cái phát hiện được (roadmap 3.1 PR2a, ADR-024 §4).
 *
 * <p>Phân loại theo CHIỀU của chênh lệch, không theo "phía nào vắng mặt": hướng tiền mới là thứ quyết
 * định ai bị thiệt và có cần gọi người lúc nửa đêm không. Một lệnh không có bút toán nào và một lệnh
 * ghi thiếu một nửa là cùng một chuyện với người trực — khách đã trả mà ví chưa nhận đủ.
 *
 * <p>Lưu vào DB dạng chuỗi để thêm loại mới (ví dụ khi có sao kê provider ở PR2b) không cần migration.
 */
public enum EReconciliationBreakType {

    /**
     * Provider đã thu (ròng) NHIỀU hơn sổ cái ghi vào — {@code delta < 0}. Khách đã trả, ví chưa nhận đủ.
     *
     * <p>Có nguyên nhân đã biết và chấp nhận được: lần hoàn tay qua portal của provider
     * ({@code MARK_REFUNDED_MANUALLY}) không để lại hàng {@code REFUND} nào ở bank-adapter, nên sổ của
     * adapter vẫn tin là tiền còn đó. Break vẫn là SỰ THẬT — sổ của adapter thiếu một lần hoàn — và
     * người đóng nó ghi lại vì sao (ADR-024 §5). Vì vậy loại này chỉ mở ticket, không page.
     */
    PROVIDER_MONEY_NOT_POSTED,

    /**
     * Sổ cái ghi vào NHIỀU hơn provider thu — {@code delta > 0}. Tiền vào ví mà không ai trả.
     *
     * <p>Đúng MỘT đường đã biết dẫn tới đây trong code: lệnh nạp tới ledger MUỘN, sau khi saga đã hoàn
     * tiền ({@code SagaService.postedWhileRefunding}, ca {@code REFUND_POSTED_CONFLICT} nằm sẵn ở
     * {@code MANUAL_REVIEW}) — adapter thu rồi hoàn (ròng 0), sổ cái vẫn có bút toán nạp. Xử lý bằng
     * {@code REVERSE_LEDGER_ENTRY} (ADR-022). Mọi nguyên nhân khác là lệnh ghi sổ đi vòng qua saga, hoặc
     * một lần thu bị lật trạng thái sau khi đã ghi sổ. Dù nguyên nhân nào, người dùng TIÊU ĐƯỢC số tiền
     * đó ngay — vì vậy alert page.
     */
    POSTED_WITHOUT_PROVIDER_MONEY;

    /** Chiều của chênh lệch quyết định loại. {@code delta = actual (sổ cái) - expected (provider)}. */
    public static EReconciliationBreakType of(long deltaMinor) {
        if (deltaMinor == 0) {
            throw new IllegalArgumentException("delta = 0 không phải chỗ lệch");
        }
        return deltaMinor < 0 ? PROVIDER_MONEY_NOT_POSTED : POSTED_WITHOUT_PROVIDER_MONEY;
    }
}
