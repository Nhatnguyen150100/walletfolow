package com.walletflow.transaction.client;

/**
 * Ba service sở hữu một bảng `reconciliation_breaks` (ADR-023 §1, ADR-024 §1).
 *
 * <p>Là một enum chứ không phải chuỗi tự do vì nó đi vào ĐƯỜNG DẪN của API quản trị
 * ({@code .../breaks/{source}/{id}/close}): một chuỗi tự do ở đó là một tham số do người gọi điều
 * khiển dùng để chọn đích của lời gọi mạng tiếp theo. Enum làm tập đích đóng lại.
 *
 * <p>{@code slug} là thứ hiện trên URL và trong `audit_events`; giữ ngắn và ổn định — đổi nó là đổi
 * hợp đồng API và làm dấu vết cũ khó tra.
 */
public enum EBreakSource {

    /** Bất biến sổ cái: `SUM(postings) = 0` theo tenant và toàn cục (ADR-023 §2). */
    LEDGER("ledger"),

    /** Read model của ví so với sổ cái (ADR-023 §3). */
    WALLET("wallet"),

    /** Tiền ròng qua `EXTERNAL_BANK` so với `provider_transactions` (ADR-024). */
    BANK("bank");

    private final String slug;

    EBreakSource(String slug) {
        this.slug = slug;
    }

    public String slug() {
        return slug;
    }

    /**
     * @throws IllegalArgumentException slug lạ — người gọi dịch thành 400, KHÔNG phải 500: đây là
     *                                  dữ liệu từ URL của người dùng, không phải lỗi lập trình
     */
    public static EBreakSource fromSlug(String slug) {
        for (EBreakSource source : values()) {
            if (source.slug.equals(slug)) {
                return source;
            }
        }
        throw new IllegalArgumentException("Nguồn chỗ lệch không hợp lệ: " + slug);
    }
}
