package com.walletflow.common.reconciliation;

import java.util.UUID;

/**
 * Hằng dùng chung của đối soát (roadmap 3.1, ADR-023).
 */
public final class ReconciliationScope {

    /**
     * Tenant giả cho các break KHÔNG thuộc về khách nào — hiện chỉ có
     * {@code LEDGER_IMBALANCE_GLOBAL}: "tổng toàn hệ thống khác 0" là một phát biểu về cả hệ thống,
     * không về sổ của một khách.
     *
     * <p>Vì sao một UUID sentinel chứ không để {@code tenant_id} nhận NULL: khoá chống nhân bản
     * {@code uk_recon_break_open (tenant_id, type, subject)} dựa trên so sánh bằng, mà trong index
     * UNIQUE của Postgres hai NULL là KHÁC nhau. Để NULL thì đúng loại break cần dedup nhất — cái
     * lặp lại mỗi giờ cho tới khi có người sửa — lại là cái sinh ra một hàng mới mỗi lượt.
     *
     * <p>Không va chạm với tenant thật: tenant thật do {@code gen_random_uuid()} sinh (phiên bản 4),
     * không bao giờ ra toàn số 0.
     */
    public static final UUID SYSTEM_TENANT = new UUID(0L, 0L);

    private ReconciliationScope() {
    }
}
