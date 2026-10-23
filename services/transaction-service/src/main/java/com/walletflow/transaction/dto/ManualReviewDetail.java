package com.walletflow.transaction.dto;

import com.walletflow.transaction.entity.SagaState;
import com.walletflow.transaction.entity.Transaction;

import java.time.Instant;
import java.util.UUID;

/**
 * Một ca cần can thiệp, kèm trạng thái saga — đủ để người trực phân loại theo runbook mà không phải
 * mở psql (roadmap 2.5).
 *
 * <p>Những thứ CHƯA có ở đây và vì sao: {@code raw_response} của provider nằm ở railsdb và bút toán
 * nằm ở ledgerdb, cả hai đều không có API đọc theo {@code transactionId} (DB-per-service — ADR-001).
 * Thêm endpoint {@code /internal} cho hai service đó là việc của 2.5 PR3; tới lúc đó runbook vẫn là
 * đường lấy hai thứ này.
 *
 * @param saga null khi saga đã bị xoá hoặc chưa từng có — bất thường, nên để lộ ra thay vì giấu
 */
public record ManualReviewDetail(ManualReviewItem transaction, SagaView saga) {

    public record SagaView(String currentStep, String stepStatus, int attemptCount,
                           UUID commandEventId, Instant deadlineAt, Instant updatedAt) {
        static SagaView of(SagaState s) {
            return new SagaView(s.getCurrentStep(), s.getStepStatus(), s.getAttemptCount(),
                    s.getCommandEventId(), s.getDeadlineAt(), s.getUpdatedAt());
        }
    }

    public static ManualReviewDetail of(Transaction t, SagaState saga, Instant now) {
        return new ManualReviewDetail(ManualReviewItem.of(t, now),
                saga == null ? null : SagaView.of(saga));
    }
}
