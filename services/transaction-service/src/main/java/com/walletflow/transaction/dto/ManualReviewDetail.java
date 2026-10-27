package com.walletflow.transaction.dto;

import com.walletflow.common.bank.ProviderContext;
import com.walletflow.common.ledger.TransactionEntries;
import com.walletflow.transaction.entity.SagaState;
import com.walletflow.transaction.entity.Transaction;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Một ca cần can thiệp, kèm trạng thái saga và ngữ cảnh từ hai service khác — đủ để người trực phân
 * loại theo runbook mà không phải mở psql (roadmap 2.5, ADR-021).
 *
 * <p>Ba nguồn trả lời ba câu khác nhau, và chỉ có đủ cả ba mới phân loại được một ca: saga nói "tôi
 * đã làm tới bước nào", {@code provider} nói "cổng thanh toán đã lấy tiền của khách chưa",
 * {@code ledger} nói "tiền đã vào sổ chưa".
 *
 * @param saga      null khi saga đã bị xoá hoặc chưa từng có — bất thường, nên để lộ ra thay vì giấu
 * @param ledger    bút toán ledger đã ghi; danh sách RỖNG = ledger chưa ghi gì. {@code null} = không
 *                  hỏi được ledger, và lúc đó tên {@code "ledger"} nằm trong {@link #unavailable}
 * @param provider  các lần thu/hoàn ở bank-adapter; cùng quy ước rỗng-khác-null như trên
 * @param unavailable tên những service không hỏi được lần này. <b>Trường này là cốt lõi, không phải
 *                  trang trí:</b> không có nó thì một ô trống vừa có nghĩa "chưa có gì xảy ra" vừa
 *                  có nghĩa "không biết có gì xảy ra không — mạng hỏng". Hai cách đọc đó dẫn tới hai
 *                  quyết định ngược nhau về tiền của khách, nên sự khác biệt phải hiện ra tường minh
 *                  chứ không để người trực tự suy
 */
public record ManualReviewDetail(ManualReviewItem transaction, SagaView saga,
                                 TransactionEntries ledger, ProviderContext provider,
                                 List<String> unavailable) {

    /**
     * Tên nguồn trong {@link #unavailable}. Là hằng vì hai chuỗi này là hợp đồng với người trực:
     * runbook {@code topup-stuck.md} và {@code saga-stuck.md} dạy đọc đúng chúng, nên đổi ở đây mà
     * quên runbook là để lại một tài liệu chữa cháy nói sai.
     */
    public static final String SOURCE_LEDGER = "ledger";

    public static final String SOURCE_BANK = "bank";

    public record SagaView(String currentStep, String stepStatus, int attemptCount,
                           UUID commandEventId, Instant deadlineAt, Instant updatedAt) {
        static SagaView of(SagaState s) {
            return new SagaView(s.getCurrentStep(), s.getStepStatus(), s.getAttemptCount(),
                    s.getCommandEventId(), s.getDeadlineAt(), s.getUpdatedAt());
        }
    }

    public static ManualReviewDetail of(Transaction t, SagaState saga, Instant now,
                                        TransactionEntries ledger, ProviderContext provider,
                                        List<String> unavailable) {
        return new ManualReviewDetail(ManualReviewItem.of(t, now),
                saga == null ? null : SagaView.of(saga),
                ledger, provider, unavailable);
    }
}
