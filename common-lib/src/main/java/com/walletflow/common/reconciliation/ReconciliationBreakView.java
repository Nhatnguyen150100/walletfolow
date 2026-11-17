package com.walletflow.common.reconciliation;

import com.walletflow.common.money.ECurrency;

import java.time.Instant;
import java.util.UUID;

/**
 * Một chỗ LỆCH đang mở, ở dạng người trực đọc được — hợp đồng của
 * {@code GET /internal/reconciliation/breaks} (roadmap 3.3, ADR-025 §1).
 *
 * <p>Ở common-lib vì **ba** service cùng trả đúng hình dạng này (ledger, wallet, bank-adapter): bảng
 * `reconciliation_breaks` của chúng cố ý giống hệt nhau (ADR-023 §1) để màn hình gom được mà không
 * phải dịch. Đây là record hợp đồng thuần, KHÔNG phải entity — nó không bị {@code @EntityScan} kéo
 * vào service nào.
 *
 * <p>Chỉ mô tả break đang {@code OPEN}, nên không mang {@code status}: đường đọc duy nhất dùng record
 * này là hàng đợi của người trực, và một hàng đợi có cả việc đã xong thì không còn là hàng đợi.
 *
 * @param type       giá trị {@code EReconciliationBreakType} của service sở hữu — tập giá trị KHÁC
 *                   nhau giữa ba service, nên để chuỗi thay vì enum dùng chung
 * @param subject    đối tượng lệch ở dạng người đọc được: mã loại tiền (ledger), id ví (wallet),
 *                   id lệnh tiền (bank-adapter)
 * @param currency   {@code null} với break toàn hệ thống ({@code LEDGER_IMBALANCE_GLOBAL})
 * @param expectedMinor con số ĐÚNG theo nguồn sự thật
 * @param actualMinor   con số đang có
 * @param autoRepaired  máy đã sửa được HẬU QUẢ (tầng 2 dựng lại read model). Break vẫn mở — rebuild
 *                      chép lại con số đúng, nó không sửa cái bug đã làm lệch (ADR-023 §5)
 * @param timesSeen  đã thấy lại bao nhiêu lượt. Lớn = lệch LẶP LẠI, tức là còn nguyên nhân chưa sửa
 * @param firstSeenAt lệch từ bao giờ — câu hỏi đầu tiên khi điều tra. KHÁC {@code detectedAt}
 */
public record ReconciliationBreakView(
    UUID id,
    UUID tenantId,
    String type,
    String subject,
    ECurrency currency,
    long expectedMinor,
    long actualMinor,
    long deltaMinor,
    boolean autoRepaired,
    int timesSeen,
    String detail,
    Instant firstSeenAt,
    Instant detectedAt
) {
}
