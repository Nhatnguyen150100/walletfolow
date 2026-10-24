package com.walletflow.transaction.services;

import com.walletflow.common.exception.ResourceNotFoundException;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.transaction.dto.ManualReviewDetail;
import com.walletflow.transaction.dto.ManualReviewItem;
import com.walletflow.transaction.entity.Transaction;
import com.walletflow.transaction.repositories.SagaRepository;
import com.walletflow.transaction.repositories.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Hàng đợi giao dịch cần người can thiệp (roadmap 2.5 PR1, ADR-021). CHỈ ĐỌC — thao tác xử lý và
 * 4-eyes nằm ở {@link ResolutionService}, nơi có ranh giới transaction riêng vì nó đụng tiền.
 *
 * <p>Mọi thao tác ở đây đều để lại dấu vết, kể cả thao tác đọc và kể cả thao tác BỊ TỪ CHỐI: người
 * vận hành xem dữ liệu tài chính của một khách hàng là việc cần giải trình được, và một lần bị từ
 * chối còn đáng ghi hơn — đó là hình dạng của việc dò id hoặc của một token bị đánh cắp.
 *
 * <p><b>Cố ý KHÔNG có {@code @Transactional} ở đây.</b> Mỗi truy vấn tự chạy trong transaction của
 * nó rồi trả connection về pool ngay. Bọc cả method trong một transaction đọc thì lời gọi audit
 * ({@code REQUIRES_NEW}) phải xin connection THỨ HAI trong khi cái thứ nhất còn bị giữ — với pool
 * mặc định 10, dùng chung với job quét saga và các Kafka listener, chỉ cần một nhúm người trực cùng
 * mở hàng đợi là mỗi luồng giữ một connection và chờ cái thứ hai. Kết cục: request treo 30 giây rồi
 * vẫn trả 200, nhưng dấu vết bị nuốt — đúng lúc đang có sự cố diện rộng là lúc nó xảy ra.
 *
 * <p>Giá phải trả: {@code detail} đọc hai hàng ở hai transaction, nên về lý thuyết saga có thể đổi
 * giữa hai lần đọc. Đây là ảnh chụp cho người xem, không phải căn cứ để quyết chuyện tiền (PR2 sẽ
 * đọc lại dưới khoá), nên không đổi gì về tính đúng.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AdminReviewService {

    /** Trần một trang. Hàng đợi khoẻ mạnh thì ngắn; dài tới mức này là đã có sự cố diện rộng. */
    static final int MAX_PAGE_SIZE = 200;

    private final TransactionRepository transactionRepository;
    private final SagaRepository sagaRepository;
    private final AuditService auditService;

    public List<ManualReviewItem> queue(Operator actor, int limit) {
        int size = Math.clamp(limit, 1, MAX_PAGE_SIZE);
        Instant now = Instant.now();
        List<ManualReviewItem> items = transactionRepository
                .findNeedingReview(TenantContext.require(), size)
                .stream()
                .map(t -> ManualReviewItem.of(t, now))
                .toList();
        auditService.record(actor, "LIST_MANUAL_REVIEW", null, Map.of("returned", items.size()));
        return items;
    }

    /**
     * Một ca, kèm trạng thái saga.
     *
     * <p>Không giới hạn theo {@code initiated_by} như đường của khách hàng: người vận hành phải xem
     * được ca của bất kỳ ai TRONG TENANT của mình — đó chính là việc của họ. Ranh giới còn lại là
     * tenant, và nó vẫn được giữ.
     *
     * <p>Trả 404 cho cả "không tồn tại" lẫn "của tenant khác", như mọi đường khác — và ghi dấu vết
     * cho cả hai ca đó trước khi ném, vì một chuỗi 404 liên tiếp chính là hình dạng của việc dò id.
     */
    public ManualReviewDetail detail(Operator actor, UUID transactionId) {
        UUID tenantId = TenantContext.require();
        Transaction txn = transactionRepository.findByTenantIdAndId(tenantId, transactionId)
                .orElseThrow(() -> {
                    auditService.record(actor, "VIEW_TRANSACTION_DENIED", transactionId.toString(),
                            Map.of("reason", "NOT_FOUND_IN_TENANT"));
                    return new ResourceNotFoundException("Không tìm thấy giao dịch");
                });
        auditService.record(actor, "VIEW_TRANSACTION", txn.getId().toString(),
                Map.of("status", txn.getStatus().name()));
        return ManualReviewDetail.of(txn,
                sagaRepository.findByTenantIdAndTransactionId(tenantId, transactionId).orElse(null),
                Instant.now());
    }
}
