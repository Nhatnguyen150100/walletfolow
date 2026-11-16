package com.walletflow.ledger.services;

import com.walletflow.common.reconciliation.EReconciliationRunStatus;
import com.walletflow.common.reconciliation.ReconciliationScope;
import com.walletflow.ledger.dto.ImbalanceRow;
import com.walletflow.ledger.entity.ReconciliationRun;
import com.walletflow.ledger.enums.EReconciliationBreakType;
import com.walletflow.ledger.repositories.PostingRepository;
import com.walletflow.ledger.repositories.ReconciliationBreakRepository;
import com.walletflow.ledger.repositories.ReconciliationRunRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Đối soát TẦNG 1 — bất biến nội tại của sổ cái (roadmap 3.1, ADR-023, đóng phần ledger của GAP-A3).
 *
 * <p>Trước job này, bất biến vàng {@code SUM(postings) = 0} chỉ được kiểm TRONG TEST
 * ({@code LedgerCommandFlowIT}, {@code TenantIsolationIT}). Khi chạy thật không có gì phát hiện được
 * tiền tự sinh hay tiền bốc hơi — đó là nguyên văn GAP-A3, mức 🔴 "sai tiền không ai biết".
 *
 * <p>Chạy HAI phép kiểm mỗi lượt, cố ý:
 * <ol>
 *   <li>Theo nhóm {@code (tenant, currency)} — bắt sổ của một khách bị lệch.</li>
 *   <li>Toàn hệ thống — bắt ca mà từng nhóm vẫn cân nhưng tổng thì không, tức là có vế trỏ vào tài
 *       khoản đã mất hoặc lệch tenant nên bị {@code JOIN} loại khỏi phép kiểm 1.</li>
 * </ol>
 * Phép kiểm 2 một mình cũng không đủ: một bút toán ghi nợ tenant A ghi có tenant B giữ tổng toàn
 * cục bằng 0 trong khi sổ của cả hai khách đều sai (javadoc {@code PostingRepository.totalOfSystem}).
 *
 * <p><b>Không bao giờ tự sửa.</b> Khác tầng 2 ở wallet-service: ở đây không tồn tại "số đúng" nào để
 * chép về, và sổ cái append-only. Imbalance ⇒ ghi break ⇒ gọi người (alert {@code LedgerImbalance}).
 *
 * <p>Chống chạy trùng nhiều instance bằng chính {@code uk_recon_run_window} — không có ShedLock,
 * không có advisory lock (ADR-023 §6).
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "walletflow.ledger.reconciliation", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class LedgerInvariantReconciler {

    /** Khoá tách lượt theo phép kiểm — xem {@code reconciliation_runs.kind}. */
    public static final String KIND = "LEDGER_INVARIANT";

    /** Dài hơn thì Postgres vẫn nhận (TEXT), nhưng một stack trace trong cột này không ai đọc. */
    private static final int MAX_FAILURE_MESSAGE = 500;

    private final PostingRepository postingRepository;
    private final ReconciliationRunRepository runRepository;
    private final ReconciliationBreakRepository breakRepository;
    private final Duration interval;
    private final AtomicLong openBreaks;
    private final AtomicLong imbalanceMinor;
    private final AtomicLong lastSuccessEpoch;
    private final Counter failures;

    public LedgerInvariantReconciler(
            PostingRepository postingRepository,
            ReconciliationRunRepository runRepository,
            ReconciliationBreakRepository breakRepository,
            MeterRegistry meterRegistry,
            @Value("${walletflow.ledger.reconciliation.interval:PT1H}") Duration interval) {
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalStateException(
                    "walletflow.ledger.reconciliation.interval phải dương, nhận: " + interval);
        }
        this.postingRepository = postingRepository;
        this.runRepository = runRepository;
        this.breakRepository = breakRepository;
        this.interval = interval;
        // Đăng ký lúc khởi động để series tồn tại ở 0 — một alert chỉ bắn được khi metric có mặt
        this.openBreaks = meterRegistry.gauge("walletflow.reconciliation.breaks",
                Tags.of("tier", "ledger"), new AtomicLong());
        this.imbalanceMinor = meterRegistry.gauge("walletflow.ledger.imbalance_minor", new AtomicLong());
        this.lastSuccessEpoch = meterRegistry.gauge("walletflow.reconciliation.last_success_timestamp",
                Tags.of("tier", "ledger"), new AtomicLong());
        // COUNTER: một lượt hỏng KHÔNG cập nhật last_success, nhưng ngưỡng của alert đó tính bằng
        // giờ — counter này gọi người ngay thay vì đợi mốc kia già đi.
        this.failures = meterRegistry.counter("walletflow.reconciliation.failures",
                Tags.of("tier", "ledger"));
    }

    /**
     * Nạp lại mốc thành công gần nhất TỪ DB lúc khởi động.
     *
     * <p>Thiếu bước này thì mỗi lần deploy, gauge về 0 và alert "đối soát không chạy" bắn oan cho
     * tới lượt quét đầu tiên — tức là tối đa một chu kỳ sau mỗi lần deploy. Alert kêu oan thì bị
     * tắt, và lúc đó nó không còn bắt được sự cố thật nữa.
     */
    @PostConstruct
    void restoreLastSuccess() {
        runRepository.findLastSuccess(KIND)
                .map(ReconciliationRun::getWindowStart)
                .ifPresent(windowStart -> lastSuccessEpoch.set(windowStart.getEpochSecond()));
        openBreaks.set(breakRepository.countOpenAcrossTenants());
    }

    /**
     * @return số break ghi được trong lượt này, hoặc {@code -1} nếu instance khác đang giữ lượt
     */
    // CÙNG MỘT property với cửa sổ làm tròn ở constructor, không phải một `interval-ms` riêng: hai
    // con số lệch nhau thì job chạy một nhịp còn cửa sổ chia theo nhịp khác, và sẽ có những cửa sổ
    // không bao giờ được quét trong khi những cửa sổ khác bị bỏ qua vì "đã có instance nhận".
    @Scheduled(fixedDelayString = "${walletflow.ledger.reconciliation.interval:PT1H}",
            initialDelayString = "${walletflow.ledger.reconciliation.initial-delay:PT1M}")
    public int reconcile() {
        Instant windowStart = floorToWindow(Instant.now());
        UUID runId = UUID.randomUUID();

        if (runRepository.claim(runId, KIND, windowStart) == 0) {
            log.debug("Đối soát ledger: cửa sổ {} đã có instance khác nhận — bỏ qua lượt này", windowStart);
            return -1;
        }

        long checked = 0;
        int breaks = 0;
        try {
            // --- Phép kiểm 1: từng nhóm (tenant, currency) ---
            List<ImbalanceRow> groups = postingRepository.sumByTenantAndCurrency();
            checked += groups.size();
            long worstGroup = 0;
            for (ImbalanceRow group : groups) {
                long total = group.getTotalMinor();
                if (total == 0) {
                    continue;
                }
                breaks++;
                worstGroup = Math.max(worstGroup, Math.abs(total));
                breakRepository.record(UUID.randomUUID(), runId, group.getTenantId(),
                        EReconciliationBreakType.LEDGER_IMBALANCE.name(), group.getCurrency(),
                        group.getCurrency(), 0L, total, false,
                        "Tổng posting của nhóm khác 0 — tiền tự sinh hoặc bốc hơi trong sổ của tenant này");
                log.error("ĐỐI SOÁT ĐỎ: tổng posting của tenant {} ({}) = {} minor, phải bằng 0",
                        group.getTenantId(), group.getCurrency(), total);
            }

            // --- Phép kiểm 2: toàn hệ thống ---
            long global = postingRepository.totalOfSystem();
            checked++;
            if (global != 0) {
                breaks++;
                breakRepository.record(UUID.randomUUID(), runId, ReconciliationScope.SYSTEM_TENANT,
                        EReconciliationBreakType.LEDGER_IMBALANCE_GLOBAL.name(), "SYSTEM", null,
                        0L, global, false,
                        "Tổng posting toàn hệ thống khác 0 — có vế trỏ vào tài khoản đã mất hoặc lệch tenant");
                log.error("ĐỐI SOÁT ĐỎ: tổng posting TOÀN HỆ THỐNG = {} minor, phải bằng 0", global);
            }

            runRepository.finish(runId, EReconciliationRunStatus.SUCCEEDED.name(), checked, breaks, null);
            lastSuccessEpoch.set(windowStart.getEpochSecond());
            // Độ lớn lệch lớn nhất quan sát được trong lượt — 0 nghĩa là bất biến vàng đang đúng
            imbalanceMinor.set(Math.max(worstGroup, Math.abs(global)));
            openBreaks.set(breakRepository.countOpenAcrossTenants());

            if (breaks == 0) {
                log.info("Đối soát ledger cửa sổ {}: {} nhóm, bất biến SUM = 0 ĐÚNG", windowStart, checked);
            } else {
                log.error("Đối soát ledger cửa sổ {}: {} chỗ lệch trên {} phép kiểm — CẦN NGƯỜI XEM NGAY",
                        windowStart, breaks, checked);
            }
            return breaks;
        } catch (RuntimeException failure) {
            // Lỗi KỸ THUẬT, không phải lệch tiền (CLAUDE.md §7). Ghi FAILED để last_success già đi và
            // alert bắn — một lượt hỏng im lặng là cách job đối soát chết mà không ai biết.
            runRepository.finish(runId, EReconciliationRunStatus.FAILED.name(), checked, breaks,
                    truncate(failure.toString()));
            failures.increment();
            log.error("Đối soát ledger cửa sổ {} HỎNG sau {} phép kiểm: {}",
                    windowStart, checked, failure.getMessage(), failure);
            return breaks;
        }
    }

    /**
     * Làm tròn xuống theo chu kỳ, lấy epoch làm gốc.
     *
     * <p>Điều này làm cửa sổ TẤT ĐỊNH: hai instance thức dậy lệch nhau vài giây vẫn tính ra cùng một
     * {@code windowStart}, nên đúng một cái giành được lượt. Lấy thẳng {@code Instant.now()} thì mỗi
     * instance có một cửa sổ riêng và cả hai cùng chạy — khoá chống trùng sẽ vô dụng.
     */
    Instant floorToWindow(Instant now) {
        long seconds = interval.toSeconds();
        return Instant.ofEpochSecond(Math.floorDiv(now.getEpochSecond(), seconds) * seconds);
    }

    private static String truncate(String message) {
        if (message == null) {
            return null;
        }
        return message.length() <= MAX_FAILURE_MESSAGE
                ? message
                : message.substring(0, MAX_FAILURE_MESSAGE);
    }
}
