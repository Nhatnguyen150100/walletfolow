package com.walletflow.wallet.services;

import com.walletflow.common.ledger.AccountBalancesRequest;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.reconciliation.EReconciliationRunStatus;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.common.wallet.EWalletStatus;
import com.walletflow.wallet.client.LedgerClient;
import com.walletflow.wallet.dto.WalletCheckRow;
import com.walletflow.wallet.entity.ReconciliationRun;
import com.walletflow.wallet.enums.EReconciliationBreakType;
import com.walletflow.wallet.repositories.ReconciliationBreakRepository;
import com.walletflow.wallet.repositories.ReconciliationRunRepository;
import com.walletflow.wallet.repositories.WalletRepository;
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Đối soát TẦNG 2 — read model so với sổ cái (roadmap 3.1, ADR-023, đóng phần wallet của GAP-A3).
 *
 * <p>Hai phép so mỗi ví, cố ý cả hai chứ không một:
 * <ol>
 *   <li><b>2a</b> {@code balance_minor} vs {@code SUM(wallet_entries)} — ví tự mâu thuẫn với chính
 *       nó. Một câu SQL, không qua mạng. Tính chất này được viết ra từ {@code V1__init_wallet.sql}.</li>
 *   <li><b>2b</b> {@code balance_minor} vs {@code SUM(postings)} bên ledger — ví lệch khỏi NGUỒN SỰ
 *       THẬT. Đây mới là phép so roadmap đòi.</li>
 * </ol>
 * 2a KHÔNG dùng làm bộ lọc cho 2b: một projector bỏ sót CẢ hàng lịch sử lẫn số dư cho ra một ví tự
 * khớp hoàn hảo mà vẫn thiếu tiền. Lọc bằng 2a sẽ giấu đúng ca đó.
 *
 * <p><b>Hai hàng rào chống báo động giả</b> (ADR-023 §4), vì read model là eventually consistent:
 * chỉ soi ví đã yên quá {@code settle-window}, và đọc lại lần thứ hai trước khi ghi break.
 *
 * <p><b>Lệch thì TỰ DỰNG LẠI, nhưng break vẫn mở.</b> Ledger luôn thắng nên để số sai nằm đó tới
 * sáng là chọn cái tệ hơn. Nhưng rebuild chỉ chép lại con số đúng — nó không sửa cái bug đã làm
 * lệch, nên break chờ người xem (ADR-023 §5).
 *
 * <p>Chạy XUYÊN tenant; mỗi ví được xử lý trong {@code TenantContext} của chính nó.
 *
 * <p><b>Về ranh giới transaction — đọc kỹ.</b> Mọi lời gọi ledger của chính reconciler nằm NGOÀI
 * transaction DB (CLAUDE.md). Nhưng đường sửa thì không: {@code WalletRebuildService.rebuild} là
 * {@code @Transactional} và KHOÁ ví bằng {@code FOR UPDATE} rồi mới gọi ledger — javadoc của nó
 * nhận ngoại lệ đó là chấp nhận được "cho một thao tác quản trị hiếm, với điều kiện client có
 * timeout ngắn". Job này biến nó thành việc chạy tự động hàng loạt, nên tiền đề "hiếm" không còn
 * đúng và thứ duy nhất còn giữ ngoại lệ đó an toàn là {@code read-timeout} 5 giây của
 * {@code HttpLedgerClient}. Ghi lại như nợ: xem ADR-023 §5.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "walletflow.wallet.reconciliation", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class WalletReconciler {

    /** Khoá tách lượt theo phép kiểm — xem {@code reconciliation_runs.kind}. */
    public static final String KIND = "WALLET_PROJECTION";

    /** Con trỏ phân trang bắt đầu từ UUID nhỏ nhất: {@code id > đây} là mọi ví. */
    private static final UUID BEFORE_FIRST = new UUID(0L, 0L);

    private static final int MAX_FAILURE_MESSAGE = 500;

    private final WalletRepository walletRepository;
    private final ReconciliationRunRepository runRepository;
    private final ReconciliationBreakRepository breakRepository;
    private final WalletRebuildService rebuildService;
    private final LedgerClient ledgerClient;
    private final Duration interval;
    private final Duration settleWindow;
    private final int batchSize;
    private final int maxBreaksPerRun;
    private final AtomicLong openBreaks;
    private final AtomicLong lastSuccessEpoch;
    private final Counter failures;

    public WalletReconciler(
            WalletRepository walletRepository,
            ReconciliationRunRepository runRepository,
            ReconciliationBreakRepository breakRepository,
            WalletRebuildService rebuildService,
            LedgerClient ledgerClient,
            MeterRegistry meterRegistry,
            @Value("${walletflow.wallet.reconciliation.interval:PT24H}") Duration interval,
            @Value("${walletflow.wallet.reconciliation.settle-window:PT1M}") Duration settleWindow,
            @Value("${walletflow.wallet.reconciliation.batch-size:200}") int batchSize,
            @Value("${walletflow.wallet.reconciliation.max-breaks-per-run:500}") int maxBreaksPerRun) {
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalStateException(
                    "walletflow.wallet.reconciliation.interval phải dương, nhận: " + interval);
        }
        if (settleWindow.isNegative()) {
            throw new IllegalStateException(
                    "walletflow.wallet.reconciliation.settle-window không được âm, nhận: " + settleWindow);
        }
        // Trần dưới 1: lô 0 ví nghĩa là vòng quét không bao giờ tiến. Trần trên là giới hạn ledger
        // cưỡng chế cho một lượt hỏi số dư — vượt qua thì MỌI lô bị từ chối, job chết mà build vẫn xanh.
        if (batchSize < 1 || batchSize > AccountBalancesRequest.MAX_REFS) {
            throw new IllegalStateException("walletflow.wallet.reconciliation.batch-size (" + batchSize
                    + ") phải nằm trong [1, " + AccountBalancesRequest.MAX_REFS + "]");
        }
        this.walletRepository = walletRepository;
        this.runRepository = runRepository;
        this.breakRepository = breakRepository;
        this.rebuildService = rebuildService;
        this.ledgerClient = ledgerClient;
        this.interval = interval;
        this.settleWindow = settleWindow;
        if (maxBreaksPerRun < 1) {
            throw new IllegalStateException("walletflow.wallet.reconciliation.max-breaks-per-run ("
                    + maxBreaksPerRun + ") phải >= 1");
        }
        this.batchSize = batchSize;
        this.maxBreaksPerRun = maxBreaksPerRun;
        // Đăng ký lúc khởi động để series tồn tại ở 0 — một alert chỉ bắn được khi metric có mặt
        this.openBreaks = meterRegistry.gauge("walletflow.reconciliation.breaks",
                Tags.of("tier", "wallet"), new AtomicLong());
        this.lastSuccessEpoch = meterRegistry.gauge("walletflow.reconciliation.last_success_timestamp",
                Tags.of("tier", "wallet"), new AtomicLong());
        // COUNTER: "vừa có một lượt hỏng" là một sự kiện. Một lượt hỏng KHÔNG cập nhật
        // last_success, nhưng ngưỡng của alert đó tính bằng ngày — counter này là thứ gọi người
        // ngay trong đêm thay vì để tới đêm thứ hai.
        this.failures = meterRegistry.counter("walletflow.reconciliation.failures",
                Tags.of("tier", "wallet"));
    }

    /**
     * Nạp lại mốc thành công gần nhất TỪ DB lúc khởi động.
     *
     * <p>Thiếu bước này thì mỗi lần deploy, gauge về 0 và alert "đối soát không chạy" bắn oan cho
     * tới lượt quét đầu tiên. Alert kêu oan thì bị tắt, và lúc đó nó không còn bắt được sự cố thật.
     */
    @PostConstruct
    void restoreLastSuccess() {
        runRepository.findLastSuccess(KIND)
                .map(ReconciliationRun::getWindowStart)
                .ifPresent(windowStart -> lastSuccessEpoch.set(windowStart.getEpochSecond()));
        openBreaks.set(breakRepository.countOpenAcrossTenants());
    }

    /** @return số break ghi được trong lượt này, hoặc {@code -1} nếu instance khác đang giữ lượt */
    // CÙNG MỘT property với cửa sổ làm tròn ở constructor — xem LedgerInvariantReconciler.
    @Scheduled(fixedDelayString = "${walletflow.wallet.reconciliation.interval:PT24H}",
            initialDelayString = "${walletflow.wallet.reconciliation.initial-delay:PT2M}")
    public int reconcile() {
        Instant windowStart = floorToWindow(Instant.now());
        UUID runId = UUID.randomUUID();

        if (runRepository.claim(runId, KIND, windowStart) == 0) {
            log.debug("Đối soát ví: cửa sổ {} đã có instance khác nhận — bỏ qua lượt này", windowStart);
            return -1;
        }

        long checked = 0;
        int breaks = 0;
        int failedBatches = 0;
        String firstFailure = null;
        boolean capped = false;
        try {
            Instant settledBefore = Instant.now().minus(settleWindow);
            UUID cursor = BEFORE_FIRST;
            while (true) {
                List<WalletCheckRow> batch =
                        walletRepository.scanSettledAcrossTenants(settledBefore, cursor, batchSize);
                if (batch.isEmpty()) {
                    break;
                }
                cursor = batch.get(batch.size() - 1).getWalletId();
                checked += batch.size();

                // Bắt lỗi THEO LÔ, không bọc cả vòng quét: ledger chập chờn 5 giây ở lô thứ ba thì
                // chỉ mất lô đó, không mất nốt mọi ví phía sau. Cửa sổ đã bị chiếm nên một lượt bỏ
                // dở là 24 giờ không ai kiểm — quá đắt cho một blip mạng.
                try {
                    breaks += inspect(runId, batch);
                } catch (RuntimeException batchFailure) {
                    failedBatches++;
                    if (firstFailure == null) {
                        firstFailure = batchFailure.toString();
                    }
                    log.error("Đối soát ví: lô kết thúc ở {} hỏng, bỏ qua lô này: {}",
                            cursor, batchFailure.getMessage(), batchFailure);
                }

                if (breaks >= maxBreaksPerRun) {
                    // Lệch DIỆN RỘNG là ca phải có người, không phải ca để máy tự sửa hàng chục
                    // nghìn lần: mỗi break kéo theo lời gọi ledger + một transaction ghi, và không
                    // có trần thì chính lượt đối soát trở thành sự cố.
                    capped = true;
                    log.error("Đối soát ví cửa sổ {}: chạm trần {} chỗ lệch — DỪNG quét, cần người"
                            + " xem ngay (lệch diện rộng, không phải vài ví lẻ)", windowStart, maxBreaksPerRun);
                    break;
                }
            }

            boolean clean = failedBatches == 0 && !capped;
            String note = clean ? null : truncate(summary(failedBatches, capped, firstFailure));
            runRepository.finish(runId,
                    clean ? EReconciliationRunStatus.SUCCEEDED.name() : EReconciliationRunStatus.FAILED.name(),
                    checked, breaks, note);
            if (clean) {
                lastSuccessEpoch.set(windowStart.getEpochSecond());
            } else {
                failures.increment();
            }
            openBreaks.set(breakRepository.countOpenAcrossTenants());

            if (breaks == 0 && clean) {
                log.info("Đối soát ví cửa sổ {}: {} ví, read model khớp sổ cái", windowStart, checked);
            } else {
                log.error("Đối soát ví cửa sổ {}: {} chỗ lệch trên {} ví đã soi, {} lô hỏng — cần người xem",
                        windowStart, breaks, checked, failedBatches);
            }
            return breaks;
        } catch (RuntimeException failure) {
            // Lỗi KỸ THUẬT ngoài phạm vi một lô (quét DB hỏng) — CLAUDE.md §7: không phải lệch tiền.
            runRepository.finish(runId, EReconciliationRunStatus.FAILED.name(), checked, breaks,
                    truncate(failure.toString()));
            failures.increment();
            log.error("Đối soát ví cửa sổ {} HỎNG sau {} ví: {}",
                    windowStart, checked, failure.getMessage(), failure);
            return breaks;
        }
    }

    private static String summary(int failedBatches, boolean capped, String firstFailure) {
        StringBuilder note = new StringBuilder();
        if (capped) {
            note.append("Chạm trần số chỗ lệch mỗi lượt — quét chưa hết. ");
        }
        if (failedBatches > 0) {
            note.append(failedBatches).append(" lô hỏng; lỗi đầu tiên: ").append(firstFailure);
        }
        return note.toString();
    }

    /** @return số break ghi được cho lô này */
    private int inspect(UUID runId, List<WalletCheckRow> batch) {
        Map<String, Long> ledgerBalances = fetchLedgerBalances(batch);
        List<Candidate> candidates = new ArrayList<>();

        for (WalletCheckRow row : batch) {
            if (row.getBalanceMinor() != row.getEntriesSumMinor()) {
                candidates.add(new Candidate(row, EReconciliationBreakType.PROJECTION_SELF_MISMATCH));
            }
            Long inLedger = ledgerBalances.get(row.getWalletId().toString());
            if (inLedger == null) {
                // Ví chưa ACTIVE thì chưa có tài khoản là BÌNH THƯỜNG — PendingWalletReconciler lo
                // việc đó. Ví đã ACTIVE mà giữ tiền không có trong sổ cái thì không bình thường.
                if (EWalletStatus.ACTIVE.name().equals(row.getStatus()) && row.getBalanceMinor() != 0) {
                    candidates.add(new Candidate(row, EReconciliationBreakType.LEDGER_ACCOUNT_MISSING));
                }
            } else if (inLedger != row.getBalanceMinor()) {
                candidates.add(new Candidate(row, EReconciliationBreakType.LEDGER_MISMATCH));
            }
        }

        int recorded = 0;
        for (Candidate candidate : candidates) {
            if (confirmAndRecord(runId, candidate)) {
                recorded++;
            }
        }
        return recorded;
    }

    /**
     * Hỏi ledger số dư của cả lô, gom theo {@code (tenant, currency)}.
     *
     * <p>Một lô quét theo id có thể trải nhiều tenant và nhiều loại tiền, trong khi ledger định danh
     * tài khoản bằng {@code (ref, currency)} và lọc tenant ở header. Nên phải gom rồi hỏi từng nhóm
     * — vẫn là vài lời gọi cho hàng trăm ví, thay vì một lời gọi mỗi ví.
     */
    private Map<String, Long> fetchLedgerBalances(List<WalletCheckRow> batch) {
        Map<TenantCurrency, List<String>> grouped = new LinkedHashMap<>();
        for (WalletCheckRow row : batch) {
            grouped.computeIfAbsent(new TenantCurrency(row.getTenantId(), row.getCurrency()),
                    key -> new ArrayList<>()).add(row.getWalletId().toString());
        }

        Map<String, Long> balances = new LinkedHashMap<>();
        grouped.forEach((key, refs) -> TenantContext.runWith(key.tenantId(),
                () -> balances.putAll(ledgerClient.balancesOf(ECurrency.valueOf(key.currency()), refs))));
        return balances;
    }

    /**
     * Đọc lại lần thứ hai rồi mới kết luận (ADR-023 §4).
     *
     * <p>Loại break được quyết LẠI ở đây chứ không lấy từ lần đọc đầu: giữa hai lần đọc, một ví
     * "ledger chưa có tài khoản" có thể đã có tài khoản. Kết luận phải mô tả cái ta thấy lúc kết
     * luận, không phải cái ta thấy lúc nghi ngờ.
     *
     * @return true nếu thực sự ghi một break
     */
    private boolean confirmAndRecord(UUID runId, Candidate candidate) {
        UUID walletId = candidate.row().getWalletId();
        UUID tenantId = candidate.row().getTenantId();
        ECurrency currency = ECurrency.valueOf(candidate.row().getCurrency());

        Optional<WalletCheckRow> found = walletRepository.recheck(walletId);
        if (found.isEmpty()) {
            // Không xảy ra trong thực tế (ví không bao giờ bị xoá), nhưng im lặng bỏ qua thì tệ hơn:
            // nó nghĩa là chính giả định đó đã sai.
            log.error("Đối soát ví: ví {} biến mất giữa hai lần đọc — dữ liệu không nhất quán", walletId);
            return false;
        }
        WalletCheckRow fresh = found.get();

        if (candidate.type() == EReconciliationBreakType.PROJECTION_SELF_MISMATCH) {
            long expected = fresh.getEntriesSumMinor();
            if (expected == fresh.getBalanceMinor()) {
                return false;
            }
            return record(runId, fresh, EReconciliationBreakType.PROJECTION_SELF_MISMATCH,
                    expected, rebuild(tenantId, walletId));
        }

        // Hai loại còn lại đều hỏi ledger — hỏi ĐÚNG MỘT lần rồi phân loại theo câu trả lời
        Optional<Long> inLedger = TenantContext.callWith(tenantId, () ->
                Optional.ofNullable(ledgerClient.balancesOf(currency, List.of(walletId.toString()))
                        .get(walletId.toString())));

        if (inLedger.isEmpty()) {
            if (!EWalletStatus.ACTIVE.name().equals(fresh.getStatus()) || fresh.getBalanceMinor() == 0) {
                return false;
            }
            // Không rebuild được: không có sổ nào để chép về. Đây là ca phải gọi người.
            return record(runId, fresh, EReconciliationBreakType.LEDGER_ACCOUNT_MISSING, 0L, false);
        }

        long expected = inLedger.get();
        if (expected == fresh.getBalanceMinor()) {
            log.debug("Đối soát ví {}: lệch biến mất ở lần đọc thứ hai — event đang bay, không phải sự cố",
                    walletId);
            return false;
        }
        return record(runId, fresh, EReconciliationBreakType.LEDGER_MISMATCH,
                expected, rebuild(tenantId, walletId));
    }

    /**
     * Dựng lại read model từ sổ cái.
     *
     * @return <b>true chỉ khi thật sự có gì đó đổi.</b> Không phải chi tiết thừa:
     *         {@code WalletRebuildService} THOÁT SỚM không đụng gì khi ledger chưa có tài khoản, và
     *         {@code auto_repaired} là thứ runbook dùng để bảo người trực "số dư đã đúng lại rồi,
     *         việc còn lại chỉ là tìm nguyên nhân". Trả true mù quáng ở đây nghĩa là nói với người
     *         trực rằng tiền đã ổn trong khi nó chưa — đúng loại lỗi khiến một ví giữ tiền không có
     *         trong sổ cái bị cho qua.
     */
    private boolean rebuild(UUID tenantId, UUID walletId) {
        WalletRebuildService.RebuildResult result =
                TenantContext.callWith(tenantId, () -> rebuildService.rebuild(walletId));
        return result.balanceAfter() != result.balanceBefore()
                || result.entriesAdded() > 0
                || result.entriesRemoved() > 0;
    }

    private boolean record(UUID runId, WalletCheckRow fresh, EReconciliationBreakType type,
                           long expected, boolean repaired) {
        UUID walletId = fresh.getWalletId();
        breakRepository.record(UUID.randomUUID(), runId, fresh.getTenantId(), type.name(),
                walletId.toString(), fresh.getCurrency(), expected, fresh.getBalanceMinor(),
                repaired, detailOf(type, repaired));
        log.error("ĐỐI SOÁT: ví {} (tenant {}) lệch kiểu {} — read model {} vs nguồn sự thật {}{}",
                walletId, fresh.getTenantId(), type, fresh.getBalanceMinor(), expected,
                repaired ? ", ĐÃ dựng lại read model từ sổ cái" : "");
        return true;
    }

    private static String detailOf(EReconciliationBreakType type, boolean repaired) {
        String cause = switch (type) {
            case PROJECTION_SELF_MISMATCH -> "balance_minor khác tổng wallet_entries của chính ví";
            case LEDGER_MISMATCH -> "balance_minor khác SUM(postings) bên ledger";
            case LEDGER_ACCOUNT_MISSING ->
                    "ví ACTIVE có số dư nhưng ledger không có tài khoản — tiền không nằm trong sổ cái";
        };
        return repaired
                ? cause + "; read model đã được dựng lại từ sổ cái, break để lại cho người tìm nguyên nhân"
                : cause;
    }

    /**
     * Làm tròn xuống theo chu kỳ — xem {@code LedgerInvariantReconciler.floorToWindow}: cửa sổ phải
     * TẤT ĐỊNH thì khoá chống chạy trùng mới có tác dụng.
     */
    Instant floorToWindow(Instant now) {
        long seconds = interval.toSeconds();
        return Instant.ofEpochSecond(Math.floorDiv(now.getEpochSecond(), seconds) * seconds);
    }

    private static String truncate(String message) {
        if (message == null) {
            return null;
        }
        return message.length() <= MAX_FAILURE_MESSAGE ? message : message.substring(0, MAX_FAILURE_MESSAGE);
    }

    private record TenantCurrency(UUID tenantId, String currency) {
    }

    /** Một chỗ NGHI lệch, chưa xác nhận. Chỉ thành break sau lần đọc thứ hai. */
    private record Candidate(WalletCheckRow row, EReconciliationBreakType type) {
    }
}
