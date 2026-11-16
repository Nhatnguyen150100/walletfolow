package com.walletflow.bank.services;

import com.walletflow.bank.client.LedgerClient;
import com.walletflow.bank.dto.ChargeCheckRow;
import com.walletflow.bank.entity.ReconciliationRun;
import com.walletflow.bank.enums.EReconciliationBreakType;
import com.walletflow.bank.repositories.ProviderTransactionRepository;
import com.walletflow.bank.repositories.ReconciliationBreakRepository;
import com.walletflow.bank.repositories.ReconciliationRunRepository;
import com.walletflow.common.ledger.ExternalMovementsRequest;
import com.walletflow.common.ledger.TopupEntryRef;
import com.walletflow.common.money.ECurrency;
import com.walletflow.common.reconciliation.EReconciliationRunStatus;
import com.walletflow.common.tenant.TenantContext;
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
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Đối soát bank-adapter ↔ sổ cái — phần tầng 3 KHÔNG cần sao kê provider (roadmap 3.1 PR2a, ADR-024).
 *
 * <p>Câu hỏi duy nhất: <b>tiền đi qua cửa ngoài theo sổ của adapter có khớp tiền sổ cái ghi qua
 * {@code EXTERNAL_BANK} không</b>, từng lệnh tiền một. Phía adapter: thu thành công − hoàn thành công.
 * Phía sổ cái: {@code -SUM(postings vào EXTERNAL_BANK)}. So tiền RÒNG chứ không so từng thao tác: nạp
 * bị ledger từ chối rồi hoàn (adapter {@code +x -x}, sổ cái không có gì) và nạp rồi bị đảo (adapter
 * {@code +x -x}, sổ cái {@code +x -x}) đều khớp mà không cần biết saga đã đi đường nào.
 *
 * <p><b>Hai chiều quét, phủ hai tập RỜI nhau</b> (ADR-024 §3):
 * <ol>
 *   <li><b>Xuôi</b> — mọi lệnh bank-adapter có hàng THU. Bắt "đã thu mà chưa ghi sổ", "ghi sổ mà lần
 *       thu đã thất bại", và lệch số tiền.</li>
 *   <li><b>Ngược</b> — mọi bút toán {@code TOPUP} của sổ cái mà bank-adapter KHÔNG có hàng nào. Chiều
 *       xuôi không bao giờ thấy được ca này: tiền vào ví mà không có cổng nào đứng sau.</li>
 * </ol>
 *
 * <p>Hai hàng rào chống báo động giả, cùng lý lẽ ADR-023 §4 — saga ghi sổ SAU KHI adapter chốt lần thu,
 * nên có một khoảng mà adapter nói "đã thu" còn sổ cái chưa có gì, và đó là bình thường:
 * chỉ so lệnh đã yên quá {@code settle-window} (mọi thao tác đã kết thúc, không đổi gì trong cửa sổ), và
 * đọc lại cả hai phía một lần nữa trước khi ghi break.
 *
 * <p><b>Không bao giờ tự sửa.</b> Không có "số đúng" để chép về: sổ cái append-only, và nạp bù hay đảo
 * bút toán là quyết định có 4-eyes (ADR-021/022). Job chỉ phát hiện; người quyết.
 *
 * <p>Mọi lời gọi ledger nằm NGOÀI transaction DB (CLAUDE.md) — mỗi câu đọc/ghi của repository tự là
 * một transaction ngắn.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "walletflow.bank.reconciliation", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class ProviderLedgerReconciler {

    /** Khoá tách lượt theo phép kiểm — xem {@code reconciliation_runs.kind}. */
    public static final String KIND = "PROVIDER_LEDGER";

    /** Con trỏ chiều xuôi bắt đầu từ UUID nhỏ nhất: {@code id > đây} là mọi hàng. */
    private static final UUID BEFORE_FIRST = new UUID(0L, 0L);

    private static final int MAX_FAILURE_MESSAGE = 500;

    private final ProviderTransactionRepository providerTransactions;
    private final ReconciliationRunRepository runRepository;
    private final ReconciliationBreakRepository breakRepository;
    private final LedgerClient ledgerClient;
    private final Duration interval;
    private final Duration settleWindow;
    private final int batchSize;
    private final int maxBreaksPerRun;
    private final AtomicLong openBreaks;
    private final AtomicLong lastSuccessEpoch;
    private final Map<EReconciliationBreakType, AtomicLong> openByType =
            new EnumMap<>(EReconciliationBreakType.class);
    private final Counter failures;

    public ProviderLedgerReconciler(
            ProviderTransactionRepository providerTransactions,
            ReconciliationRunRepository runRepository,
            ReconciliationBreakRepository breakRepository,
            LedgerClient ledgerClient,
            MeterRegistry meterRegistry,
            @Value("${walletflow.bank.reconciliation.interval:PT24H}") Duration interval,
            @Value("${walletflow.bank.reconciliation.settle-window:PT1H}") Duration settleWindow,
            @Value("${walletflow.bank.reconciliation.batch-size:200}") int batchSize,
            @Value("${walletflow.bank.reconciliation.max-breaks-per-run:500}") int maxBreaksPerRun) {
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalStateException(
                    "walletflow.bank.reconciliation.interval phải dương, nhận: " + interval);
        }
        if (settleWindow.isNegative()) {
            throw new IllegalStateException(
                    "walletflow.bank.reconciliation.settle-window không được âm, nhận: " + settleWindow);
        }
        // Trần trên là giới hạn ledger cưỡng chế cho MỘT lời gọi (cả hai endpoint cùng 500). Vượt qua thì
        // mọi lô bị ledger từ chối 400 — job chết hằng đêm mà build vẫn xanh.
        int ledgerCap = Math.min(ExternalMovementsRequest.MAX_IDS, TopupEntryRef.MAX_PAGE);
        if (batchSize < 1 || batchSize > ledgerCap) {
            throw new IllegalStateException("walletflow.bank.reconciliation.batch-size (" + batchSize
                    + ") phải nằm trong [1, " + ledgerCap + "]");
        }
        if (maxBreaksPerRun < 1) {
            throw new IllegalStateException("walletflow.bank.reconciliation.max-breaks-per-run ("
                    + maxBreaksPerRun + ") phải >= 1");
        }
        this.providerTransactions = providerTransactions;
        this.runRepository = runRepository;
        this.breakRepository = breakRepository;
        this.ledgerClient = ledgerClient;
        this.interval = interval;
        this.settleWindow = settleWindow;
        this.batchSize = batchSize;
        this.maxBreaksPerRun = maxBreaksPerRun;
        // Đăng ký lúc khởi động để series tồn tại ở 0 — alert chỉ bắn được khi metric có mặt. Cùng tên
        // và nhãn `tier` với tầng 1/2, nên ReconciliationBreakOpen và ReconciliationRunFailed tự phủ tầng
        // này; chỉ alert "job chết" cần một rule riêng (chu kỳ khác).
        this.openBreaks = meterRegistry.gauge("walletflow.reconciliation.breaks",
                Tags.of("tier", "bank"), new AtomicLong());
        this.lastSuccessEpoch = meterRegistry.gauge("walletflow.reconciliation.last_success_timestamp",
                Tags.of("tier", "bank"), new AtomicLong());
        this.failures = meterRegistry.counter("walletflow.reconciliation.failures", Tags.of("tier", "bank"));
        // Theo LOẠI: hai loại cần hai mức alert khác nhau (ADR-024 §6) — gauge tổng không tách được.
        for (EReconciliationBreakType type : EReconciliationBreakType.values()) {
            openByType.put(type, meterRegistry.gauge("walletflow.reconciliation.provider_ledger.open",
                    Tags.of("type", type.name()), new AtomicLong()));
        }
    }

    /**
     * Nạp mốc thành công và số break đang mở TỪ DB — lúc khởi động, và định kỳ, TÁCH khỏi việc giành lượt.
     *
     * <p>Lúc khởi động: để deploy không làm alert kêu oan. Định kỳ: vì alert {@code PostedWithoutProviderMoney}
     * là PAGE. Nếu gauge chỉ cập nhật cuối một lượt đã giành được thì (1) người trực đóng break xong vẫn bị
     * gọi tới tận lượt ngày hôm sau, và (2) một replica thua lượt giữ mãi con số đọc lúc khởi động — mà
     * alert lấy {@code max by (job)}, nên nó kêu tới khi pod đó restart. Hai câu COUNT đi trên partial index
     * {@code idx_recon_break_open}, rẻ.
     */
    @PostConstruct
    @Scheduled(fixedDelayString = "${walletflow.bank.reconciliation.gauge-refresh:PT5M}",
            initialDelayString = "${walletflow.bank.reconciliation.gauge-refresh:PT5M}")
    public void refreshGauges() {
        runRepository.findLastSuccess(KIND)
                .map(ReconciliationRun::getWindowStart)
                .ifPresent(windowStart -> lastSuccessEpoch.set(windowStart.getEpochSecond()));
        refreshOpenGauges();
    }

    /** @return số break ghi được trong lượt này, hoặc {@code -1} nếu instance khác đang giữ lượt */
    // CÙNG MỘT property với cửa sổ làm tròn ở constructor — xem LedgerInvariantReconciler.
    @Scheduled(fixedDelayString = "${walletflow.bank.reconciliation.interval:PT24H}",
            initialDelayString = "${walletflow.bank.reconciliation.initial-delay:PT5M}")
    public int reconcile() {
        Instant windowStart = floorToWindow(Instant.now());
        UUID runId = UUID.randomUUID();

        if (runRepository.claim(runId, KIND, windowStart) == 0) {
            log.debug("Đối soát bank ↔ sổ cái: cửa sổ {} đã có instance khác nhận — bỏ qua", windowStart);
            return -1;
        }

        Pass pass = new Pass(runId, Instant.now().minus(settleWindow));
        try {
            forward(pass);
            if (!pass.capped) {
                backward(pass);
            }

            boolean clean = pass.failedBatches == 0 && !pass.capped;
            runRepository.finish(runId,
                    clean ? EReconciliationRunStatus.SUCCEEDED.name() : EReconciliationRunStatus.FAILED.name(),
                    pass.checked, pass.breaks, clean ? null : truncate(pass.summary()));
            if (clean) {
                lastSuccessEpoch.set(windowStart.getEpochSecond());
            } else {
                failures.increment();
            }
            refreshOpenGauges();

            if (pass.breaks == 0 && clean) {
                log.info("Đối soát bank ↔ sổ cái cửa sổ {}: {} lệnh đã soi ({} chưa yên, để lượt sau), khớp",
                        windowStart, pass.checked, pass.unsettled);
            } else {
                log.error("Đối soát bank ↔ sổ cái cửa sổ {}: {} chỗ lệch trên {} lệnh đã soi, {} lô hỏng"
                        + " — cần người xem", windowStart, pass.breaks, pass.checked, pass.failedBatches);
            }
            return pass.breaks;
        } catch (RuntimeException failure) {
            // Lỗi KỸ THUẬT ngoài phạm vi một lô (quét DB hỏng) — CLAUDE.md §7: không phải lệch tiền.
            runRepository.finish(runId, EReconciliationRunStatus.FAILED.name(), pass.checked, pass.breaks,
                    truncate(failure.toString()));
            failures.increment();
            log.error("Đối soát bank ↔ sổ cái cửa sổ {} HỎNG sau {} lệnh: {}",
                    windowStart, pass.checked, failure.getMessage(), failure);
            return pass.breaks;
        }
    }

    // ------------------------------------------------------------------
    // Chiều xuôi — từ provider_transactions
    // ------------------------------------------------------------------

    private void forward(Pass pass) {
        UUID cursor = BEFORE_FIRST;
        while (true) {
            List<ChargeCheckRow> batch = providerTransactions.scanChargesAcrossTenants(cursor, batchSize);
            if (batch.isEmpty()) {
                return;
            }
            cursor = batch.getLast().getChargeId();

            List<ChargeCheckRow> settled = new ArrayList<>(batch.size());
            for (ChargeCheckRow row : batch) {
                if (isSettled(row, pass.settledBefore)) {
                    settled.add(row);
                } else {
                    pass.unsettled++;
                }
            }
            pass.checked += settled.size();

            // Bắt lỗi THEO LÔ (ADR-023 *Amendments* A2): ledger chập chờn ở một lô chỉ mất lô đó. Con trỏ
            // đã tiến trước khi gọi ledger, nên lô hỏng bị bỏ qua chứ không bị thử lại vô hạn.
            try {
                inspectForward(pass, settled);
            } catch (RuntimeException batchFailure) {
                pass.batchFailed("chiều xuôi, lô kết thúc ở " + cursor, batchFailure);
            }
            if (pass.hitCap(maxBreaksPerRun)) {
                return;
            }
        }
    }

    private void inspectForward(Pass pass, List<ChargeCheckRow> settled) {
        Map<UUID, List<ChargeCheckRow>> byTenant = new LinkedHashMap<>();
        for (ChargeCheckRow row : settled) {
            byTenant.computeIfAbsent(row.getTenantId(), id -> new ArrayList<>()).add(row);
        }

        for (Map.Entry<UUID, List<ChargeCheckRow>> group : byTenant.entrySet()) {
            UUID tenantId = group.getKey();
            List<UUID> ids = group.getValue().stream().map(ChargeCheckRow::getTransactionId).toList();
            Map<UUID, Map<ECurrency, Long>> ledger =
                    TenantContext.callWith(tenantId, () -> ledgerClient.externalMovementsOf(ids));

            for (ChargeCheckRow row : group.getValue()) {
                Map<ECurrency, Long> inLedger = ledger.getOrDefault(row.getTransactionId(), Map.of());
                if (!mismatches(providerSide(row), inLedger).isEmpty()) {
                    confirmForward(pass, row);
                }
            }
        }
    }

    /**
     * Đọc lại CẢ HAI phía rồi mới kết luận (ADR-023 §4). Kết luận phải mô tả cái thấy lúc kết luận:
     * giữa hai lần đọc, saga có thể vừa ghi sổ, hoặc lệnh hoàn vừa chốt.
     */
    private void confirmForward(Pass pass, ChargeCheckRow suspect) {
        UUID tenantId = suspect.getTenantId();
        UUID transactionId = suspect.getTransactionId();

        Optional<ChargeCheckRow> fresh = providerTransactions.recheckCharge(tenantId, transactionId);
        if (fresh.isEmpty() || !isSettled(fresh.get(), pass.settledBefore)) {
            log.debug("Đối soát bank ↔ sổ cái: lệnh {} vừa đổi giữa hai lần đọc — để lượt sau", transactionId);
            return;
        }
        Map<ECurrency, Long> inLedger = TenantContext.callWith(tenantId,
                () -> ledgerClient.externalMovementsOf(List.of(transactionId)))
                .getOrDefault(transactionId, Map.of());

        record(pass, tenantId, transactionId, mismatches(providerSide(fresh.get()), inLedger),
                "theo sổ của bank-adapter");
    }

    // ------------------------------------------------------------------
    // Chiều ngược — từ bút toán TOPUP của sổ cái
    // ------------------------------------------------------------------

    private void backward(Pass pass) {
        Instant afterPostedAt = null;
        UUID afterId = null;
        while (true) {
            List<TopupEntryRef> page;
            try {
                page = ledgerClient.topupsAfter(afterPostedAt, afterId, pass.settledBefore, batchSize);
            } catch (RuntimeException pageFailure) {
                // KHÁC chiều xuôi: không lấy được trang thì không biết con trỏ tiếp theo, nên không thể
                // bỏ qua một trang mà quét tiếp. Dừng chiều ngược, lượt kết thúc FAILED và alert bắn.
                pass.batchFailed("chiều ngược, trang sau " + afterPostedAt + "/" + afterId, pageFailure);
                return;
            }
            if (page.isEmpty()) {
                return;
            }
            TopupEntryRef last = page.getLast();
            if (!advances(last, afterPostedAt, afterId)) {
                // Ledger trả một trang không đi qua con trỏ (bug phía ledger, hoặc đồng hồ ngược): quét tiếp
                // là lặp vô hạn trên thread của bộ lập lịch, chiếm luôn chỗ của outbox relay.
                pass.batchFailed("chiều ngược, con trỏ không tiến ở " + afterPostedAt + "/" + afterId,
                        new IllegalStateException("trang bút toán nạp không đi qua con trỏ đã gửi"));
                return;
            }
            afterPostedAt = last.postedAt();
            afterId = last.entryId();

            try {
                inspectBackward(pass, page);
            } catch (RuntimeException batchFailure) {
                pass.batchFailed("chiều ngược, trang kết thúc ở " + afterId, batchFailure);
            }
            if (pass.hitCap(maxBreaksPerRun)) {
                return;
            }
        }
    }

    private void inspectBackward(Pass pass, List<TopupEntryRef> page) {
        Map<UUID, Set<UUID>> byTenant = new LinkedHashMap<>();
        for (TopupEntryRef entry : page) {
            byTenant.computeIfAbsent(entry.tenantId(), id -> new LinkedHashSet<>()).add(entry.transactionId());
        }

        for (Map.Entry<UUID, Set<UUID>> group : byTenant.entrySet()) {
            UUID tenantId = group.getKey();
            Set<UUID> unknown = new LinkedHashSet<>(group.getValue());
            unknown.removeAll(new HashSet<>(providerTransactions.knownTransactionIds(tenantId, group.getValue())));
            // Chỉ đếm lệnh adapter KHÔNG biết: lệnh nó biết đã được đếm ở chiều xuôi.
            pass.checked += unknown.size();
            if (unknown.isEmpty()) {
                // Lệnh adapter biết thì chiều xuôi đã so — hai chiều phủ hai tập RỜI nhau.
                continue;
            }
            Map<UUID, Map<ECurrency, Long>> ledger =
                    TenantContext.callWith(tenantId, () -> ledgerClient.externalMovementsOf(unknown));
            for (UUID transactionId : unknown) {
                Map<ECurrency, Long> inLedger = ledger.getOrDefault(transactionId, Map.of());
                if (!mismatches(Map.of(), inLedger).isEmpty()) {
                    confirmBackward(pass, tenantId, transactionId);
                }
            }
        }
    }

    private void confirmBackward(Pass pass, UUID tenantId, UUID transactionId) {
        if (!providerTransactions.knownTransactionIds(tenantId, List.of(transactionId)).isEmpty()) {
            // Adapter vừa nhận lệnh này giữa hai lần đọc — chiều xuôi của lượt sau sẽ so nó.
            return;
        }
        Map<ECurrency, Long> inLedger = TenantContext.callWith(tenantId,
                () -> ledgerClient.externalMovementsOf(List.of(transactionId)))
                .getOrDefault(transactionId, Map.of());

        record(pass, tenantId, transactionId, mismatches(Map.of(), inLedger),
                "bank-adapter KHÔNG có hàng nào cho lệnh này — không cổng nào đứng sau bút toán nạp");
    }

    // ------------------------------------------------------------------
    // Phép so và ghi break
    // ------------------------------------------------------------------

    /**
     * Mọi loại tiền mà hai phía nói khác nhau. {@code EXTERNAL_BANK} là một tài khoản cho mỗi loại tiền,
     * nên so riêng từng loại: một lệnh nạp VND mà sổ cái ghi vào USD là HAI chỗ lệch, không phải một.
     */
    private static Map<ECurrency, Mismatch> mismatches(Map<ECurrency, Long> provider, Map<ECurrency, Long> ledger) {
        Set<ECurrency> currencies = new LinkedHashSet<>(provider.keySet());
        currencies.addAll(ledger.keySet());
        Map<ECurrency, Mismatch> result = new EnumMap<>(ECurrency.class);
        for (ECurrency currency : currencies) {
            long expected = provider.getOrDefault(currency, 0L);
            long actual = ledger.getOrDefault(currency, 0L);
            if (expected != actual) {
                result.put(currency, new Mismatch(expected, actual));
            }
        }
        return result;
    }

    private static Map<ECurrency, Long> providerSide(ChargeCheckRow row) {
        return Map.of(ECurrency.valueOf(row.getCurrency()), row.getNetInMinor());
    }

    /**
     * Ghi break và đếm NGAY sau mỗi lần ghi thành công — không cộng dồn qua giá trị trả về của cả lô.
     * Một lô hỏng giữa chừng (lần đọc lại thứ hai ném) vẫn để lại các break đã ghi trước đó trong DB;
     * chúng phải có mặt trong {@code break_count} và được trần {@code max-breaks-per-run} nhìn thấy.
     */
    private void record(Pass pass, UUID tenantId, UUID transactionId,
                        Map<ECurrency, Mismatch> confirmed, String context) {
        if (confirmed.isEmpty()) {
            log.debug("Đối soát bank ↔ sổ cái: lệch ở lệnh {} biến mất ở lần đọc thứ hai — không phải sự cố",
                    transactionId);
            return;
        }
        for (Map.Entry<ECurrency, Mismatch> entry : confirmed.entrySet()) {
            Mismatch mismatch = entry.getValue();
            EReconciliationBreakType type = EReconciliationBreakType.of(mismatch.delta());
            breakRepository.record(UUID.randomUUID(), pass.runId, tenantId, type.name(),
                    transactionId + ":" + entry.getKey().name(), entry.getKey().name(),
                    mismatch.expected(), mismatch.actual(), detailOf(type, context));
            pass.breaks++;
            log.error("ĐỐI SOÁT: lệnh {} (tenant {}) lệch kiểu {} — provider {} vs sổ cái {} {} ({})",
                    transactionId, tenantId, type, mismatch.expected(), mismatch.actual(), entry.getKey(), context);
        }
    }

    private static String detailOf(EReconciliationBreakType type, String context) {
        String cause = switch (type) {
            case PROVIDER_MONEY_NOT_POSTED -> "provider đã thu (ròng) nhiều hơn sổ cái ghi qua EXTERNAL_BANK"
                    + " — khách đã trả mà ví chưa nhận đủ. Nếu ca đã được hoàn tay qua portal thì sổ của"
                    + " bank-adapter thiếu lần hoàn đó: ghi rõ khi đóng break";
            case POSTED_WITHOUT_PROVIDER_MONEY -> "sổ cái ghi qua EXTERNAL_BANK nhiều hơn provider thu"
                    + " — tiền vào ví mà không ai trả";
        };
        return cause + "; " + context;
    }

    private void refreshOpenGauges() {
        openBreaks.set(breakRepository.countOpenAcrossTenants());
        openByType.forEach((type, gauge) -> gauge.set(breakRepository.countOpenAcrossTenantsByType(type.name())));
    }

    /**
     * Phần tử cuối của trang phải đứng SAU con trỏ đã gửi. Chỉ so được chắc chắn theo {@code postedAt}
     * và theo đẳng thức: thứ tự UUID của Postgres (byte không dấu) khác {@code UUID.compareTo} của Java
     * (long có dấu), nên khi trùng {@code postedAt} thì chỉ bắt được ca "trả lại đúng con trỏ cũ".
     */
    private static boolean advances(TopupEntryRef last, Instant afterPostedAt, UUID afterId) {
        if (afterPostedAt == null) {
            return true;
        }
        int byTime = last.postedAt().compareTo(afterPostedAt);
        return byTime > 0 || (byTime == 0 && !last.entryId().equals(afterId));
    }

    /**
     * Lệnh đã YÊN: mọi thao tác đã kết thúc, và không thao tác nào đổi trong {@code settle-window}. Hàng
     * rào thứ nhất (ADR-023 §4): saga ghi sổ bất đồng bộ SAU KHI lần thu chốt, nên lệnh vừa chốt mà sổ
     * cái chưa có gì là bình thường.
     */
    private static boolean isSettled(ChargeCheckRow row, Instant settledBefore) {
        return !row.getInFlight() && row.getLastChangedAt().isBefore(settledBefore);
    }

    /** Làm tròn xuống theo chu kỳ — xem {@code LedgerInvariantReconciler.floorToWindow}. */
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

    /** {@code delta = actual (sổ cái) - expected (provider)} — cùng quy ước với cột {@code delta_minor}. */
    private record Mismatch(long expected, long actual) {

        /**
         * {@code subtractExact}, không {@code -} trần (ADR-005): tràn số thì dấu của delta lật âm thầm và
         * break mang sai LOẠI (page ↔ ticket). Ném thì lô hỏng, lượt FAILED, alert bắn — đúng thứ tự.
         */
        long delta() {
            return Math.subtractExact(actual, expected);
        }
    }

    /** Trạng thái của MỘT lượt — gom lại để hai chiều quét cùng cộng vào một chỗ. */
    private static final class Pass {

        private final UUID runId;
        private final Instant settledBefore;
        private long checked;
        private long unsettled;
        private int breaks;
        private int failedBatches;
        private String firstFailure;
        private boolean capped;

        private Pass(UUID runId, Instant settledBefore) {
            this.runId = runId;
            this.settledBefore = settledBefore;
        }

        private void batchFailed(String where, RuntimeException failure) {
            failedBatches++;
            if (firstFailure == null) {
                firstFailure = where + ": " + failure;
            }
            log.error("Đối soát bank ↔ sổ cái: {} hỏng, bỏ qua: {}", where, failure.getMessage(), failure);
        }

        /**
         * Lệch DIỆN RỘNG là ca phải có người (ADR-023 *Amendments* A3): mỗi break kéo theo hai lời gọi
         * ledger và một lần ghi, không có trần thì chính lượt đối soát thành sự cố.
         */
        private boolean hitCap(int maxBreaksPerRun) {
            if (breaks >= maxBreaksPerRun) {
                capped = true;
                log.error("Đối soát bank ↔ sổ cái: chạm trần {} chỗ lệch — DỪNG quét, cần người xem ngay",
                        maxBreaksPerRun);
            }
            return capped;
        }

        private String summary() {
            StringBuilder note = new StringBuilder();
            if (capped) {
                note.append("Chạm trần số chỗ lệch mỗi lượt — quét chưa hết. ");
            }
            if (failedBatches > 0) {
                note.append(failedBatches).append(" lô hỏng; lỗi đầu tiên: ").append(firstFailure);
            }
            return note.toString();
        }
    }
}
