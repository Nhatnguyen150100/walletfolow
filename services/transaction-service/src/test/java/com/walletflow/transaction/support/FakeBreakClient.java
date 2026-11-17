package com.walletflow.transaction.support;

import com.walletflow.common.reconciliation.ReconciliationBreakView;
import com.walletflow.common.tenant.TenantContext;
import com.walletflow.transaction.client.BreakClient;
import com.walletflow.transaction.client.EBreakSource;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Ba service sở hữu chỗ lệch giả (roadmap 3.3, ADR-025), phân theo tenant như thật.
 *
 * <p>Phân biệt ba trạng thái mà test phải kiểm riêng từng cái: <b>có chỗ lệch</b>, <b>không có chỗ
 * lệch nào</b> (danh sách rỗng) và <b>không hỏi được</b> ({@code Optional.empty()} —
 * {@link #goDown(EBreakSource)}). Gộp hai cái sau làm một là lỗi mà {@code available} sinh ra để chặn
 * (ADR-021 B1).
 *
 * <p>Ghi lại mọi lời gọi {@code close} — kể cả lời gọi trả {@code NOT_FOUND}/{@code UNAVAILABLE} — để
 * test chứng minh cả hai chiều: validate/RBAC chặn THẬT trước khi sang service sở hữu (không có lời
 * gọi nào), và lời gọi đi sang mang đúng người đóng, đúng lý do.
 */
public class FakeBreakClient implements BreakClient {

    /** Một lời gọi {@code close} đã đi tới service sở hữu. */
    public record CloseCall(EBreakSource source, UUID breakId, UUID closedBy, String note) {
    }

    private final Map<EBreakSource, List<ReconciliationBreakView>> breaks = new EnumMap<>(EBreakSource.class);
    private final Set<EBreakSource> down = EnumSet.noneOf(EBreakSource.class);
    private final List<CloseCall> closeCalls = new CopyOnWriteArrayList<>();
    private volatile CloseOutcome outcome = CloseOutcome.CLOSED;

    public synchronized void reset() {
        breaks.clear();
        down.clear();
        closeCalls.clear();
        outcome = CloseOutcome.CLOSED;
    }

    /** Khai một chỗ lệch đang mở ở một nguồn. Tenant lấy từ chính view, như bảng thật. */
    public synchronized void put(EBreakSource source, ReconciliationBreakView view) {
        breaks.computeIfAbsent(source, s -> new ArrayList<>()).add(view);
    }

    /** Nguồn không hỏi được — client thật nuốt lỗi và trả {@code Optional.empty()}. */
    public synchronized void goDown(EBreakSource source) {
        down.add(source);
    }

    public synchronized void goUp(EBreakSource source) {
        down.remove(source);
    }

    /** Đặt kết cục cho các lần {@code close} sau. Mặc định {@code CLOSED}. */
    public void respondToClose(CloseOutcome value) {
        this.outcome = value;
    }

    /** Các lời gọi {@code close} đã tới — rỗng nghĩa là validate/RBAC đã chặn trước. */
    public List<CloseCall> closeCalls() {
        return List.copyOf(closeCalls);
    }

    @Override
    public synchronized Optional<List<ReconciliationBreakView>> openBreaks(EBreakSource source, int limit) {
        if (down.contains(source)) {
            return Optional.empty();
        }
        UUID tenantId = TenantContext.require();
        return Optional.of(breaks.getOrDefault(source, List.of()).stream()
                .filter(view -> tenantId.equals(view.tenantId()))
                .limit(limit)
                .toList());
    }

    @Override
    public CloseOutcome close(EBreakSource source, UUID breakId, UUID closedBy, String note) {
        closeCalls.add(new CloseCall(source, breakId, closedBy, note));
        return outcome;
    }
}
