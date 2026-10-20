package com.walletflow.bank.provider.mock;

import com.walletflow.bank.enums.EProviderState;
import com.walletflow.bank.provider.ChargeRequest;
import com.walletflow.bank.provider.ChargeResult;
import com.walletflow.bank.provider.PaymentProvider;
import com.walletflow.bank.provider.ProviderStatus;
import com.walletflow.bank.provider.ProviderUnavailableException;
import com.walletflow.bank.provider.RefundRequest;
import com.walletflow.bank.provider.RefundResult;
import com.walletflow.bank.provider.VerifiedWebhook;
import com.walletflow.common.money.ECurrency;
import lombok.extern.slf4j.Slf4j;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Provider giả, giữ phiên trong bộ nhớ, có <b>fail injection</b> (roadmap 2.2): độ trễ, tỉ lệ lỗi kỹ
 * thuật, từ chối theo số tiền, và ca khó nhất — tạo phiên xong rồi mới mất câu trả lời.
 *
 * <p>Cư xử như một provider thật ở hai điểm mà phần còn lại của service dựa vào:
 * <ul>
 *   <li><b>Idempotent theo khoá:</b> {@link #charge} gọi lại cùng idempotency key trả về đúng phiên cũ.</li>
 *   <li><b>Không đoán:</b> hỏi một phiên mà nó không biết thì báo lỗi kỹ thuật, không trả "thất bại".</li>
 * </ul>
 *
 * <p><b>Không bao giờ được chạy ở môi trường thật</b> — "thu" ở đây không lấy tiền của ai, nhưng saga vẫn
 * sẽ cộng tiền thật vào ví. {@code PaymentProviderConfig} từ chối khởi động nó dưới profile {@code prod}.
 * Phiên mất khi khởi động lại; lần thu đang chờ lúc đó sẽ bị hỏi lại mãi và báo lỗi — chấp nhận được với
 * máy dev.
 */
@Slf4j
public class MockProvider implements PaymentProvider {

    public static final String NAME = "mock";

    private final String paymentBaseUrl;
    private final Clock clock;
    private final Settings defaults;
    private volatile Settings settings;

    private final ConcurrentMap<String, Session> sessionsByKey = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Session> sessionsByRef = new ConcurrentHashMap<>();
    /**
     * Khoá ký webhook của mock. {@code null} = KHÔNG cấu hình ⇒ mock từ chối mọi webhook. Cố ý không có giá
     * trị mặc định (CLAUDE.md §6): thiếu bí mật thì tính năng TẮT, không bao giờ là một khoá ai cũng đoán được.
     */
    private final String webhookSecret;

    /** Câu trả lời CUỐI theo khoá hoàn — cả hoàn xong lẫn từ chối: cùng khoá thì cùng kết quả (PaymentProvider). */
    private final ConcurrentMap<String, RefundResult> refundsByKey = new ConcurrentHashMap<>();
    private final AtomicInteger chargeCalls = new AtomicInteger();
    private final AtomicInteger failNextCalls = new AtomicInteger();
    private final AtomicInteger loseNextChargeResponses = new AtomicInteger();

    /**
     * @param autoSettleAfter phiên tự chuyển sang {@code settleOutcome} sau chừng này — thay cho người dùng
     *                        bấm thanh toán; {@code null} = chỉ đổi khi gọi {@link #settle}
     */
    public MockProvider(String paymentBaseUrl, Clock clock, Duration latency, double failureRate,
                        long declineAboveMinor, Duration autoSettleAfter, EProviderState settleOutcome,
                        String webhookSecret) {
        this.paymentBaseUrl = paymentBaseUrl;
        this.clock = clock;
        this.defaults = new Settings(latency, failureRate, declineAboveMinor, autoSettleAfter, settleOutcome);
        this.settings = defaults;
        this.webhookSecret = webhookSecret == null || webhookSecret.isBlank() ? null : webhookSecret;
        log.warn("Bank-adapter đang dùng MockProvider — KHÔNG có tiền thật nào được thu.");
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String account() {
        return NAME;
    }

    @Override
    public ChargeResult charge(ChargeRequest request) {
        chargeCalls.incrementAndGet();
        simulateNetwork("charge");

        Settings current = settings;
        if (current.declineAboveMinor() > 0 && request.amountMinor() > current.declineAboveMinor()) {
            return new ChargeResult.Declined("AMOUNT_LIMIT_EXCEEDED",
                    "{\"result\":\"DECLINED\",\"code\":\"AMOUNT_LIMIT_EXCEEDED\"}");
        }

        Session session = sessionsByKey.computeIfAbsent(request.idempotencyKey(), key -> {
            Instant now = clock.instant();
            Session created = new Session("MOCK-" + UUID.randomUUID(), request.amountMinor(),
                    request.currency(), now, now.plus(request.sessionTtl()));
            sessionsByRef.put(created.providerRef, created);
            return created;
        });

        if (takeOne(loseNextChargeResponses)) {
            // Phiên ĐÃ tạo ở "phía provider", nhưng câu trả lời không về tới — đúng ca timeout mơ hồ
            throw new ProviderUnavailableException("Mock: mất câu trả lời sau khi đã tạo phiên");
        }
        // Gọi lại cùng khoá: đúng phiên cũ, kể cả mốc tạo và hạn phiên cũ — như provider thật
        return new ChargeResult.Created(session.providerRef, paymentBaseUrl + "/" + session.providerRef,
                "{\"result\":\"CREATED\",\"ref\":\"" + session.providerRef + "\"}", session.createdAt,
                session.expiresAt);
    }

    /** Mock tra phiên theo mã của chính nó; {@code sessionCreatedAt} chỉ cần cho provider như VNPay. */
    @Override
    public ProviderStatus getStatus(String providerRef, Instant sessionCreatedAt) {
        simulateNetwork("getStatus");
        Session session = sessionsByRef.get(providerRef);
        if (session == null) {
            throw new ProviderUnavailableException("Mock không biết phiên " + providerRef
                    + " (phiên chỉ nằm trong bộ nhớ — mock đã khởi động lại?)");
        }
        autoSettle(session);
        EProviderState state = session.state;
        return new ProviderStatus(state, state == EProviderState.FAILED ? "PAYMENT_FAILED" : null,
                session.capturedMinor, session.currency,
                "{\"ref\":\"" + providerRef + "\",\"state\":\"" + state + "\"}");
    }

    @Override
    public synchronized RefundResult refund(RefundRequest request) {
        simulateNetwork("refund");
        RefundResult existing = refundsByKey.get(request.idempotencyKey());
        if (existing != null) {
            return existing;
        }
        Session session = sessionsByRef.get(request.providerRef());
        if (session == null) {
            throw new ProviderUnavailableException("Mock không biết phiên " + request.providerRef());
        }
        autoSettle(session);
        if (request.amountMinor() <= 0) {
            return decline(request, "INVALID_AMOUNT");
        }
        // Tiền: phép tính có kiểm tràn (CLAUDE.md) — mock cũng nằm trong src/main
        long refundable = Math.subtractExact(session.capturedMinor, session.refundedMinor);
        if (session.state != EProviderState.SUCCEEDED || request.amountMinor() > refundable) {
            return decline(request, "NOT_REFUNDABLE");
        }
        session.refundedMinor = Math.addExact(session.refundedMinor, request.amountMinor());
        String refundRef = "MOCK-RF-" + UUID.randomUUID();
        RefundResult.Refunded refunded = new RefundResult.Refunded(refundRef,
                "{\"result\":\"REFUNDED\",\"ref\":\"" + refundRef + "\"}");
        refundsByKey.put(request.idempotencyKey(), refunded);
        return refunded;
    }

    /**
     * Từ chối cũng được nhớ theo khoá: provider thật trả lời lần hoàn đã từ chối bằng đúng lời từ chối đó. Không nhớ
     * thì gửi lại cùng khoá sau khi phiên đổi trạng thái có thể ra "hoàn xong" — hai câu trả lời trái nhau cho MỘT
     * lần hoàn.
     */
    private RefundResult decline(RefundRequest request, String code) {
        RefundResult declined = new RefundResult.Declined(code,
                "{\"result\":\"DECLINED\",\"code\":\"" + code + "\"}");
        refundsByKey.put(request.idempotencyKey(), declined);
        return declined;
    }

    // ------------------------------------------------------------------
    // Điều khiển cho test / chaos cục bộ
    // ------------------------------------------------------------------

    /**
     * Người dùng "trả tiền" (hoặc trả hỏng) ở trang của provider. Chỉ đổi phiên chưa có kết quả cuối.
     * {@code UNRESOLVED} giả lập ca provider báo trạng thái lơ lửng (đảo giao dịch, nghi gian lận).
     */
    public void settle(String providerRef, EProviderState outcome) {
        Session session = sessionsByRef.get(providerRef);
        if (session == null) {
            throw new IllegalArgumentException("Không có phiên " + providerRef);
        }
        session.settle(outcome);
    }

    /**
     * Người dùng trả tiền nhưng provider thu một số KHÁC số đã yêu cầu (sửa số trên trang thanh toán, lỗi
     * phía provider). Adapter phải phát hiện, không được báo thành công theo số tiền của chính mình.
     */
    public void settleWithCapturedAmount(String providerRef, long capturedMinor) {
        Session session = sessionsByRef.get(providerRef);
        if (session == null) {
            throw new IllegalArgumentException("Không có phiên " + providerRef);
        }
        session.capturedMinor = capturedMinor;
        session.settle(EProviderState.SUCCEEDED);
    }

    /** {@code n} lời gọi kế tiếp lỗi kỹ thuật TRƯỚC khi provider làm gì. */
    public void failNextCalls(int n) {
        failNextCalls.set(n);
    }

    /** {@code n} lần charge kế tiếp tạo phiên xong rồi mới lỗi — câu trả lời "bị mất". */
    public void loseNextChargeResponses(int n) {
        loseNextChargeResponses.set(n);
    }

    public void setLatency(Duration latency) {
        Settings current = settings;
        settings = new Settings(latency, current.failureRate(), current.declineAboveMinor(),
                current.autoSettleAfter(), current.settleOutcome());
    }

    public void setDeclineAboveMinor(long declineAboveMinor) {
        Settings current = settings;
        settings = new Settings(current.latency(), current.failureRate(), declineAboveMinor,
                current.autoSettleAfter(), current.settleOutcome());
    }

    /** Số lần {@link #charge} được gọi — kể cả lần lỗi. */
    public int chargeCalls() {
        return chargeCalls.get();
    }

    /** Số phiên thật sự tồn tại ở "phía provider" — thước đo của "thu hai lần". */
    public int sessionCount() {
        return sessionsByKey.size();
    }

    /** Về cấu hình ban đầu, xoá mọi phiên và bộ đếm. */
    public void reset() {
        settings = defaults;
        sessionsByKey.clear();
        sessionsByRef.clear();
        refundsByKey.clear();
        chargeCalls.set(0);
        failNextCalls.set(0);
        loseNextChargeResponses.set(0);
    }

    /**
     * Mock ký webhook cùng kiểu VNPay (HMAC-SHA512 trên chuỗi query đã sắp xếp) để test được đúng hình dạng
     * tấn công thật, nhưng bằng tham số riêng: {@code ref} và {@code signature}.
     *
     * <p>Chưa cấu hình {@code walletflow.bank.mock.webhook-secret} thì mọi webhook đều bị từ chối.
     */
    @Override
    public Optional<VerifiedWebhook> verifyWebhook(Map<String, String> params) {
        String secret = webhookSecret;
        String received = params.get("signature");
        if (secret == null || received == null || received.isEmpty()) {
            return Optional.empty();
        }
        SortedMap<String, String> signed = new TreeMap<>(params);
        signed.remove("signature");
        if (signed.isEmpty()) {
            return Optional.empty();
        }
        // PHẢI encode cả tên lẫn giá trị, y như VNPay. Nối thô thì một giá trị chứa '&' hoặc '=' dựng ra
        // cùng chuỗi ký với một payload khác hẳn: bắt được webhook "ref=A&state=PENDING" là phát lại được
        // "?ref=A%26state%3DPENDING" với ĐÚNG chữ ký cũ, nhưng provider_ref ghi vào DB thành thứ khác.
        String data = signed.entrySet().stream()
                .map(entry -> URLEncoder.encode(entry.getKey(), StandardCharsets.US_ASCII)
                        + "=" + URLEncoder.encode(entry.getValue(), StandardCharsets.US_ASCII))
                .collect(Collectors.joining("&"));
        String expected = hmacSha512(secret, data);
        if (!MessageDigest.isEqual(expected.getBytes(StandardCharsets.US_ASCII),
                received.toLowerCase(Locale.ROOT).getBytes(StandardCharsets.US_ASCII))) {
            return Optional.empty();
        }
        String ref = signed.get("ref");
        return Optional.of(new VerifiedWebhook(ref == null || ref.isEmpty() ? null : ref, data));
    }

    /** Dùng lại thuật toán của VNPay để mock là bản sao trung thực về hình dạng, không phải một thứ dễ hơn. */
    private static String hmacSha512(String secret, String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA512");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA512"));
            return HexFormat.of().formatHex(mac.doFinal(data.getBytes(StandardCharsets.UTF_8)));
        } catch (GeneralSecurityException impossible) {
            throw new IllegalStateException("JDK không có HmacSHA512", impossible);
        }
    }

    private void simulateNetwork(String operation) {
        Settings current = settings;
        if (!current.latency().isZero()) {
            try {
                Thread.sleep(current.latency().toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ProviderUnavailableException("Mock: bị ngắt khi đang " + operation, e);
            }
        }
        if (takeOne(failNextCalls)
                || (current.failureRate() > 0 && ThreadLocalRandom.current().nextDouble() < current.failureRate())) {
            throw new ProviderUnavailableException("Mock: lỗi kỹ thuật giả lập khi " + operation);
        }
    }

    private void autoSettle(Session session) {
        Settings current = settings;
        if (current.autoSettleAfter() != null
                && !clock.instant().isBefore(session.createdAt.plus(current.autoSettleAfter()))) {
            session.settle(current.settleOutcome());
        }
    }

    private static boolean takeOne(AtomicInteger counter) {
        return counter.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0;
    }

    private record Settings(Duration latency, double failureRate, long declineAboveMinor,
                            Duration autoSettleAfter, EProviderState settleOutcome) {
    }

    private static final class Session {
        private final String providerRef;
        private final ECurrency currency;
        private final Instant createdAt;
        private final Instant expiresAt;
        private volatile EProviderState state = EProviderState.PENDING;
        // Số provider thực thu — mặc định bằng số yêu cầu, settleWithCapturedAmount đổi được
        private volatile long capturedMinor;
        private long refundedMinor;

        private Session(String providerRef, long amountMinor, ECurrency currency, Instant createdAt,
                        Instant expiresAt) {
            this.providerRef = providerRef;
            this.capturedMinor = amountMinor;
            this.currency = currency;
            this.createdAt = createdAt;
            this.expiresAt = expiresAt;
        }

        private synchronized void settle(EProviderState outcome) {
            if (state == EProviderState.PENDING || state == EProviderState.UNRESOLVED) {
                state = outcome;
            }
        }
    }
}
