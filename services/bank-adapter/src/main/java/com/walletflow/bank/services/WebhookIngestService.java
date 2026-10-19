package com.walletflow.bank.services;

import com.walletflow.bank.provider.PaymentProvider;
import com.walletflow.bank.provider.VerifiedWebhook;
import com.walletflow.bank.repositories.WebhookEventRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Nhận webhook/IPN của cổng thanh toán (roadmap 2.3, ADR-018).
 *
 * <p>Chỉ làm đúng ba việc, theo thứ tự này: <b>kiểm chữ ký → ghi nguyên văn → trả lời ngay</b>. Việc đọc
 * nghĩa payload và hỏi lại provider thuộc về worker bất đồng bộ (2.3b).
 *
 * <p><b>Vì sao không xử lý tại chỗ:</b> theo ADR-016 §1 kết quả chỉ được chốt sau khi adapter tự
 * {@code getStatus} — tức một lời gọi mạng ra provider. Làm việc đó bên trong request của webhook là
 * nguyên nhân kinh điển của "provider retry 5 lần vì bạn trả lời chậm", và mỗi lần retry lại là một
 * request nữa phải phục vụ.
 *
 * <p><b>Ranh giới tin cậy:</b> endpoint nằm ngoài JWT nên mọi thứ chưa qua {@code verifyWebhook} đều là dữ
 * liệu của người lạ. Chữ ký sai ⇒ không ghi gì cả: ghi cả thứ chưa xác thực là biến endpoint công khai
 * thành kênh bơm dữ liệu rác vào DB.
 */
@Slf4j
@Service
public class WebhookIngestService {

    /** Kết quả nhận một webhook — controller dịch sang hình dạng câu trả lời mà từng provider mong đợi. */
    public enum Outcome {
        /** Chữ ký hợp lệ, vừa ghi lại. Worker 2.3b sẽ xử lý. */
        ACCEPTED,
        /** Chữ ký hợp lệ nhưng đã nhận đúng webhook này rồi — không ghi lần hai. */
        DUPLICATE,
        /** Chữ ký sai/thiếu, hoặc provider trong đường dẫn không phải provider đang chạy. */
        REJECTED
    }

    private final PaymentProvider provider;
    private final WebhookEventRepository repository;
    private final Counter accepted;
    private final Counter duplicates;
    private final Counter rejected;

    public WebhookIngestService(PaymentProvider provider, WebhookEventRepository repository,
                                MeterRegistry meterRegistry) {
        this.provider = provider;
        this.repository = repository;
        // Đăng ký lúc khởi động để series tồn tại ở 0
        this.accepted = meterRegistry.counter("walletflow.bank.webhook.received", "outcome", "accepted");
        this.duplicates = meterRegistry.counter("walletflow.bank.webhook.received", "outcome", "duplicate");
        this.rejected = meterRegistry.counter("walletflow.bank.webhook.received", "outcome", "rejected");
        // Tồn đọng chưa xử lý. Trong 2.3a chưa có worker nên con số này chỉ tăng — cố ý đo ngay từ bước
        // này để biết bước sau có đang nợ việc không, thay vì phát hiện lúc bảng đã phình.
        meterRegistry.gauge("walletflow.bank.webhook.unprocessed", this, service -> service.repository.countUnprocessed());
    }

    /**
     * @param providerName tên provider lấy từ đường dẫn — phải khớp provider đang chạy, nếu không thì một
     *                     webhook VNPay gửi tới hệ thống đang chạy mock sẽ được kiểm bằng khoá của mock
     * @param params       tham số đã giải mã URL
     * @param rawPayload   nguyên văn query string, lưu làm bằng chứng đối soát
     */
    @Transactional
    public Outcome accept(String providerName, Map<String, String> params, String rawPayload) {
        if (!provider.name().equals(providerName)) {
            rejected.increment();
            // DEBUG chứ không WARN: endpoint công khai, mỗi request rác một dòng log là kênh bơm log cho
            // người lạ và dìm chết tín hiệu thật. Nguồn alert là counter ...{outcome=rejected}.
            log.debug("Bank: webhook gửi cho provider '{}' nhưng đang chạy '{}' — từ chối",
                    providerName, provider.name());
            return Outcome.REJECTED;
        }
        Optional<VerifiedWebhook> verified = provider.verifyWebhook(params);
        if (verified.isEmpty()) {
            rejected.increment();
            // KHÔNG log payload: nó của người lạ và có thể mang dữ liệu nhạy cảm hoặc rác cố tình bơm vào.
            // DEBUG vì lý do như trên — counter mới là thứ để alert.
            log.debug("Bank: webhook {} có chữ ký KHÔNG hợp lệ — bỏ qua, không ghi", providerName);
            return Outcome.REJECTED;
        }

        VerifiedWebhook webhook = verified.get();
        String eventId = sha256Hex(webhook.signedData());
        int inserted = repository.insertIfAbsent(UUID.randomUUID(), providerName, eventId,
                webhook.providerRef(), rawPayload);
        if (inserted == 0) {
            duplicates.increment();
            log.info("Bank: webhook {} trùng (ref {}) — đã nhận trước đó, không xử lý lại",
                    providerName, webhook.providerRef());
            return Outcome.DUPLICATE;
        }
        accepted.increment();
        log.info("Bank: nhận webhook {} cho ref {} — đã ghi, sẽ xử lý bất đồng bộ",
                providerName, webhook.providerRef());
        return Outcome.ACCEPTED;
    }

    /**
     * Khoá chống phát lại: SHA-256 của CHÍNH chuỗi provider đã ký (ADR-018).
     *
     * <p>Băm chuỗi đã ký chứ không chọn một trường nào đó làm khoá, vì VNPay không bảo đảm trường nào là
     * duy nhất. Hệ quả có chủ ý: IPN phát lại y hệt thì trùng, còn IPN mang trạng thái MỚI khác payload
     * nên vẫn được xử lý — đúng thứ ta cần, vì webhook đến sai thứ tự là chuyện bình thường.
     */
    private static String sha256Hex(String data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(data.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("JDK không có SHA-256", impossible);
        }
    }
}
