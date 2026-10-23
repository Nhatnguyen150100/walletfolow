package com.walletflow.transaction.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Ghi dấu vết thao tác vận hành (roadmap 2.5 PR1, ADR-021).
 *
 * <p><b>Transaction RIÊNG, và lỗi được NUỐT — khác với audit của thao tác đụng tiền.</b> Ở PR1 mọi
 * thao tác là ĐỌC, và một thao tác đọc không được hỏng chỉ vì không ghi được dấu vết: mất dấu vết
 * của một lần xem là chuyện đáng kêu to, không phải lý do để người trực không nhìn được hàng đợi
 * đúng lúc đang có sự cố.
 *
 * <p>Thao tác ĐỤNG TIỀN (2.5 PR2) thì ngược lại hoàn toàn: dấu vết phải vào CÙNG transaction với
 * việc ghi tiền, vì "không có thao tác đụng tiền nào không để lại audit" là một bất biến chứ không
 * phải một nỗ lực. <b>Đừng dùng lại lớp này cho chúng.</b>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AuditService {

    private final AuditWriter writer;
    private final ObjectMapper objectMapper;

    /**
     * @param target id của thứ bị tác động, hoặc null khi thao tác không nhắm vào một hàng cụ thể
     * @param detail ngữ cảnh thêm. Nhận {@code Map} chứ không nhận chuỗi JSON dựng sẵn: cột là
     *               {@code jsonb}, nên một tài liệu hỏng vì ghép chuỗi sẽ làm INSERT nổ, bị nuốt ở
     *               đây, và mất dấu vết trong im lặng. KHÔNG được chứa token, mật khẩu hay dữ liệu
     *               thanh toán — bảng này không xoá được.
     */
    public void record(Operator actor, String action, String target, Map<String, ?> detail) {
        try {
            writer.write(actor, action, target, objectMapper.writeValueAsString(detail));
        } catch (Exception failure) {
            // Nuốt có chủ ý — xem javadoc của lớp. ERROR vì một audit không ghi được là bất thường
            // thật sự, kể cả khi nó không chặn được việc đang làm.
            log.error("Không ghi được audit action={} target={} bởi {}: {}",
                    action, target, actor.id(), failure.toString());
        }
    }
}
