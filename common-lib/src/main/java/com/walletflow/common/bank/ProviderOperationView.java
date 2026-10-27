package com.walletflow.common.bank;

import com.walletflow.common.event.EBankOperation;
import com.walletflow.common.money.ECurrency;

import java.time.Instant;
import java.util.UUID;

/**
 * Một thao tác với provider (một lần thu, hoặc một lần hoàn), nhìn từ ngoài bank-adapter.
 *
 * @param status      trạng thái ở bank-adapter dưới dạng chuỗi, không phải enum: {@code EProviderTxnStatus}
 *                    thuộc về bank-adapter và chỉ bank-adapter mới được phép đổi nó. Kéo nó lên
 *                    common-lib để người gọi {@code switch} trên nó là biến một chi tiết nội bộ thành
 *                    hợp đồng, rồi thêm một trạng thái mới sẽ phá người gọi.
 * @param rawResponse câu trả lời của provider, đã đi qua {@code SensitiveDataMasker} trước khi rời
 *                    bank-adapter (ADR-017, ADR-021 B3). Không parse thành các trường đã chọn: thứ
 *                    người trực cần là mã lỗi mà ta chưa từng lường trước, và mọi lần ta "chọn ra
 *                    vài trường" là một lần thứ đó biến mất. {@code null} khi provider chưa trả lời.
 *                    <p><b>Che tới đâu:</b> chữ ký, secret, token, API key, số thẻ qua được Luhn
 *                    (giữ 4 số cuối), và các khoá PII thường gặp khai tường minh (cvv, pin, email,
 *                    phone, số tài khoản). KHÔNG che được dữ liệu cá nhân nằm trong text tự do hay
 *                    dưới một tên khoá lạ — provider đặt tên gì là chuyện của họ. Đây là lưới an
 *                    toàn, không phải bảo đảm; đừng coi chuỗi này là đã sạch PII.
 *                    <p><b>Không đảm bảo parse được:</b> việc che có thể thay một số không nằm
 *                    trong nháy kép bằng {@code ***1111}, làm JSON hỏng cú pháp. Hiển thị như text
 *                    thô, đừng {@code JSON.parse}. Chuỗi cũng đã được {@code jsonb} chuẩn hoá lúc
 *                    ghi (bỏ khoảng trắng, sắp lại khoá) nên không dùng để kiểm lại chữ ký.
 * @param refundOf    id lần THU mà hàng {@code REFUND} này hoàn; {@code null} với hàng {@code CHARGE}
 */
public record ProviderOperationView(
    UUID id,
    EBankOperation operation,
    String provider,
    String providerRef,
    long amountMinor,
    ECurrency currency,
    String status,
    String failureCode,
    Instant createdAt,
    Instant updatedAt,
    UUID refundOf,
    String rawResponse
) {
}
