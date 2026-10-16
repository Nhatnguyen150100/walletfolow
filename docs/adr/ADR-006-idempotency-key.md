# ADR-006 — Idempotency key cho mọi lệnh ghi tiền

**Trạng thái:** Accepted · **Ngày:** 2026-08-12 · **Milestone:** 1 (ledger), 3 (API)

## Context

Client gửi lệnh chuyển tiền, mạng timeout. Client **không biết** server đã xử lý hay chưa — đó là
bản chất của timeout, không phải lỗi của client. Nó chỉ có hai lựa chọn: thử lại (có thể trừ tiền
hai lần) hoặc bỏ cuộc (có thể mất một giao dịch đã thành công).

Vấn đề này không giải được ở phía client. Nó phải được giải ở phía server.

## Decision

Mọi lệnh ghi tiền phải idempotent theo một khoá **tường minh**, ở hai tầng:

**Tầng API** (`transaction-service`, Phase 1): client gửi header `Idempotency-Key`, giữ nguyên khi
retry. Bảng `idempotency_keys` với `PRIMARY KEY (tenant_id, key)` và lưu `request_hash` + response
đã trả.

**Tầng ledger** (đã có): idempotent theo `(transaction_id, type)` — ràng buộc UNIQUE ở DB.

Chi tiết quyết định mọi thứ: **lệnh trùng TRẢ VỀ kết quả cũ, không ném lỗi.** Nếu ledger báo lỗi khi
nhận lại một lệnh đã xử lý, Saga orchestrator rất dễ hiểu đó là "ghi sổ thất bại" và chạy bù trừ
cho một giao dịch mà tiền **đã** vào sổ — tức là mất tiền thật. Lệnh trùng không phải lỗi; với
Kafka at-least-once, nó là chuyện bình thường.

Ngoài ra `request_hash` phát hiện client tái dùng key sai (cùng key, body khác) → `422`, vì đó là
bug phía client và phải báo to chứ không đoán ý.

## Consequences

- ✅ Retry an toàn ở mọi tầng; đây là tiền đề để Saga được phép retry bất kỳ bước nào.
- ✅ Chống replay attack ở tầng lệnh.
- ⚠️ Bảng `idempotency_keys` cần TTL và job dọn.
- ⚠️ `bank-adapter` **cũng phải** idempotent theo khoá của chính nó, nếu không Saga retry bước
  charge sẽ charge thật hai lần (Phase 2).
- ⚠️ Hai lệnh trùng chạy song song cần xử lý riêng: ledger kiểm tra trùng **hai lần** — trước và
  sau khi lấy khoá hàng.

## Liên quan
ADR-002, ADR-008, `LedgerService.postJournal`, `LedgerServiceIT`
