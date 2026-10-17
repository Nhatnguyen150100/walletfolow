# ADR-013 — Nhận lệnh chuyển tiền idempotent: một transaction cục bộ, key theo người dùng, lỗi RFC 7807

**Trạng thái:** Accepted · **Ngày:** 2026-10-17 · **Milestone:** Phase 1 (1.2a)

## Context

`POST /api/v1/transactions/transfer` là cửa vào đầu tiên mà người dùng chạm tới để làm tiền di chuyển.
Mạng di động rớt, client timeout rồi gửi lại, người dùng bấm hai lần — ở production, **mọi lệnh tiền
đều sẽ bị gửi nhiều hơn một lần**. Yêu cầu của DoD Phase 1: gửi lại cùng `Idempotency-Key` thì không
trừ tiền lần hai.

ADR-006 chốt "dùng Idempotency-Key". Roadmap đề xuất bảng `idempotency_keys` với trạng thái
`IN_PROGRESS | COMPLETED`, khoá `(tenant, key)`, và bảng luật 6 dòng gồm cả `409 + Retry-After` lẫn
"key thuộc người khác → 404".

## Decision

1. **Toàn bộ phần nhận lệnh là MỘT transaction cục bộ:** giành key + ghi câu trả lời 202 +
   `transactions` + `saga_state` + lệnh `PostJournal` vào outbox. Cùng sống hoặc cùng chết.
2. **Giành key bằng một câu lệnh:** `INSERT … ON CONFLICT DO UPDATE … WHERE expires_at <= now()`.
   Trả 1 = key mới (hoặc key quá hạn, coi như mới) → ghi phần còn lại. Trả 0 = key còn hạn đã có câu
   trả lời → không ghi gì, trả lại câu trả lời đó.
3. **Không có trạng thái `IN_PROGRESS`, không có `409`.** Hai request trùng chạy song song: Postgres
   bắt câu INSERT thứ hai **chờ** ở khoá chính tới khi transaction thứ nhất commit, rồi nó đi nhánh
   conflict. Không có thời điểm nào một request nhìn thấy "đang xử lý".
4. **Key theo `(tenant, người dùng, key)`**, không theo `(tenant, key)`.
5. **Kiểm tra ví (qua wallet-service) chạy TRƯỚC transaction**, không bao giờ bên trong.
6. **Gửi lại không kiểm tra lại gì** — trả nguyên văn câu trả lời đã lưu, từng byte, kèm header
   `Idempotent-Replayed: true`.
7. **Cùng key, khác nội dung → 422 `IDEMPOTENCY_KEY_REUSED`.** Nội dung so bằng SHA-256 của dạng
   chuẩn hoá (không phụ thuộc thứ tự field/khoảng trắng JSON).
8. **Không kiểm tra số dư** ở đây. Chỉ ledger, dưới `FOR UPDATE`, được quyết "đủ tiền".
9. **Mô hình lỗi RFC 7807** (`application/problem+json`) cho toàn hệ thống, giữ `errorCode` như
   trường mở rộng; lỗi 500 không trả thông điệp kỹ thuật.

## Vì sao từng điểm

- **(1–3)** Mô hình 202 cho phép điều này: phần "làm" (ghi sổ) chạy bất đồng bộ qua outbox, nên phần
  "nhận" chỉ là ghi vài hàng cục bộ — đủ ngắn để nằm trọn trong một transaction. `IN_PROGRESS` chỉ cần
  khi phần xử lý nằm NGOÀI transaction giữ key (gọi ngân hàng đồng bộ chẳng hạn); khi đó mới có cửa sổ
  "đang chạy" phải trả 409. Bỏ được trạng thái đó là bỏ được cả lớp bug "key kẹt IN_PROGRESS vì
  process chết giữa chừng".
- **(4)** Với `(tenant, key)`, hai người tình cờ dùng cùng chuỗi key thì người thứ hai bị từ chối vì
  hành động của người thứ nhất, và mọi câu trả lời khác 202 đều là một oracle "key này có người dùng
  rồi". Theo người dùng thì cả hai vấn đề biến mất — cũng là cách Stripe làm.
- **(5)** Giữ kết nối DB và khoá khoá chính idempotency trong lúc chờ HTTP là cách nhanh nhất làm cạn
  pool khi wallet-service chậm. Đổi lại có một khe hở vô hại: ví có thể đổi trạng thái giữa lúc kiểm
  và lúc commit — ledger vẫn cưỡng chế cuối.
- **(6)** Lệnh đã nhận là đã nhận. Nếu lần gửi lại kiểm tra lại và ví vừa bị đóng băng, client nhận
  lỗi và tin lệnh thất bại — trong khi lệnh gốc vẫn đang đi tới ledger.
- **(5) + lỗi** Lệnh bị từ chối ở bước kiểm tra (kể cả `503` khi wallet-service sập) **không ghi key**:
  gửi lại cùng key sau đó là đúng và được nhận. Một lỗi tạm thời không được "ghim" key vĩnh viễn.
- **(7)** Lặng lẽ trả câu trả lời của lệnh cũ cho một lệnh mới (khác số tiền) khiến client tin lệnh
  mới đã được nhận. Đây là bug phía client và phải hỏng to.
- **(9)** Một bộ xử lý lỗi cho mọi service và cho gateway. `errorCode` giữ nguyên tên field cũ nên
  client hiện có không vỡ.

## Consequences

- ✅ Không có đường nào cho ra hai lệnh ghi sổ từ một key — kể cả 20 request song song
  (`IdempotencyIT`).
- ✅ Không có trạng thái treo cần dọn, không có 409 cần client xử lý.
- ⚠️ Câu trả lời lưu nguyên văn — đổi định dạng response thì key cũ vẫn trả định dạng cũ tới khi hết
  hạn (24h). Chấp nhận được: đó chính là ngữ nghĩa "cùng câu trả lời".
- ⚠️ Bảng `idempotency_keys` lớn dần; cần job dọn key quá hạn (index `expires_at` đã có).
- ⚠️ 202 nghĩa là "đã nhận", chưa phải "đã chuyển". Trạng thái cuối (`COMPLETED`/`FAILED`) đến ở
  1.2b khi saga nhận kết quả từ ledger.

## Alternatives rejected

- **`IN_PROGRESS` + 409 + Retry-After** — cần thiết cho xử lý đồng bộ dài; ở đây chỉ thêm trạng thái
  treo và một mã lỗi client phải hiểu.
- **Khoá `(tenant, key)` + 404 cho key của người khác** — xem (4).
- **Dedup ở ledger là đủ** (ledger đã idempotent theo `transactionId`) — không: mỗi lần gửi lại sinh
  một `transactionId` mới, ledger thấy hai giao dịch khác nhau và ghi cả hai.
- **Kiểm tra số dư bằng read model trước khi nhận** — read model trễ; quyết định tiền dựa trên nó là
  sai về nguyên tắc (roadmap §5.1).

## Liên quan
`TransferService`, `IdempotencyRepository.claim`, `TransactionController`, `GlobalExceptionHandler`,
`InternalWalletController`, txn `V1__init_txn.sql`, `IdempotencyIT`, `TransferApiIT`, ADR-006
