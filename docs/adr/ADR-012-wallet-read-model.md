# ADR-012 — Read model ví: khử trùng theo (ví, bút toán), dựng lại bằng "khoá rồi mới đọc"

**Trạng thái:** Accepted · **Ngày:** 2026-10-16 · **Milestone:** Phase 1 (1.1)

## Context

wallet-service giữ số dư và lịch sử giao dịch của ví như một **read model** suy ra từ sổ cái
(ADR-004, roadmap §5.1). Nguồn cập nhật là `ledger.event.posted.v1` — giao **at-least-once**, và
**không có thứ tự** giữa các ví: event được partition theo ví bị ghi nợ, nên với lệnh chuyển A→B,
vế của B đi trên partition của A.

Roadmap gốc đề xuất: `processed_event` (khử trùng theo event) + `@Version` + Spring Retry +
`last_applied_event_id`, và một endpoint rebuild "đặt lại số dư từ ledger". Đọc kỹ thì có ba vấn đề:

1. **Khử trùng theo event vỡ khi một event chạm hai ví.** Dựng lại ví A phải đánh dấu "bút toán E đã
   áp cho A" để event E đến muộn không bị cộng hai lần. Đánh dấu theo event thì cũng chặn luôn E áp
   cho B — và B mất tiền vĩnh viễn trong read model.
2. **Rebuild "đặt balance = SUM(ledger)" cộng đôi event đang bay**, hoặc ngược lại làm mất event,
   tuỳ thứ tự khoá/đọc.
3. `@Version` + Retry bảo vệ một read-modify-write **không tồn tại**: số dư đổi bằng
   `UPDATE … SET balance = balance + :delta`, vốn đã atomic.

## Decision

1. **Bảng lịch sử `wallet_entries` CHÍNH LÀ bảng khử trùng**, khoá `UNIQUE (wallet_id, entry_id)`.
   Áp một event = với mỗi ví nó chạm: `INSERT … ON CONFLICT DO NOTHING`; chỉ khi chèn được hàng mới
   thì `UPDATE balance = balance + delta`. Hai thao tác trong cùng một transaction.
2. **Projection chỉ cộng delta, không bao giờ gán số dư** → giao hoán, thứ tự event không quan trọng.
3. **Trạng thái ví áp theo `statusVersion`** (`UPDATE … WHERE status_version < :v`) — không cần bảng
   khử trùng cho `AccountOpened`, và bản cũ đến muộn không ghi đè được bản mới.
4. **Rebuild = KHOÁ VÍ → ĐỌC SAO KÊ LEDGER → đối chiếu**: gỡ dòng thừa/sai, chèn dòng thiếu, đặt số dư
   = tổng sao kê, áp trạng thái nếu mới hơn — một transaction.
5. **Projector cũng khoá ví trước** (`SELECT … FOR UPDATE ORDER BY id`) rồi mới đụng `wallet_entries`
   — cùng trình tự với rebuild, nên hai bên không deadlock nhau.
6. Ledger có **API nội bộ chỉ đọc** `GET /internal/accounts/{ref}` trả `AccountStatement`; mọi thay
   đổi sổ cái vẫn chỉ qua Kafka.
7. **Ví `PENDING` quá hạn được gửi lại lệnh mở tài khoản** bởi một job nền — an toàn vì ledger trả lời
   idempotent (ADR-011).

## Vì sao "khoá rồi mới đọc" đúng

Với ví đã khoá, mọi bút toán chạm ví rơi vào một trong hai nhóm, và đều được tính **đúng một lần**:

| Bút toán | Điều gì xảy ra |
|---|---|
| Có trong sao kê | Rebuild chèn hàng lịch sử của nó. Projector (đang chờ khoá) chèn trượt → không cộng. |
| Ghi sổ **sau** lần đọc | Không có trong sao kê. Projector áp nó sau khi rebuild nhả khoá → cộng lên trên. |

Đảo thứ tự (đọc rồi mới khoá): một bút toán ghi sổ sau lần đọc nhưng được projector áp **trước** khi
rebuild khoá → bị ghi đè mất, và vì hàng lịch sử của nó đã có nên **không bao giờ** được áp lại.

## Consequences

- ✅ Không có cách nào cộng đôi hay mất một bút toán — kể cả khi event lặp, sai thứ tự, song song, hay
  đang bay lúc rebuild. Mỗi tình huống có một test (`WalletProjectionIT`, `WalletRebuildIT`); riêng
  thứ tự "khoá rồi mới đọc" có test chen một bút toán vào **đúng lúc** rebuild đang đọc sổ cái — đảo
  thứ tự là test đỏ. Test chống deadlock (A→B và B→A song song) mang tính xác suất: nó bắt được hồi
  quy trong thực tế, nhưng không phải chứng minh.
- ✅ Đối soát nội bộ miễn phí: `SUM(wallet_entries) = balance_minor` cho mọi ví.
- ✅ `processed_event` không cần cho projection; giữ lại cho consumer sau này dùng `EventDeduplicator`.
- ⚠️ Mọi event của một ví tuần tự hoá trên khoá hàng ví. Đủ cho Phase 1; ví cực nóng là bài toán
  Phase 4 (cùng loại với nghẽn `EXTERNAL_BANK`, GAP-A6).
- ⚠️ Rebuild gọi HTTP sang ledger **trong** transaction đang giữ khoá ví → bắt buộc timeout ngắn
  (2s/5s). Ledger treo thì rebuild thất bại (503), ví không bị khoá lâu.
- ⚠️ **Sao kê chưa phân trang.** Với ví có lịch sử rất dài, rebuild có thể chạm timeout ở mọi lần thử
  — và read-timeout 5s tính cho từng lần đọc socket, không cho cả response, nên khoá ví có thể bị giữ
  lâu hơn con số đó. Chấp nhận ở Phase 1 (rebuild là công cụ quản trị hiếm); khi ví lớn thì cần sao kê
  theo mốc (checkpoint số dư + các dòng sau mốc).
- ⚠️ Thứ tự lịch sử là thứ tự **ghi vào read model** (`seq`), không phải thứ tự ghi sổ; dòng được
  rebuild chèn bù sẽ hiện ở đầu danh sách. Đổi lại phân trang cursor không bao giờ xô lệch.
- ⚠️ API nội bộ của ledger tin header `X-Tenant-Id` vì tin mạng — cùng giới hạn GAP-B5, Phase 4 thay
  bằng mTLS.

## Alternatives rejected

- **`processed_event` theo event** — vỡ ở event chạm hai ví (xem Context 1).
- **`last_applied_event_id` / gán số dư theo thứ tự** — không có thứ tự toàn cục cho một ví để dựa vào.
- **wallet-service đọc thẳng `ledgerdb` để rebuild** — phá ADR-001, và biến schema của ledger thành
  hợp đồng ngầm.
- **Rebuild bằng cách replay topic từ đầu** — retention 7 ngày, và chậm tỉ lệ với lịch sử toàn hệ
  thống thay vì một ví.

## Liên quan
`WalletProjector`, `WalletRebuildService`, `PendingWalletReconciler`, `InternalAccountController`,
wallet `V1__init_wallet.sql`, `WalletProjectionIT`, `WalletRebuildIT`, `WalletApiIT`, `WalletFlowIT`
