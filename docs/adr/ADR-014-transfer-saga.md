# ADR-014 — Saga chuyển tiền: ledger thắng, khoá saga trước, gửi lại bằng eventId mới

**Trạng thái:** Accepted · **Ngày:** 2026-10-17 · **Milestone:** Phase 1 (1.2b)

## Context

Sau 1.2a, một lệnh chuyển được nhận (202) và lệnh ghi sổ đi ra outbox, nhưng giao dịch dừng ở
`PROCESSING` mãi. 1.2b khép vòng: nhận câu trả lời của ledger, và xử lý ca câu trả lời **không bao giờ
tới**.

Ba sự thật về môi trường quyết định thiết kế:

1. `LedgerPosted` và `LedgerRejected` đi trên **hai topic khác nhau** — không có thứ tự giữa chúng, và
   mỗi cái có thể đến nhiều lần (at-least-once).
2. Ledger khử trùng theo `eventId` và **không phát lại** kết quả cho `eventId` đã xử lý (roadmap 1.2b,
   ADR-011 điểm 4). Nhưng ledger idempotent theo `transactionId`: lệnh mới cùng `transactionId` gặp
   bút toán đã có thì trả lại bút toán đó và phát lại `LedgerPosted`.
3. Ledger **không ghi lại lời từ chối**. Lệnh bị từ chối (thiếu tiền) rồi được gửi lại sau khi tiền về
   thì có thể được ghi sổ.

## Decision

**Máy trạng thái tường minh**, mọi chuyển đổi quyết dưới khoá hàng:

| Hiện tại | `LedgerPosted` | `LedgerRejected` | Quá hạn, hết lượt |
|---|---|---|---|
| `PROCESSING` | → `COMPLETED` | → `FAILED` | → `MANUAL_REVIEW` |
| `MANUAL_REVIEW` | → `COMPLETED` | → `FAILED` | — |
| `FAILED` | → `COMPLETED` ⚠ xung đột | bỏ qua (lặp) | — |
| `COMPLETED` | bỏ qua (lặp) | bỏ qua (đến muộn) | — |

1. **Ledger thắng.** `LedgerPosted` nghĩa là tiền đã đi; không trạng thái nào được phủ nhận điều đó,
   kể cả `FAILED`. Ca `FAILED → COMPLETED` ghi log lỗi + metric `walletflow.saga.status_conflict`.
2. **Một event kết quả cho mỗi lần chuyển trạng thái** (`txn.event.completed/failed.v1`), `eventId` tất
   định theo (loại, giao dịch). Event lặp của ledger gặp trạng thái đã đổi nên không phát lại. Không
   cần bảng khử trùng cho consumer này.
3. **Thứ tự khoá cố định: `saga_state` trước, `transactions` sau** — ở cả đường nhận câu trả lời lẫn
   job quét.
4. **Job quét saga treo** (`deadline_at` đã qua, `step_status = 'SENT'`), xuyên tenant, `FOR UPDATE SKIP
   LOCKED`, mỗi saga một transaction:
   - còn lượt: gửi lại lệnh với **cùng `transactionId`, cùng vế, `eventId` MỚI**; hạn chót kế tiếp =
     n × hạn chót;
   - hết lượt (mặc định 3): `MANUAL_REVIEW` + metric `walletflow.saga.manual_review` → alert page.
5. **Không bao giờ gửi lại saga đã đóng** (`DONE`/`FAILED`/`MANUAL_REVIEW`).
6. Một nơi duy nhất dựng lệnh ghi sổ (`LedgerCommandSender`) cho cả lần đầu lẫn lần gửi lại.

## Vì sao từng điểm

- **(1)** Ca `FAILED → COMPLETED` có thật: lệnh gốc bị từ chối nhưng lời từ chối đến muộn hơn hạn chót;
  job quét đã gửi lại; tiền về trong lúc đó; lần gửi lại được ghi sổ. Giữ `FAILED` là báo cáo nói dối
  về tiền đã thật sự di chuyển. Báo người dùng "thất bại" rồi "thành công" khó chịu nhưng đúng — và
  được cảnh báo để vận hành liên hệ người dùng nếu họ đã gửi lại lệnh mới.
- **(1) ngược lại** `LedgerRejected` đến sau `COMPLETED` bị bỏ qua: đó là lời từ chối của lệnh gốc đến
  muộn hơn bút toán của lần gửi lại.
- **(3)** Nếu nhận câu trả lời khoá giao dịch trước còn job quét khoá saga trước, hai bên chạy cùng
  lúc trên cùng giao dịch sẽ deadlock. Khoá saga trước còn có lợi thứ hai: job quét dùng `SKIP
  LOCKED`, nên saga đang được trả lời bị **bỏ qua** thay vì bị gửi lại vô ích.
- **(4) eventId mới** — gửi lại đúng `eventId` cũ khi câu trả lời đầu đã mất là saga treo vĩnh viễn:
  ledger thấy `eventId` đã xử lý và im lặng. `transactionId` giữ nguyên nên không bao giờ có bút toán
  thứ hai.
- **(4) MANUAL_REVIEW chứ không FAILED** — sau nhiều lần không có câu trả lời, ta không biết tiền đã đi
  hay chưa (bút toán có thể đã ghi, chỉ event mất). Đoán `FAILED` là có thể báo thất bại cho một giao
  dịch đã chuyển tiền; đoán `COMPLETED` là ngược lại. Câu trả lời đến muộn vẫn tự đóng ca này.
- **(4) SKIP LOCKED + một saga/transaction** — nhiều instance không gửi trùng, không đứng chờ nhau; một
  saga lỗi không kéo cả lô rollback.
- **(5)** Gửi lại một giao dịch đã `FAILED` có thể làm tiền đi **sau** khi người dùng được báo thất bại
  — đúng ca xung đột ở (1), nhưng do chính ta tự gây ra.

## Consequences

- ✅ Không giao dịch nào nằm `PROCESSING` mãi mà không ai biết: hoặc có kết quả, hoặc `MANUAL_REVIEW` +
  alert (`SagaManualReview`, runbook `saga-stuck.md`).
- ✅ Câu trả lời bị mất được tự phục hồi (`SagaFlowIT`), không có bút toán thừa.
- ⚠️ Ca xung đột `FAILED → COMPLETED` vẫn có thể xảy ra (do ledger không ghi lời từ chối). Giảm bằng
  hạn chót saga dài hơn độ trễ thật của ledger; triệt để thì ledger phải ghi lại lời từ chối — chưa làm.
- ✅ Một saga lỗi lặp lại không chặn hàng đợi: lỗi được bắt theo từng saga, saga đó bị đẩy lùi hạn chót
  và đếm vào `walletflow.saga.scan_failures` (alert `SagaScanFailing`).
- ✅ Counter của saga được đăng ký lúc khởi động (series tồn tại ở 0): counter sinh ra muộn khiến
  `increase()` coi mẫu đầu là mốc và alert bỏ lỡ lần sự cố đầu tiên — với sự cố hiếm, thường là duy nhất.
- ✅ Mở lại một ca `MANUAL_REVIEW` = đặt lại cả giao dịch lẫn saga trong một transaction (runbook, có test).
- ⚠️ Mọi event của một giao dịch tuần tự hoá trên khoá hàng saga — không đáng kể (một giao dịch chỉ có
  vài event).
- ⚠️ Chưa có giao diện quản trị cho `MANUAL_REVIEW`; hiện xử lý theo runbook (roadmap 2.5).

## Alternatives rejected

- **Bảng `processed_event` cho saga** — không cần: trạng thái giao dịch dưới khoá hàng đã là khoá khử
  trùng, và còn xử lý được thứ tự (posted/rejected) mà khử trùng theo `eventId` không xử lý được.
- **Gửi lại với cùng eventId** — saga treo vĩnh viễn khi câu trả lời đầu đã mất.
- **Tự đánh FAILED khi hết lượt** — có thể báo thất bại cho giao dịch đã chuyển tiền.
- **Giữ FAILED khi ledger ghi sổ sau** — trạng thái giao dịch mâu thuẫn với sổ cái.

## Liên quan
`SagaService`, `StuckSagaScanner`, `LedgerCommandSender`, `LedgerResultListener`, txn
`V2__saga_states.sql`, `SagaStateMachineIT`, `StuckSagaIT`, `SagaFlowIT`, runbook `saga-stuck.md`,
ADR-002, ADR-011, ADR-013
