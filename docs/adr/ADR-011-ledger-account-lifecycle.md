# ADR-011 — Vòng đời tài khoản sổ cái: mở qua command, trạng thái do ledger cưỡng chế

**Trạng thái:** Accepted · **Ngày:** 2026-10-16 · **Milestone:** Phase 1 (1.0b)

## Context

Sau M1, ledger ghi được bút toán nhưng **không có cách nào tạo tài khoản**: tài khoản hệ thống được
seed bằng migration, tài khoản người dùng chỉ tồn tại trong test. `LedgerService.resolveAccounts()`
ném lỗi khi gặp ref lạ. wallet-service (1.1) cần một cách để ví có tài khoản sổ cái, và roadmap yêu
cầu ví `FROZEN` bị ledger từ chối ghi nợ — nhưng `accounts` không có cột trạng thái.

Thêm vào đó, PR #17 làm tài khoản hệ thống thành per-tenant, nhưng không có gì cấp chúng cho tenant
mới.

## Decision

1. **Tài khoản chỉ ra đời qua lệnh mở.** Ledger **không** tự tạo tài khoản khi gặp ref lạ trong bút
   toán.
2. **Lệnh mở đi qua Kafka:** `wallet.command.open-account.v1` → `ledger.event.account-opened.v1`.
   wallet-service ghi ví `PENDING` + command vào outbox trong cùng transaction; ví thành `ACTIVE` khi
   nhận event.
3. **Ref phải là UUID dạng chuẩn chữ thường** (chính là wallet id). Chỉ mở được tài khoản `USER`.
4. **Idempotent tự nhiên, không qua bảng dedup:** `INSERT … ON CONFLICT DO NOTHING`, event có
   `eventId = accountId` cố định, và **mỗi** lần nhận lệnh đều phát lại event.
5. **Trạng thái nằm ở ledger** (`ACTIVE | FROZEN | CLOSED`), kiểm tra **sau** khi đã khoá hàng.
   `FROZEN` chặn ghi nợ nhưng vẫn nhận tiền; `CLOSED` chặn cả hai.
6. **`SystemAccountProvisioner`** cấp `EXTERNAL_BANK`, `FEE_INCOME`, `SETTLEMENT` × mọi currency cho
   một tenant, idempotent. Chạy lúc khởi động cho các tenant trong cấu hình; Phase 5 gọi lại từ API
   onboarding.

## Vì sao từng điểm

- **(1)** Tự tạo tài khoản nghĩa là một lệnh chuyển tới ví gõ sai mã sẽ "thành công" vào một tài
  khoản ma. Tiền mất mà không có lỗi nào được báo — loại thất bại tệ nhất.
- **(3)** `UUID.fromString` chấp nhận cả chữ hoa lẫn chữ thường, nhưng unique key so sánh chuỗi: hai
  cách viết của cùng một wallet id thành hai tài khoản, số dư của một ví bị chẻ đôi. UUID cũng không
  bao giờ va với tên tài khoản hệ thống và không chứa PII (roadmap 5.4).
- **(4)** Dedup theo `eventId` có hệ quả phụ: lệnh trùng bị bỏ qua **và không phát lại event**. Nếu
  event gốc bị mất, producer gửi lại sẽ không bao giờ có câu trả lời và ví nằm `PENDING` mãi. Mở tài
  khoản vốn đã idempotent, nên bỏ dedup không mất gì mà đổi lại được "gửi lại luôn có câu trả lời".
  (Cùng bẫy này áp cho saga retry `PostJournal` — xem roadmap 1.2b.)
- **(5)** Chỉ nơi đang giữ khoá `FOR UPDATE` trên tài khoản mới quyết được "lệnh này có được ghi nợ
  không". Trạng thái chỉ ở read model bị vượt qua bởi mọi lệnh đến trước khi read model cập nhật.
  `FROZEN` vẫn nhận tiền vì bước bù trừ của saga hoàn tiền về chính ví đó — chặn chiều này thì tiền
  kẹt ở tài khoản trung gian.
- **Lệnh mở không hợp lệ đi DLT**, không phát event "bị từ chối": không có saga nào chờ để bù trừ, và
  nó là bug phía producer — cần người xem, không cần một luồng xử lý tự động.

## Consequences

- ✅ Không có tài khoản ma; mọi tài khoản đều truy được về một lệnh mở.
- ✅ Đóng băng ví có hiệu lực ngay ở nguồn sự thật, không phụ thuộc độ trễ projection.
- ✅ Tenant mới dùng được ngay sau khi provision.
- ⚠️ Mở ví là bất đồng bộ: có khoảng thời gian ví `PENDING`. API của wallet-service phải phản ánh
  điều đó (không cho chuyển tiền tới ví `PENDING`).
- ⚠️ Chưa có command đổi trạng thái (đóng băng / đóng ví). Hiện chỉ đổi được bằng SQL; thêm cùng
  admin API ở 1.1 / 2.5, kèm audit và 4-eyes.

## Alternatives rejected

- **Tự tạo tài khoản khi gặp lần đầu** — tài khoản ma (xem trên).
- **wallet-service gọi HTTP đồng bộ sang ledger để mở tài khoản** — đơn giản hơn nhưng tạo phụ thuộc
  runtime: ledger chậm/chết thì không tạo được ví, và lại cần xử lý "gọi thành công nhưng mất phản
  hồi" — tức là tự viết lại outbox.
- **Cột `status` chỉ ở wallet-service** — vượt qua được bằng race (xem điểm 5).

## Liên quan
`AccountService`, `SystemAccountProvisioner`, `LedgerCommandHandler.openAccount`,
`LedgerService.validateAccountStatus`, ledger `V5__account_status.sql`, `AccountLifecycleIT`,
`LedgerCommandFlowIT`
