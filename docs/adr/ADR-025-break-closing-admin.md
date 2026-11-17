# ADR-025 — Đóng break đối soát: ai đóng, ở đâu, và dấu vết

**Trạng thái:** Accepted · 2026-11-17 · roadmap 3.3 PR1
**Liên quan:** [ADR-023](ADR-023-reconciliation-internal.md) (đối soát nội bộ, *Amendments* A4 nợ API này),
[ADR-024](ADR-024-reconciliation-provider-ledger.md) §5 (hoàn tay sinh break thật, người đóng),
[ADR-021](ADR-021-admin-review-audit.md) (khu vực vận hành, audit, RBAC, quy ước B1),
[ADR-010](ADR-010-tenant-propagation.md) (mọi query lọc tenant)

## Context

Sau 3.1 PR1 và PR2a, **ba** service có bảng `reconciliation_breaks` (ledgerdb, walletdb, railsdb) và
ba job ghi vào đó. Nhưng `close()` và `findOpen()` ở cả ba repository **không có một người gọi nào**
— kể cả trong test. Không controller nào đọc hay đóng break.

Hệ quả thực tế: `docs/runbooks/reconciliation-break.md` đang bảo người trực chạy `UPDATE` bằng tay.
Thao tác đó **không để lại audit** — trái tinh thần ô DoD Phase 2 "không có thao tác đụng tiền nào
không để lại audit", và trái chính ADR-021 vốn mở đầu bằng vấn đề "sửa bằng SQL tay, không ai biết".

ADR-024 §5 làm nó gấp hơn: **mỗi lần hoàn tay qua portal VNPay đẻ một break
`PROVIDER_MONEY_NOT_POSTED` thật**, và đó là đường hoàn tiền đang dùng (sandbox chưa cho `refund`).
Break sinh ra đều đặn mà không có cách nào kết thúc.

Thêm một lỗi phát hiện khi khảo sát: **`close()` ở cả ba repository không lọc `tenant_id`**
(`WHERE id = :id AND status = 'OPEN'`) — trái ADR-010.

## Decision

### 1. Màn hình và quyền ở transaction-service; dữ liệu ở lại DB của chủ sở hữu

Người trực đã có một khu vực vận hành (`/api/v1/admin/transactions/**`), một bảng `audit_events`,
một mô hình vai (`Operator`), và một quy ước xử lý "không hỏi được" (ADR-021 B1). Dựng khu vực thứ
hai ở ba service khác nghĩa là ba bảng audit mới, ba mô hình vai, và người trực phải mở ba màn hình
để biết đêm qua có chuyện gì.

Nên: **transaction-service gom**, qua `/internal/reconciliation/breaks` (GET danh sách + POST đóng)
ở mỗi service. Break **không** bị sao chép sang txndb — chủ sở hữu dữ liệu không đổi (ADR-001).

Hệ quả phải chấp nhận: hàng đợi là **ba lời gọi mạng**. Theo đúng quy ước B1, một nguồn không hỏi
được thì ô đó là **`unavailable`**, không phải danh sách rỗng — "không có break" và "không biết có
break không" là hai câu trả lời khác nhau, và chỉ câu đầu cho phép người trực đi ngủ.

### 2. Audit ghi TRƯỚC, rồi mới đóng — không phải ngược lại

ADR-021 A4 chốt "audit **cùng transaction** với việc ghi tiền" và cài bằng `AuditWriter` với
`@Transactional(MANDATORY)`. **Ở đây không dùng được**: việc đóng nằm trong DB của service khác, sau
một lời gọi HTTP. Không có transaction nào bao được cả hai.

Khi không thể nguyên tử, phải chọn **chiều hỏng**:

| Thứ tự | Hỏng ở giữa thì sao |
|---|---|
| Đóng trước, audit sau | Break đã đóng, **không có dấu vết ai đóng**. Đúng thứ ADR-021 sinh ra để chặn. |
| **Audit trước, đóng sau** ✅ | Audit nói "đã thử đóng", break vẫn `OPEN`. Audit **thừa** một dòng. |

Chọn vế thứ hai: audit thừa đọc được và sửa được bằng mắt; audit thiếu thì vĩnh viễn không khôi phục.
Lời gọi đóng trả về xong, ghi thêm **một dòng audit kết quả** (`BREAK_CLOSED` /
`BREAK_CLOSE_FAILED`). Người đọc audit thấy cặp "ý định → kết quả", không bao giờ thấy một kết quả
không ai gây ra.

Dòng audit *ý định* dùng `AuditWriter.writeInCurrentTransaction` (MANDATORY, **không nuốt lỗi**):
audit hỏng ⇒ không đóng gì cả. Dòng *kết quả* dùng `AuditService.record` (nuốt lỗi) — tới lúc đó
việc đã xảy ra, và làm hỏng câu trả lời vì không ghi nổi dòng thứ hai là vô ích.

### 3. Không 4-eyes. ADMIN hoặc OPS, bắt buộc có lý do

Đóng break **không tạo chuyển động tiền nào**: nó ghi lại một phán đoán của con người ("đã hiểu
nguyên nhân, đã xử lý"). Thao tác đụng tiền thật vẫn đi đường cũ — `POST /resolve` có 4-eyes
(ADR-021), hoặc `REVERSAL` (ADR-022). Bắt break chờ người thứ hai lúc 3 giờ sáng là đổi một rủi ro
thật lấy một thủ tục, đúng lý lẽ V6 đã dùng cho việc mở lại ca kẹt.

Vai: `MAY_PROPOSE` = `{ADMIN, OPS}` — OPS là người trực, họ là người thực sự điều tra break.
`AUDITOR` chỉ đọc: một vai kiểm toán mà sửa được chính thứ mình kiểm toán thì mất ý nghĩa.

`reason` **bắt buộc**, không rỗng. Lý lẽ y hệt `resolution_requests.reason` (V6): một thao tác không
giải thích được thì không nên xảy ra. `ck_recon_break_closed` ở DB đã bắt buộc `closed_by` và
`closed_at`; `closed_note` thì chưa, nên tầng app cưỡng chế.

**Đóng break không sửa nguyên nhân.** Lượt đối soát sau gặp lại đúng chỗ lệch đó sẽ ghi một hàng
**mới** — `uk_recon_break_open` chỉ ràng buộc các hàng `OPEN`. Đó là chủ ý: đóng nhầm thì đêm sau nó
quay lại, chứ không biến mất vĩnh viễn.

### 4. Sửa `close()` thiếu tenant (lỗi, không phải tính năng)

Cả ba repository đóng theo mỗi `id`. Id break là UUID nên không đoán được trong thực tế, nhưng
"không đoán được" chưa bao giờ là mô hình bảo mật của ADR-010 — và cùng loại lỗi này đã phải trả một
lần ở `SagaRepository` (ADR-021 B5). Thêm `tenant_id` vào mệnh đề `WHERE` ở cả ba, mỗi service một
IT chứng minh tenant sai ⇒ **0 hàng đổi**.

## Consequences

- Người trực lần đầu kết thúc được một ca đối soát mà không cần `psql`.
- Runbook `reconciliation-break.md` mục D đổi từ câu `UPDATE` tay sang một lời gọi API.
- Audit có thể chứa dòng "đã thử đóng" mà không có kết quả — **đó là thiết kế**, không phải lỗi dữ
  liệu. Người đọc audit cần biết điều này (ghi trong runbook).
- Ba lời gọi mạng cho một lần mở hàng đợi; một service chết làm khuyết một ô, không làm hỏng cả màn.
- Chưa có: đóng hàng loạt, lọc theo loại/tenant ở API, phân trang con trỏ (hiện `limit` + cũ nhất
  trước, như `manual-review`). Bảng break nhỏ nên chưa cần.

### ⚠️ Đây là đường GHI đầu tiên trên `/internal/**` — ghi ra để không ai quên

Mọi `/internal` trước PR này đều là truy vấn (`/internal/accounts/balances` và
`/internal/entries/external-movements` là POST nhưng chỉ để mang danh sách tham số dài). Nay có một
endpoint **đổi trạng thái** ở đó, và `/internal/**` **không xác thực** — mô hình tin cậy của nó là
"tin mạng" (GAP-B5), không có route gateway nào trỏ tới.

Hệ quả phải nói thẳng: bất cứ thứ gì chạm được vào mạng cụm (kể cả một lỗ SSRF ở service khác)
đều có thể `POST .../close` với `X-Tenant-Id` + `X-User-Id` tự chế, **xoá đúng tín hiệu chứng minh
tiền đang đúng, và không để lại một dòng audit nào** — vì audit nằm ở txndb, còn đường vòng này
không đi qua đó.

Vì sao vẫn chấp nhận hôm nay: ranh giới mạng là *cùng một* ranh giới đang bảo vệ mọi đường
`/internal` khác, trong đó có những đường đọc được số dư của mọi tenant; và đóng một break không di
chuyển tiền — nó chỉ xoá một *dấu hiệu*. Nhưng nó **nâng giá trị của việc đóng ranh giới đó**
(mTLS / token nội bộ, Phase 4 GAP-B6) so với trước PR này.

Cách phát hiện nếu đã bị lạm dụng: một break `CLOSED` mà **không** có cặp
`BREAK_CLOSE_REQUESTED` / `BREAK_CLOSED` tương ứng trong `audit_events` của txndb. Runbook mô tả
phép đối chiếu này.

## Alternatives rejected

- **Mỗi service một khu vực admin riêng** — không phải gom qua HTTP, audit nằm cùng DB với việc đóng
  (nguyên tử thật). Nhưng: ba bảng `audit_events` mới, ba mô hình vai phải giữ đồng bộ, và người trực
  mở ba màn hình. Đổi một tính chất kỹ thuật lấy ba bản sao của cùng một khái niệm.
- **Sao chép break về txndb** — một hàng đợi, một transaction, audit nguyên tử. Nhưng tạo nguồn sự
  thật thứ hai cho chính thứ sinh ra để phát hiện nguồn sự thật thứ hai bị lệch.
- **Tái dùng `resolution_requests` + 4-eyes** — nhất quán với thao tác đụng tiền, nhưng bảng đó có
  `transaction_id` NOT NULL với FK tới `transactions`, trong khi break của ledger
  (`LEDGER_IMBALANCE`) không gắn với một lệnh tiền nào. Và 4-eyes cho một thao tác không đụng tiền là
  thủ tục không mua được gì (§3).
- **Tự đóng break khi lượt sau không thấy lệch nữa** — không còn hàng đợi để người trực phải đọc.
  Nhưng đó đúng là điều ADR-023 §5 cấm: break tự sinh tự đóng mỗi đêm là một bug ẩn chạy mãi.
