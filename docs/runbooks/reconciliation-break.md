# Runbook — `LedgerImbalance` / `ReconciliationBreakOpen` / `ReconciliationStale`

> Ba alert, ba mức độ khác nhau, **một bảng để nhìn**: `reconciliation_breaks`.
>
> - `LedgerImbalance` — **page.** Sổ cái tự mâu thuẫn. Tiền tự sinh hoặc bốc hơi.
> - `ReconciliationStale` — **page.** Không ai đang kiểm tiền. Hệ thống trông sạch vì không ai nhìn.
> - `ReconciliationBreakOpen` — **ticket.** Có chỗ lệch chờ người kết luận.
>
> Bối cảnh thiết kế: [ADR-023](../adr/ADR-023-reconciliation-internal.md), roadmap 3.1.

## Luật bất di bất dịch

1. **Không bao giờ `UPDATE` bảng `postings` / `journal_entries`.** Chúng append-only và có trigger DB
   chặn — nếu bạn nghĩ ra cách lách, đó là lúc dừng lại. Sai sổ cái sửa bằng một bút toán `REVERSAL`
   ([ADR-022](../adr/ADR-022-ledger-reversal-operation.md)), qua `POST /resolve` có 4-eyes.
2. **Không bao giờ `UPDATE wallets.balance_minor` bằng tay.** Có đường đúng:
   `POST /api/v1/admin/wallets/{id}/rebuild` dựng lại từ sổ cái.
3. **Break không tự đóng, và bạn cũng đừng đóng nó chỉ vì số đã đúng lại.** Với tầng wallet, máy đã
   tự rebuild (`auto_repaired = true`) — số dư đúng rồi, nhưng *cái bug làm nó lệch* thì chưa ai tìm.
   Đóng break = tuyên bố đã hiểu nguyên nhân.

---

## A. `LedgerImbalance` — sổ cái lệch (page)

### Triệu chứng
`walletflow_ledger_imbalance_minor > 0`; log ledger-service có dòng `ĐỐI SOÁT ĐỎ:`.

### Chẩn đoán — 5 phút

**1. Lệch ở đâu, bao nhiêu:**

```bash
docker exec wf-postgres psql -U walletflow -d ledgerdb -c \
  "SELECT tenant_id, type, subject, expected_minor, actual_minor, delta_minor, times_seen, first_seen_at
     FROM reconciliation_breaks WHERE status = 'OPEN' ORDER BY first_seen_at;"
```

| `type` | Nghĩa |
|---|---|
| `LEDGER_IMBALANCE` | Tổng của một nhóm `(tenant, currency)` khác 0 — sổ của **một khách** sai. `subject` là mã loại tiền. |
| `LEDGER_IMBALANCE_GLOBAL` | Tổng toàn hệ thống khác 0 trong khi từng nhóm vẫn cân. `tenant_id` là sentinel toàn số 0. Nghĩa là có vế trỏ vào **tài khoản đã mất hoặc lệch tenant** nên bị `JOIN` loại khỏi phép kiểm theo nhóm. |

**2. Tìm bút toán không cân** (phép kiểm theo nhóm chỉ nói nhóm nào, không nói bút toán nào):

```bash
docker exec wf-postgres psql -U walletflow -d ledgerdb -c \
  "SELECT j.id, j.transaction_id, j.type, j.created_at, SUM(p.amount_minor) AS lech
     FROM journal_entries j JOIN postings p ON p.entry_id = j.id
    WHERE j.tenant_id = '<tenant>'
    GROUP BY j.id, j.transaction_id, j.type, j.created_at
   HAVING SUM(p.amount_minor) <> 0
    ORDER BY j.created_at;"
```

**3. Nếu bước 2 không ra gì** → lệch nằm ở các vế **mồ côi**, đúng ca `LEDGER_IMBALANCE_GLOBAL`:

```bash
docker exec wf-postgres psql -U walletflow -d ledgerdb -c \
  "SELECT p.id, p.entry_id, p.account_id, p.tenant_id, p.amount_minor
     FROM postings p LEFT JOIN accounts a ON a.id = p.account_id AND a.tenant_id = p.tenant_id
    WHERE a.id IS NULL;"
```

### Xử lý

| Tình huống | Hành động |
|---|---|
| Một bút toán không cân, xác định được đúng số tiền | `REVERSAL` bút toán đó qua `POST /resolve` (4-eyes), rồi ghi lại bút toán đúng bằng lệnh Kafka bình thường. |
| Vế mồ côi (tài khoản không tồn tại / lệch tenant) | **Không xoá vế.** Đây là bug ở đường ghi — tìm nó trước. Mở issue ⚠️ chặn release, đính kèm `entry_id`. |
| Không giải thích được | **Leo thang ngay.** Lệch sổ cái không có cách "chờ xem". Dừng mọi deploy đụng ledger. |

Sau khi xử lý, đóng break (xem mục D).

---

## B. `ReconciliationBreakOpen` — read model lệch (ticket)

### Chẩn đoán

```bash
docker exec wf-postgres psql -U walletflow -d walletdb -c \
  "SELECT tenant_id, type, subject AS wallet_id, expected_minor, actual_minor, delta_minor,
          auto_repaired, times_seen, first_seen_at, detail
     FROM reconciliation_breaks WHERE status = 'OPEN' ORDER BY first_seen_at;"
```

| `type` | Nghĩa | `auto_repaired` |
|---|---|---|
| `PROJECTION_SELF_MISMATCH` | `balance_minor` ≠ tổng `wallet_entries` của chính ví. Lỗi projector hoặc một câu `UPDATE` gõ tay. | `true` — đã rebuild |
| `LEDGER_MISMATCH` | `balance_minor` ≠ `SUM(postings)` bên ledger. Ví lệch khỏi **nguồn sự thật**. | `true` — đã rebuild |
| `LEDGER_ACCOUNT_MISSING` | Ví `ACTIVE` có số dư nhưng ledger **không có tài khoản** cho nó. Tiền không nằm trong sổ cái. | `false` — không có sổ để chép về |

### Xử lý

**`PROJECTION_SELF_MISMATCH` / `LEDGER_MISMATCH`** — số dư đã đúng lại rồi, việc của bạn là tìm *vì
sao*. Theo thứ tự:

1. `times_seen` lớn (lệch lặp lại) → projector đang có bug, không phải sự cố một lần. Chặn release.
2. Xem DLT của `ledger.event.posted.v1` — event bị rơi là nguyên nhân thường gặp nhất
   ([`dlt-not-empty.md`](dlt-not-empty.md)).
3. Đối chiếu `wallet_entries` với sao kê ledger của đúng ví đó:
   `GET /internal/accounts/{walletId}?currency=VND` trên ledger-service.
4. `delta_minor` đúng bằng một bút toán → tìm bút toán đó, xem nó có trong `wallet_entries` không.

**`LEDGER_ACCOUNT_MISSING`** — nặng hơn: một ví đang hiển thị tiền mà sổ cái không biết.

1. Ví này có `status = ACTIVE` nhưng ledger không có tài khoản ⇒ `AccountOpened` đã được áp vào read
   model trong khi tài khoản bên ledger không tồn tại (hoặc đã bị tạo ở tenant khác).
2. **Đừng rebuild** — rebuild sẽ thấy sổ rỗng và có thể quét số dư về 0, trong khi ta chưa biết tiền
   đó có thật hay không.
3. Kiểm tra `wallet_entries` của ví: có `entry_id` nào không? Có → tra từng `entry_id` bên ledgerdb.
   Không → số dư đó chưa bao giờ có bút toán nào, tức là nó được ghi bằng tay.
4. Leo thang. Đây là ca duy nhất ở tầng 2 có thể là **tiền thật bị sai**, không chỉ hiển thị sai.

---

## C. `ReconciliationStale` — job đối soát chết (page)

```bash
docker exec wf-postgres psql -U walletflow -d ledgerdb -c \
  "SELECT kind, window_start, status, started_at, finished_at, checked_count, break_count, failure_message
     FROM reconciliation_runs ORDER BY window_start DESC LIMIT 10;"
# rồi lặp lại với -d walletdb
```

| Thấy gì | Nghĩa | Làm gì |
|---|---|---|
| Hàng `RUNNING` bỏ lại | Lượt đó chết giữa chừng (process bị giết, OOM). **Cửa sổ đó không tự chạy lại** — đúng thiết kế. | Xem log quanh `started_at`. Lượt kế tiếp (cửa sổ sau) chạy bình thường. **Restart service KHÔNG ép quét lại được**: cửa sổ tính bằng `floor(now / interval)` nên restart trong cùng giờ (tầng 1) / cùng ngày (tầng 2) vẫn ra đúng cửa sổ đã bị chiếm, và lượt mới bỏ qua im lặng. Thật sự cần quét ngay thì đổi `window_start` của chính hàng `RUNNING` đó sang một giá trị cũ rồi restart. |
| Hàng `FAILED` | Lỗi kỹ thuật, `failure_message` nói rõ. Hay gặp nhất: wallet-service không gọi được ledger. | Sửa nguyên nhân kỹ thuật. |
| Không có hàng nào gần đây | Job không chạy. | Kiểm `walletflow.{ledger,wallet}.reconciliation.enabled` — có ai tắt nó không? Kiểm service còn sống không. |
| `absent(...)` trong alert | Metric biến mất hẳn — service không khởi động được, hoặc Prometheus không scrape được. | [`service-down.md`](service-down.md) |

> Cờ `enabled: false` trong production là một quyết định phải có người ký. Tắt đối soát = quay lại
> đúng GAP-A3: sai tiền mà không ai biết.

---

## D. Đóng một break

Chỉ đóng khi **đã hiểu nguyên nhân** và đã xử lý (hoặc đã mở issue có chủ sở hữu). Hiện chưa có API
đóng break — đóng bằng SQL, và ghi `closed_by` là **id người thật**, không phải một UUID tuỳ ý:

```bash
docker exec wf-postgres psql -U walletflow -d walletdb -c \
  "UPDATE reconciliation_breaks
      SET status = 'CLOSED', closed_at = now(), closed_by = '<user-id>',
          closed_note = '<nguyên nhân + số issue>', updated_at = now()
    WHERE id = '<break-id>' AND status = 'OPEN';"
```

`ck_recon_break_closed` cưỡng chế: đóng mà thiếu `closed_by` hoặc `closed_at` thì câu lệnh bị từ chối.

> **Nợ đã biết:** API đóng break + màn hình cho người trực thuộc 3.3 (công cụ vận hành). Tới lúc đó
> thao tác này là SQL tay — và vì vậy nó **không** để lại `audit_events`. Ghi lại vào issue.

## Leo thang

1. Trực ca → 2. Chủ sở hữu ledger-service → 3. Trưởng nhóm.
`LedgerImbalance` leo thẳng bước 2, không chờ hết ca.
