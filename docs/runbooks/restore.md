# Runbook — Khôi phục database từ backup

> **Trạng thái diễn tập:** ✅ **ĐÃ DIỄN TẬP 2026-10-16 — RTO đo được: 4 giây.**
> Xem bảng "Lịch sử diễn tập" ở cuối file.

## Bối cảnh rủi ro

Hiện tại cả 5 database nằm trong **một** container Postgres với **một** volume
(`walletflow_pgdata`), **không replica, không PITR**. Mất volume đó là mất toàn bộ sổ cái — nguồn
sự thật về tiền, thứ không dựng lại được từ bất kỳ đâu khác.

Hạ tầng HA + PITR là hạng mục **Phase 4** (`walletflow-production-roadmap.md` §4.2). Cho tới lúc
đó, `scripts/backup.sh` là biện pháp phòng vệ **duy nhất**.

## Sao lưu

```bash
./scripts/backup.sh                 # -> ./backups/<timestamp>/
```

Nên đặt cron hằng ngày:

```cron
0 2 * * * cd /home/nhatnx/my-project/walletflow && ./scripts/backup.sh >> /var/log/wf-backup.log 2>&1
```

Mỗi bản gồm: `00-globals.sql` (role & quyền), một `.dump` cho mỗi database, và `MANIFEST.txt`
(thời điểm, git commit, phiên bản Postgres).

> `pg_dumpall --globals-only` là phần hay bị bỏ sót. Restore mà thiếu role thì database dựng xong
> vẫn không ai kết nối được — một bản backup "thành công" nhưng vô dụng, và chỉ phát hiện ra đúng
> vào lúc đang có sự cố.

## Khôi phục

```bash
./scripts/restore.sh ./backups/20261016-120000
```

Script sẽ hỏi xác nhận trước khi ghi đè.

## ⭐ Kiểm chứng sau khi restore — bắt buộc

Restore "thành công" mà sổ cái lệch thì **tệ hơn** không restore, vì ta sẽ tin vào dữ liệu sai.

```bash
# 1. Bất biến vàng: tổng mọi posting = 0
docker exec wf-postgres psql -U walletflow -d ledgerdb -c \
  "SELECT SUM(amount_minor) AS phai_bang_0 FROM postings;"

# 2. Số bút toán khớp với kỳ vọng (so với số ghi lại trước sự cố)
docker exec wf-postgres psql -U walletflow -d ledgerdb -c \
  "SELECT count(*) FROM journal_entries;"

# 3. Số dư từng tài khoản
docker exec wf-postgres psql -U walletflow -d ledgerdb -c "
  SELECT a.external_ref, COALESCE(SUM(p.amount_minor),0) AS balance_minor
  FROM accounts a LEFT JOIN postings p ON p.account_id = a.id
  GROUP BY a.external_ref ORDER BY 1;"

# 4. Flyway: schema đúng phiên bản
docker exec wf-postgres psql -U walletflow -d ledgerdb -c \
  "SELECT version, description, success FROM flyway_schema_history ORDER BY installed_rank;"

# 5. Trigger append-only còn sống (pg_restore có thể bỏ sót function/trigger)
#    HỎI THẲNG CATALOG, đừng dựa vào một lệnh UPDATE.
docker exec wf-postgres psql -U walletflow -d ledgerdb -tAc \
  "SELECT c.relname||' -> '||t.tgname FROM pg_trigger t
     JOIN pg_class c ON c.oid = t.tgrelid
    WHERE NOT t.tgisinternal ORDER BY 1;"
# Kỳ vọng ĐÚNG 2 dòng:
#   journal_entries -> trg_journal_entries_append_only
#   postings        -> trg_postings_append_only
```

> ⚠️ **Đừng kiểm tra trigger bằng `UPDATE postings ... WHERE true`.** Trigger là `FOR EACH ROW`,
> nên trên một bảng RỖNG nó không bao giờ kích hoạt: lệnh trả về `UPDATE 0` và trông hệt như
> "đã bị chặn". Một bản restore mất sạch trigger sẽ **pass** phép kiểm tra đó.
> *(Đây chính là cái bẫy đã vấp phải trong lần diễn tập 2026-10-16 — phép kiểm tra cũ ghi ở đây
> cho ra kết quả dương tính giả.)*
>
> Muốn kiểm bằng DML thì phải có ít nhất một hàng thật:
> ```bash
> docker exec wf-postgres psql -U walletflow -d ledgerdb -c "
>   INSERT INTO journal_entries(id,transaction_id,type,created_at)
>     VALUES (gen_random_uuid(), gen_random_uuid(), 'TOPUP', now());
>   UPDATE journal_entries SET type='TRANSFER' WHERE true;"
> # Kỳ vọng: ERROR ... APPEND-ONLY
> # Dọn: TRUNCATE journal_entries, postings;  (TRUNCATE không kích hoạt trigger FOR EACH ROW)
> ```

Bước 5 là bước dễ quên nhất và nguy hiểm nhất: sổ cái vẫn còn đủ dữ liệu, nhưng tính bất biến —
thứ làm nên giá trị của nó — đã biến mất, một cách hoàn toàn im lặng.

## Dữ liệu mất giữa hai bản backup

Backup hằng ngày nghĩa là **RPO tối đa 24 giờ**. Phần giao dịch giữa bản backup cuối và thời điểm
sự cố là mất thật.

Cách giảm thiệt hại trong lúc chưa có PITR:
- Topic Kafka giữ 7 ngày (xem `KafkaTopicsAutoConfiguration`) → có thể replay
  `txn.command.post-journal.v1` từ mốc thời gian backup để dựng lại các bút toán đã mất.
  Ledger idempotent theo `(transactionId, type)` nên replay chồng lấn là **an toàn**.
- Đây chính là lý do retention của topic lệnh tiền được khai tường minh chứ không để theo mặc định
  của broker.

## Lịch sử diễn tập

| Ngày | Dung lượng | RTO đo thật | Kết quả |
|---|---|---|---|
| 2026-10-16 | 52 KB (5 DB, dữ liệu dev) | **4 giây** | ✅ Đạt — chi tiết bên dưới |

### Diễn tập 2026-10-16

**Kịch bản:** tạo dữ liệu thật (1 user qua `POST /api/auth/register`, 3 tài khoản hệ thống trong
ledger) → `backup.sh` → **cố ý phá** (`DELETE FROM users`, `DELETE FROM accounts WHERE type='SYSTEM'`)
→ `restore.sh` → kiểm chứng.

| Kiểm chứng | Kết quả |
|---|---|
| `users` quay về | ✅ 1 hàng, đúng `drill@walletflow.test` |
| `accounts` quay về | ✅ 3 → 0 → **3** |
| `SUM(postings)` | ✅ **0** |
| `flyway_schema_history` ledgerdb | ✅ V1, V2, V3 đều `success=true` |
| `flyway_schema_history` authdb | ✅ V1 `success=true` |
| Trigger append-only (3 cái) | ✅ còn đủ, và chặn thật khi thử `UPDATE` trên hàng thật |
| Partial index outbox | ✅ `idx_outbox_unpublished`, `idx_outbox_published_cleanup` còn |

**Hai thứ học được:**

1. **Phép kiểm tra trigger cũ trong runbook này cho dương tính giả.** `UPDATE postings WHERE true`
   trên bảng rỗng trả `UPDATE 0` và trông như đã bị chặn — trong khi trigger có thể đã mất sạch.
   Đã sửa thành truy vấn `pg_trigger`. Xem cảnh báo ở mục kiểm chứng phía trên.
2. **`pg_dumpall --globals-only` in `ERROR: role "walletflow" already exists`** khi restore vào một
   instance đã có sẵn role. Vô hại (psql đi tiếp), nhưng dòng ERROR giữa một quy trình khôi phục
   là thứ làm người vận hành hoảng đúng lúc không nên hoảng. `restore.sh` nay lọc riêng trường hợp này.

**Cảnh báo về con số 4 giây:** đây là RTO với **52 KB dữ liệu dev**. Nó chứng minh *quy trình* chạy
đúng, **không** chứng minh thời gian khôi phục ở quy mô thật. Phải đo lại khi ledger có hàng triệu
posting — và ở quy mô đó, `pg_restore` một file dump sẽ không còn là chiến lược đúng nữa; đó là lý do
Phase 4 chuyển sang PITR.
