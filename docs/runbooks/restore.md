# Runbook — Khôi phục database từ backup

> **Trạng thái diễn tập:** ⚠️ **CHƯA DIỄN TẬP LẦN NÀO.**
> Một bản backup chưa từng restore thử thì chưa phải là backup — nó chỉ là một thư mục file.
> Hạng mục này của Phase 0 chỉ được coi là xong khi bảng "Lịch sử diễn tập" bên dưới có ít nhất
> một dòng với thời gian **đo thật**.

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
docker exec wf-postgres psql -U walletflow -d ledgerdb -c \
  "UPDATE postings SET amount_minor = 1 WHERE true;"
# Kỳ vọng: ERROR ... APPEND-ONLY. Nếu lệnh này THÀNH CÔNG -> lá chắn cuối cùng
# của sổ cái đã mất sau khi restore. Phải chạy lại V2 trước khi mở dịch vụ.
```

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

| Ngày | Người thực hiện | Dung lượng | Thời gian restore (RTO đo thật) | Ghi chú |
|---|---|---|---|---|
| _(chưa có)_ | | | | |

**Việc cần làm:** chạy một lần `backup.sh` → `restore.sh` khi chưa có sự cố, rồi điền vào bảng trên.
Cho tới lúc đó, RTO của hệ thống này là **một con số chưa ai biết**.
