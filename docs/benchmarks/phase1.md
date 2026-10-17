# Số liệu cơ sở Phase 1 — chuyển tiền qua gateway (roadmap 1.5b)

> Mục đích: một **mốc đối chiếu**, không phải bảng thành tích. Mọi lần "tối ưu" về sau (roadmap 4.5)
> phải chạy lại đúng kịch bản này và so với các con số dưới đây.

## Tóm tắt

- **Chịu bền được 20 TPS, không chịu được 40 TPS.** Kết luận này đúng ở cả 4 lượt.
  - 20 TPS: độ trễ end-to-end p95 3,3–3,7 s.
  - 40 TPS: p95 25–26 s; hàng đợi tăng suốt mức tải.
  - 80 TPS: p95 95–103 s ở 3 lượt; lượt A: 251 s.
- **Công suất bền vững ≈ 25 TPS, bị chặn bởi outbox relay của transaction-service.**
  - Relay chạm đúng trần cấu hình **50 bản ghi/s ở cả 4 lượt**: 100 bản ghi mỗi lần quét, quét lại 2 s
    sau khi lần trước xong.
  - Mỗi giao dịch cần 2 bản ghi outbox ở service này (`PostJournal`, rồi `txn.event.completed`), nên
    50 ÷ 2 = 25 TPS.
  - Khi quá tải, hàng đợi nằm trong bảng outbox (~4.600 bản ghi, cũ nhất ~100 s), không nằm trên Kafka
    (consumer lag gần như 0).
- **Thời gian gửi Kafka không phải nút cổ chai.** `fixedDelay` tính từ lúc lần quét trước KẾT THÚC.
  Đo được đúng 50,0 bản ghi/s = 100 bản ghi / 2,0 s, nghĩa là việc gửi 100 bản ghi (kể cả chờ ack) gần
  như không tốn thời gian. Đòn bẩy là **kích thước lô và chu kỳ quét**, không phải gửi bất đồng bộ.
- **Hệ thống không ổn định khi quá tải kéo dài.** Lượt A bị cạn pool DB của transaction-service
  (190 thread chờ kết nối). Hệ quả nối tiếp:
  1. Độ trễ nhận lệnh vọt lên p95 18 s, k6 bỏ lỡ 1.489 lệnh.
  2. Độ trễ end-to-end vượt deadline saga 2 phút.
  3. Job quét **gửi lại 350 lệnh** — tự tạo thêm tải đúng lúc đang quá tải.
- **Tiền đúng dưới tải ở cả 4 lượt** (~35.700 lệnh), kể cả lượt có 350 lần gửi lại:
  - `SUM(postings)=0` theo tenant; không tài khoản USER nào âm.
  - Mỗi lệnh COMPLETED có đúng một bút toán TRANSFER. Đây là điều constraint unique của ledger đảm bảo;
    load test xác nhận lại.
  - Tổng số dư của 41 ví seed không đổi.
  - Read model khớp sổ cái; DLT không tăng; không lệnh nào `FAILED`.

## Cấu hình đo

| | |
|---|---|
| Ngày | 2026-10-17. Code service = commit `b297940` (master sau PR #25) |
| Máy | AMD Ryzen 5 7500F (6 nhân/12 luồng), 15,7 GB RAM, Windows 11 |
| Docker | Docker Desktop 27.3.1 trên WSL2 — VM 12 CPU, 7,6 GB RAM |
| Stack | `scripts/load/up.sh`: cấu hình **production** (không áp `docker-compose.e2e.yml`, deadline saga `PT2M`), mỗi service 1 instance, Postgres/Kafka/Redis 1 node, Prometheus scrape 10 s |
| Rate limit | Giữ nguyên production: 10 rps/user ở `/api/v1/transactions`, 5 rps/IP ở `/api/auth` |
| Dữ liệu | `scripts/load/seed.sh`: 40 người gửi (mỗi ví 10.000.000 VND minor) + 1 ví merchant. DB còn dữ liệu của các lượt trước (~50.000 giao dịch) |
| Tải | `k6/transfer.js`, mô hình **mở** (`constant-arrival-rate`): 5 → 10 → 20 → 40 → 80 TPS, mỗi mức 60 s, nghỉ 10 s; mỗi lệnh 1.000 |
| `cold` | Người gửi ngẫu nhiên → người nhận ngẫu nhiên |
| `hot` | Người gửi ngẫu nhiên → **một** ví merchant (mọi bút toán khoá cùng tài khoản nhận) |

Cách đo:
- **Độ trễ nhận lệnh**: phía client, theo k6.
- **Độ trễ end-to-end** (nhận lệnh → `COMPLETED`): SQL trên `txndb` (`updated_at - created_at`), nhóm
  theo mức nhờ trường `note` của lệnh. Không poll API, vì poll ăn vào cùng hạn mức 10 rps/user và làm
  méo chính thứ đang đo.
- **Relay, lag, Hikari, CPU**: giá trị lớn nhất trong cửa sổ chạy, lấy từ Prometheus. Gauge lấy mẫu
  10 s nên đỉnh ngắn có thể bị sót.
- Mỗi lượt chỉ bắt đầu khi outbox của cả ba service rỗng, và chỉ kết thúc khi mọi lệnh đã kết thúc
  **và** outbox rỗng.

Chạy lại: `./scripts/load/up.sh && ./scripts/load/seed.sh && ./scripts/load/run.sh cold && ./scripts/load/run.sh hot`.

## Kết quả — 4 lượt

| Lượt | Tải | Lệnh COMPLETED | Tồn khi k6 dừng | Tốc độ xả sau khi k6 dừng* | Saga bị gửi lại |
|---|---|---|---|---|---|
| A `20261017T154717` | cold | 7.815 (k6 bỏ lỡ 1.489) | 2.962 | 12,4 TPS | **350** |
| B `20261017T155816` | hot | 9.305 | 3.341 | 34,7 TPS | 0 |
| C `20261017T160724` | cold | 9.304 | 3.342 | 33,2 TPS | 0 |
| D `20261017T161632` | hot | 9.305 | 3.291 | 37,0 TPS | 0 |

\* **Tốc độ xả là số TẠM THỜI, không phải công suất bền vững.**
- Outbox gửi theo FIFO. Sau khi k6 dừng, relay dành gần hết 50 bản ghi/s cho các `PostJournal` còn
  tồn. Event `txn.event.completed` sinh ra trong lúc đó xếp sau và được trả dần, nên outbox chỉ rỗng
  sau thêm ~70 s (70 s × 50 ≈ 3.500 event, khớp số lệnh tồn).
- Số trong bảng là trung vị theo bucket 10 s, đo bằng phiên bản đầu của `run.sh`. Phiên bản hiện tại
  báo tổng số lệnh ÷ thời gian xả, ghi rõ là tạm thời.

### Độ trễ end-to-end (nhận lệnh → COMPLETED), ms — p50 / p95 / p99

| Mức | A (cold) | B (hot) | C (cold) | D (hot) |
|---|---|---|---|---|
| 5 TPS | 1.914 / 7.984 / 9.684 | 2.208 / 3.088 / 3.183 | 2.424 / 10.514 / 11.934 | 2.117 / 3.016 / 3.125 |
| 10 TPS | 2.407 / 3.859 / 3.995 | 1.891 / 2.793 / 2.976 | 2.856 / 3.757 / 3.899 | 2.057 / 2.938 / 3.047 |
| 20 TPS | 2.684 / 3.617 / 3.849 | 2.224 / 3.744 / 3.986 | 2.338 / 3.348 / 3.557 | 2.017 / 3.384 / 3.833 |
| 40 TPS | 13.691 / 25.985 / 28.677 | 14.121 / 25.591 / 27.134 | 14.144 / 25.631 / 26.455 | 14.215 / 25.147 / 26.219 |
| 80 TPS | **225.150 / 251.153 / 252.712** | 57.209 / 95.315 / 98.072 | 58.529 / 103.201 / 106.088 | 76.949 / 99.189 / 102.437 |

Lưu ý khi đọc bảng:
- **Ở 40 và 80 TPS, con số phụ thuộc lịch tải.** Hàng đợi tăng suốt mức đó, và khoảng nghỉ 10 s không
  đủ xả phần dư của mức 40 trước khi mức 80 bắt đầu. Đây là độ dài hàng đợi sau N giây quá tải, không
  phải đặc tính của mức tải.
- **Mức sàn ~2–3 s ngay ở 5 TPS** (p50) đi cùng thiết kế 2 chặng outbox: transaction → ledger, rồi
  ledger → transaction. Mỗi chặng chờ relay quét theo chu kỳ 2 s. Chưa đo tách từng chặng.
- **Đuôi dài ở mức 5 TPS** của các lượt A và C (p95 8–10 s): mức đầu tiên của lượt chạy sau một lúc hệ
  thống nghỉ. Nhiều khả năng do khởi động lạnh (JIT, pool, cache), chưa xác nhận.

### Độ trễ nhận lệnh (POST → 202), ms — p50 / p95 / p99

| Mức | A (cold) | B (hot) | C (cold) | D (hot) |
|---|---|---|---|---|
| 5 TPS | 22,6 / 1.311,3 / 2.397,7 | 9,7 / 19,5 / 44,2 | 9,3 / 560,0 / 2.686,2 | 8,3 / 10,8 / 21,5 |
| 10 TPS | 13,1 / 21,6 / 82,4 | 7,3 / 9,5 / 14,7 | 6,9 / 8,8 / 12,5 | 7,1 / 12,2 / 442,2 |
| 20 TPS | 11,6 / 15,8 / 21,4 | 6,9 / 9,0 / 28,6 | 6,6 / 8,8 / 26,4 | 6,6 / 9,5 / 411,8 |
| 40 TPS | 7,7 / 10,2 / 16,2 | 6,4 / 11,3 / 149,1 | 6,3 / 8,1 / 12,1 | 6,4 / 7,9 / 11,6 |
| 80 TPS | 12,6 / **18.258,8 / 25.584,2** | 5,8 / 8,3 / 167,0 | 5,9 / 9,8 / 94,5 | 6,0 / 9,6 / 152,6 |

Tỉ lệ 202 = 100% ở mọi mức, không có 429. Thiết kế 202 + bất đồng bộ (ADR-013) giữ bước nhận lệnh ở
~6–13 ms (p50) ngay cả khi hàng đợi phía sau dài hàng phút — trừ lượt A.

### Prometheus — giá trị lớn nhất trong lượt

| Chỉ số | A | B | C | D |
|---|---|---|---|---|
| Relay publish — transaction-service (bản ghi/s) | 50,0 | 50,0 | 50,0 | 50,0 |
| Relay publish — ledger-service (bản ghi/s) | 50,0 | 42,9 | 41,9 | 49,5 |
| Outbox chờ gửi — transaction-service | 3.222 | 4.679 | 4.684 | 4.557 |
| Tuổi outbox cũ nhất — transaction-service | 199 s | 97 s | 100 s | 99 s |
| Consumer lag — ledger-service (records) | **366** | 0 | 0 | 62 |
| Hikari chờ kết nối — transaction-service | **190** | 9 | 0 | 0 |
| Hikari đang dùng — transaction-service (pool 10) | 10 | 10 | 3 | 10 |
| CPU — transaction-service (0–1, đã chia 12 nhân) | 0,26 | 0,02 | 0,02 | 0,03 |

## Phân tích

**Trần của relay** (`common-lib/.../outbox/OutboxRelay.java`, `readers/JpaOutboxReader.java`)
- Cấu hình hiện tại:
  - `@Scheduled(fixedDelay = 2000)`, chỉnh được qua `outbox.relay.delay-ms`.
  - Mỗi lần lấy tối đa **100** bản ghi; con số này hard-code trong `JpaOutboxReader`.
  - Gửi tuần tự `send().get()` trong một transaction DB.
- Cả 4 lượt đều chạm đúng 50,0 bản ghi/s, tức 100 bản ghi trong 2,0 s. Vì `fixedDelay` tính từ lúc lần
  trước xong, phần gửi 100 bản ghi — kể cả chờ Kafka ack và ghi DB — gần như không tốn thời gian.
- Trần vì thế do **kích thước lô ÷ chu kỳ quét** quyết định. Gửi bất đồng bộ theo lô sẽ hầu như không
  nâng được trần này.
- Mỗi giao dịch cần 2 bản ghi ở transaction-service → trần bền vững ~25 TPS, khớp với "chịu 20, gãy 40".

**Vì sao ví nóng ≈ ví nguội**
- Ledger chạy **1 thread consumer** (không cấu hình `concurrency`), nên không bao giờ có hai bút toán
  cùng tranh khoá tài khoản merchant.
- Tải cũng không tới được ledger nhanh hơn trần relay.
- Số liệu này **không** nói gì về tranh chấp khoá. Khi relay được nâng trần và ledger chạy nhiều
  thread, phải đo lại kịch bản `hot`.

**Pool kết nối của transaction-service**
- Pool 10 kết nối bị dùng hết ở 3/4 lượt (A, B, D). Có thread phải chờ ở A (190) và B (9).
- Pool là tài nguyên căng nhất của service này khi tải cao, nhưng chỉ lượt A thì nó gây sụp đổ.

**Lượt A — suy biến khi quá tải**
- Pool cạn nặng: 190 thread chờ. CPU transaction-service lên ~3 nhân (0,26 × 12), ledger-service có
  consumer lag 366.
- Bước nhận lệnh nghẽn theo: ở 80 TPS p95 18 s, k6 hết VU và bỏ lỡ lệnh.
- Độ trễ end-to-end lên 225–253 s, **vượt deadline saga `PT2M`**, nên `StuckSagaScanner` gửi lại 350
  lệnh — thêm tải vào đúng lúc hệ thống đang ngộp.
- Các lần gửi lại đều được ledger trả lời idempotent: mỗi lệnh vẫn đúng một bút toán, tổng tiền giữ
  nguyên. Tính đúng không bị ảnh hưởng, chỉ có độ trễ.
- Nguyên nhân gốc của đợt cạn pool nặng **chưa xác định** (một lượt đo trước đó từng thấy 53 thread
  chờ). Ứng viên:
  - các luồng dùng kết nối đồng thời: nhận lệnh, relay, consumer kết quả từ ledger, job quét;
  - autovacuum/checkpoint của Postgres;
  - Docker Desktop/WSL2 nghẽn I/O.

**Hàng đợi khi quá tải**
- Lệnh vẫn được nhận ngay rồi nằm chờ trong outbox. Không mất lệnh nào và không lệnh nào lỗi.
- Không có back-pressure: ở 80 TPS người dùng nhận 202 rồi chờ 1,5–4 phút mới thấy `COMPLETED`.
- Alert `OutboxStalled` / `OutboxBacklogHigh` là tín hiệu đúng; `SagaRetriesHigh` bắt được kiểu lượt A.

## Hạn chế của số đo

1. **Không có histogram server-side.** Không bật `management.metrics.distribution.percentiles-histogram`,
   nên percentile chỉ có phía client (k6) và SQL. Panel p95 trên Grafana và alert `LatencyP95High` hiện
   không có dữ liệu.
2. **`walletflow.outbox.published` không có nhãn topic**, nên không tách được relay gửi bao nhiêu
   `PostJournal` và bao nhiêu `txn.event.completed` trong từng pha.
3. **Không còn "retry optimistic lock" để đo** (roadmap ghi trước đây). Ledger và wallet dùng khoá bi
   quan (`FOR UPDATE`). Retry của Kafka `DefaultErrorHandler` không có counter; DLT không tăng ở mọi
   lượt.
4. **Một máy, một instance mỗi service, Docker Desktop trên Windows.** Con số tuyệt đối chỉ để so với
   chính nó qua thời gian. Biến động giữa các lượt lớn (lượt A), nên mỗi lần so sánh cần ≥ 2 lượt mỗi
   kịch bản.
5. **Gateway khởi động trước auth-service thì token hợp lệ đầu tiên bị 401.** Lần nạp JWKS lúc khởi
   động hỏng và gateway giữ 0 khoá; khoá chỉ được nạp lại sau khi một request mang `kid` lạ bị từ chối
   (`JwksKeyLocator.find`). `scripts/load/up.sh` chờ qua giai đoạn này.
6. **Các lượt đo đầu tiên trong ngày bị loại**, vì hai lỗi phương pháp đã sửa:
   - Throughput tính trên cả lượt (bị lịch tải chặn trên).
   - Một lượt bắt đầu khi outbox của lượt trước chưa xả.

## Việc nên làm tiếp (đã đưa vào roadmap 4.5 — làm SAU, có số trước/sau)

1. **Nâng trần relay:** cho cấu hình kích thước lô (đang hard-code 100) và rút chu kỳ quét
   (`outbox.relay.delay-ms`). Đo lại xem trần mới nằm ở đâu: DB, Kafka ack hay consumer. Thêm nhãn
   topic cho `walletflow.outbox.published`.
2. **Chống retry storm:** job quét không nên gửi lại lệnh vẫn còn nằm trong outbox, tức chưa từng rời
   service.
3. **Ledger consumer `concurrency`** = số partition, rồi đo lại `hot`.
4. Tìm nguyên nhân cạn pool Hikari ở transaction-service (`pg_stat_activity`, thời gian giữ kết nối
   của từng luồng).
5. Bật percentile histogram cho `http.server.requests`.
6. Gateway: gặp `kid` lạ thì chờ nạp JWKS (có timeout) thay vì 401 ngay.
