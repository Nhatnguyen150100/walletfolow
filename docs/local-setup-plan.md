# WalletFlow — Plan triển khai & phát triển tại máy local

> Tài liệu thực thi. Mỗi bước có lệnh chạy được và cách **tự kiểm chứng** là nó đã đúng.
> Bám theo [`walletflow-production-roadmap.md`](walletflow-production-roadmap.md) nhưng thu về phạm vi
> một máy.
>
> **Ngày khảo sát môi trường:** 2026-10-16

---

## 0. Hiện trạng máy bạn (đã đo thật, không phải giả định)

| Thành phần | Thực tế | Đánh giá |
|---|---|---|
| Java | OpenJDK **21.0.12** | ✅ Đúng yêu cầu |
| Maven | **3.6.3** (apt Ubuntu), **không có wrapper** | 🟠 Vừa đủ mức tối thiểu của Boot 3.5; Dockerfile lại dùng 3.9 → host và image build khác version |
| Docker | **29.5.3** | ✅ |
| Docker Compose | **v5.1.4** | ✅ |
| CPU | 12 cores | ✅ |
| Disk | còn 98 GB | ✅ |
| **RAM** | 15 GB tổng — **đã dùng 10 GB, chỉ còn ~2,9 GB khả dụng** | 🔴 **Chặn cứng** |
| **Port 8888** | đang bị **node (pid 18589)** chiếm | 🔴 config-server không bind được |
| Container khác đang chạy | `edvpath-mysql`, `edvpath-firebase-1` | 🟠 Đang ăn RAM |
| Volume `walletflow_pgdata` | **đã tồn tại, đủ 5 DB** (`authdb`, `walletdb`, `ledgerdb`, `txndb`, `railsdb`) | ✅ Không cần init lại |
| Image đã build | `config-server`, `discovery-server`, `ledger-service` | 🟠 Thiếu `auth-service`, `api-gateway` |
| Build offline | `mvn -o package -DskipTests` → **EXIT 0** | ✅ Đã verify, `~/.m2` 1,7 GB đủ dependency |

### Hai điều về code cần biết trước khi chạy

1. **`ledger-service` KHÔNG có REST controller.** Nó chỉ nhận lệnh qua Kafka topic
   `txn.command.post-journal.v1`. Muốn test tay thì phải **bắn message vào Kafka**, không có endpoint nào.
2. **Không có cách nào tạo tài khoản ví (`type = USER`) ngoài SQL.** `V1__init_ledger.sql` chỉ seed
   3 tài khoản hệ thống (`EXTERNAL_BANK`, `FEE_INCOME`, `SETTLEMENT`). Việc tạo ví sẽ thuộc về
   `wallet-service` ở Phase 1. **Hiện tại phải `INSERT` tay** — plan dưới đây có sẵn script.

---

## 1. Chọn chế độ chạy — quyết định quan trọng nhất

Với **2,9 GB RAM khả dụng**, chạy cả 11 container là không khả thi (full stack cần ~7 GB). Dùng 3 chế độ,
mỗi chế độ cho một mục đích:

| Chế độ | Gồm gì | RAM | Dùng khi nào |
|---|---|---|---|
| **A — Lite** ⭐ *mặc định hằng ngày* | Docker: `postgres` + `kafka` + `redis`.<br>Host JVM: service đang code (`mvn spring-boot:run`) | **~1,3 GB** | 95% thời gian. Vòng lặp sửa–chạy tính bằng **giây** |
| **B — Full** | Toàn bộ `docker-compose.yml` | ~7 GB | Khi cần verify "demo được", trước khi tag release. **Phải tắt `edvpath-*` trước** |
| **C — Test** | Không cần gì chạy sẵn; Testcontainers tự dựng Postgres+Kafka rồi tự dọn | ~1,5 GB lúc chạy | `mvn verify` — chạy trước mỗi commit |

> **Vì sao chế độ A là mặc định, không phải B:** `application.yml` của mọi service đã dùng
> `${DB_HOST:localhost}`, `${KAFKA_BROKERS:localhost:9092}` — tức là **chạy ngoài Docker không cần sửa
> một dòng config nào**. Đồng thời sửa code xong chỉ cần restart JVM (~5 giây) thay vì
> `docker compose build` (~2 phút). Thiết kế config của dự án đã sẵn sàng cho chế độ này.

> ⚠️ Chế độ A và C **không chạy cùng lúc** được một cách thoải mái về RAM. Chạy `mvn verify` thì
> nên `docker compose -f docker-compose.infra.yml down` trước.

---

# PHẦN A — Ngày 1: Dọn đường và lên được (~3 giờ)

## A1. Giải phóng tài nguyên · 10 phút

```bash
# Tắt stack của dự án khác (dữ liệu vẫn còn trong volume, bật lại sau)
docker stop edvpath-mysql edvpath-firebase-1

# Dọn container đã chết từ lâu
docker container prune -f

# Kiểm tra lại
free -h | head -2        # kỳ vọng: available tăng lên ~5-6 GB
```

**Về port 8888** (đang bị `node` pid 18589 chiếm): **không kill process đó** — nhiều khả năng là dev
server của dự án khác. Thay vào đó:
- **Chế độ A:** không chạy `config-server` → không có xung đột. Xong.
- **Chế độ B:** đổi port trong `docker-compose.yml` thành `"8889:8888"` (chỉ đổi port host, bên trong
  container vẫn 8888 nên không phải sửa config service nào).

**Kiểm chứng:** `ss -ltn | grep -E ':(5432|9092|6379|8080|8083) '` không ra gì → các port cần dùng đều rảnh.

## A2. Tạo `docker-compose.infra.yml` · 20 phút

File mới ở gốc repo. Đây là bản rút gọn của compose hiện có, **chỉ hạ tầng**, dùng **cùng volume**
`walletflow_pgdata` (để không mất 5 DB đã tạo):

```yaml
# Hạ tầng tối thiểu cho chế độ A. Service chạy bằng JVM trên host.
#   docker compose -f docker-compose.infra.yml up -d
name: walletflow          # giữ nguyên tên project -> dùng lại volume walletflow_pgdata

services:
  postgres:
    image: postgres:16
    container_name: wf-postgres
    environment:
      POSTGRES_USER: walletflow
      POSTGRES_PASSWORD: walletflow
      POSTGRES_DB: walletflow
    ports: ["5432:5432"]
    volumes:
      - ./deploy/postgres/init.sql:/docker-entrypoint-initdb.d/init.sql
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U walletflow"]
      interval: 10s
      timeout: 5s
      retries: 5
    deploy:
      resources: { limits: { memory: 512M } }

  kafka:
    image: apache/kafka:3.9.1
    container_name: wf-kafka
    ports: ["9092:9092"]
    environment:
      KAFKA_NODE_ID: "0"
      KAFKA_PROCESS_ROLES: "controller,broker"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "0@kafka:9093"
      # Khác bản full: thêm listener nội bộ để tiến trình trên HOST kết nối được
      KAFKA_LISTENERS: "PLAINTEXT://:9092,CONTROLLER://:9093"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://localhost:9092"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"       # Phase 0 sẽ tắt
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1"
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: "0"
      KAFKA_HEAP_OPTS: "-Xmx512m -Xms256m"
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list || exit 1"]
      interval: 15s
      timeout: 10s
      retries: 10

  redis:
    image: redis:7
    container_name: wf-redis
    ports: ["6379:6379"]
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  pgdata:
    external: true
    name: walletflow_pgdata
```

> ⚠️ **Điểm dễ sai nhất:** `KAFKA_ADVERTISED_LISTENERS` ở bản full là `PLAINTEXT://kafka:9092` —
> đúng khi client nằm trong mạng Docker, nhưng **sai** khi service chạy trên host: Kafka sẽ bảo
> client "hãy kết nối tới `kafka:9092`", host không phân giải được tên đó và client treo rồi timeout.
> Ở chế độ A phải là `localhost:9092`.

**Kiểm chứng:**
```bash
docker compose -f docker-compose.infra.yml up -d
docker compose -f docker-compose.infra.yml ps     # cả 3 phải healthy
docker exec wf-postgres psql -U walletflow -c '\l' | grep -c db   # kỳ vọng >= 5
```

## A3. Profile `local` cho service · 20 phút

Tạo `services/ledger-service/src/main/resources/application-local.yml`:

```yaml
# Profile chạy ngoài Docker: không cần discovery/config server.
spring:
  cloud:
    config:
      enabled: false        # tắt hẳn, không chỉ 'optional' -> khởi động nhanh hơn, log sạch
eureka:
  client:
    enabled: false          # chạy một instance, không cần danh bạ
logging:
  level:
    com.walletflow: DEBUG   # thấy được log outbox relay + dedup
    org.hibernate.SQL: DEBUG
```

Làm tương tự cho `auth-service` khi cần chạy.

**Chạy:**
```bash
mvn -pl services/ledger-service -am spring-boot:run -Dspring-boot.run.profiles=local
```

**Kiểm chứng:**
```bash
curl -s localhost:8083/actuator/health | jq
# {"status":"UP", ...}  -> Flyway đã migrate, Kafka listener đã lên
```

## A4. Maven wrapper · 15 phút

Host dùng Maven 3.6.3, Dockerfile dùng 3.9 — hai build khác môi trường là nguồn lỗi "máy tôi chạy được".

```bash
mvn -N wrapper:wrapper -Dmaven=3.9.9
./mvnw -v          # kỳ vọng: Apache Maven 3.9.9
```

Sửa `.gitignore`: dòng `!.mvn/wrapper/maven-wrapper.jar` đang tồn tại nhưng **không có dòng
`.mvn/` nào bị ignore trước đó** → dòng phủ định đó vô nghĩa. Thay bằng:

```gitignore
.mvn/wrapper/maven-wrapper.jar     # tải lại bằng ./mvnw, không commit binary
```

Từ đây **mọi lệnh dùng `./mvnw`**, không dùng `mvn`.

## A5. Chạy test Testcontainers · 30 phút

```bash
docker compose -f docker-compose.infra.yml down    # nhường RAM cho Testcontainers
./mvnw -pl common-lib,services/ledger-service -am verify
```

**Kỳ vọng:** `MoneyTest`, `LedgerServiceIT`, `OutboxRelayIT`, `LedgerCommandFlowIT` xanh.

Nếu lỗi `client version 1.32 is too old`: pom đã có `docker.api.version=1.44`, Docker server của bạn là
29.5.3 nên không nên gặp. Nếu vẫn gặp: `./mvnw verify -Ddocker.api.version=1.47`.

Nếu lỗi hết RAM: chạy tách từng module (`-pl common-lib` trước, rồi `-pl services/ledger-service`).

> **Đây là mốc quan trọng nhất của Ngày 1.** Test xanh nghĩa là double-entry, append-only trigger,
> idempotency, dedup và outbox đều **thực sự hoạt động** trên máy bạn — không phải tin vào README.

---

# PHẦN B — Ngày 1: Smoke test chứng minh M1 sống (~1 giờ)

Test tự động đã chứng minh code đúng. Phần này chứng minh **hệ thống đang chạy** đúng — và quan trọng
hơn, cho bạn công cụ để nhìn thấy luồng tiền bằng mắt.

Bật lại chế độ A (`docker compose -f docker-compose.infra.yml up -d` + chạy ledger với profile `local`).

## B1. Seed tài khoản ví · `scripts/seed-accounts.sql`

```sql
-- Hiện chưa có service nào tạo được tài khoản USER (sẽ là việc của wallet-service ở Phase 1).
INSERT INTO accounts (id, external_ref, type, currency, created_at, updated_at)
VALUES (gen_random_uuid(), 'WALLET-ALICE', 'USER', 'VND', now(), now()),
       (gen_random_uuid(), 'WALLET-BOB',   'USER', 'VND', now(), now())
ON CONFLICT (external_ref, currency) DO NOTHING;
```

```bash
docker exec -i wf-postgres psql -U walletflow -d ledgerdb < scripts/seed-accounts.sql
```

## B2. Nạp 100.000đ vào ví Alice

Message là JSON của `EventEnvelope<PostJournalCommand>`, **phải nằm trên một dòng**
(console producer cắt message theo dấu xuống dòng):

```bash
TXN=$(uuidgen); EVT=$(uuidgen)
echo "{\"eventId\":\"$EVT\",\"eventType\":\"PostJournal\",\"version\":1,\"occurredAt\":\"2026-10-16T10:00:00Z\",\"traceId\":null,\"payload\":{\"transactionId\":\"$TXN\",\"type\":\"TOPUP\",\"referenceId\":null,\"currency\":\"VND\",\"legs\":[{\"accountRef\":\"EXTERNAL_BANK\",\"amountMinor\":-100000},{\"accountRef\":\"WALLET-ALICE\",\"amountMinor\":100000}]}}" \
| docker exec -i wf-kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic txn.command.post-journal.v1

echo "TXN=$TXN"   # ghi lại để dùng ở B4
```

## B3. Bốn phép kiểm chứng

```bash
# 1) Bút toán đã vào sổ, đúng 2 vế
docker exec wf-postgres psql -U walletflow -d ledgerdb -c "
  SELECT j.type, a.external_ref, p.amount_minor
  FROM postings p
  JOIN journal_entries j ON p.entry_id = j.id
  JOIN accounts a        ON p.account_id = a.id
  ORDER BY j.created_at DESC, p.amount_minor LIMIT 4;"

# 2) ⭐ BẤT BIẾN VÀNG: tổng mọi posting toàn hệ thống = 0
docker exec wf-postgres psql -U walletflow -d ledgerdb -c \
  "SELECT SUM(amount_minor) AS phai_bang_0 FROM postings;"

# 3) Số dư suy ra từ sổ cái
docker exec wf-postgres psql -U walletflow -d ledgerdb -c "
  SELECT a.external_ref, COALESCE(SUM(p.amount_minor),0) AS balance_minor
  FROM accounts a LEFT JOIN postings p ON p.account_id = a.id
  GROUP BY a.external_ref ORDER BY 1;"

# 4) Outbox đã publish -> Kafka
docker exec wf-postgres psql -U walletflow -d ledgerdb -c \
  "SELECT event_type, topic, msg_key, published, published_at FROM outbox_event ORDER BY created_at DESC LIMIT 3;"
```

**Kỳ vọng:** `EXTERNAL_BANK = -100000` (âm là **đúng** — đó là lượng tiền đã đi vào hệ thống từ bên
ngoài), `WALLET-ALICE = +100000`, tổng = **0**, outbox có `published = true`.

Và xem event đi ra:
```bash
docker exec wf-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic ledger.event.posted.v1 --from-beginning --timeout-ms 5000
```

## B4. Ba kịch bản chứng minh tính chất khó — phần đáng giá nhất

**(a) Dedup tầng messaging** — gửi lại **y nguyên** message ở B2 (cùng `eventId`):
→ log hiện `bỏ qua message trùng eventId=...`, **không** có bút toán mới.

**(b) Idempotency tầng ledger** — gửi lại với **`eventId` MỚI** nhưng **giữ nguyên `transactionId`**:
→ qua được dedup, nhưng `LedgerService` thấy `(transactionId, type)` đã tồn tại → **trả về bút toán cũ**,
vẫn **không** có bút toán mới. *Đây là hai lớp bảo vệ khác nhau, và chỉ kịch bản này phân biệt được chúng.*

**(c) Từ chối nghiệp vụ** — bắn bút toán **lệch** (`-100000` và `+90000`):
```bash
TXN2=$(uuidgen); EVT2=$(uuidgen)
echo "{\"eventId\":\"$EVT2\",\"eventType\":\"PostJournal\",\"version\":1,\"occurredAt\":\"2026-10-16T10:05:00Z\",\"traceId\":null,\"payload\":{\"transactionId\":\"$TXN2\",\"type\":\"TRANSFER\",\"referenceId\":null,\"currency\":\"VND\",\"legs\":[{\"accountRef\":\"WALLET-ALICE\",\"amountMinor\":-100000},{\"accountRef\":\"WALLET-BOB\",\"amountMinor\":90000}]}}" \
| docker exec -i wf-kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic txn.command.post-journal.v1
```
→ không có bút toán nào được ghi, và `ledger.event.rejected.v1` có event với
`errorCode` tương ứng. **Tổng vẫn = 0.**

**(d) Append-only** — thử sửa sổ cái bằng SQL trực tiếp:
```bash
docker exec wf-postgres psql -U walletflow -d ledgerdb -c \
  "UPDATE postings SET amount_minor = 999999 WHERE true;"
# Kỳ vọng: ERROR ... APPEND-ONLY: không được UPDATE ...
```

## B5. Đóng gói thành `scripts/smoke.sh`

Gộp B1–B4 thành một script tự kiểm tra, in `PASS/FAIL` cho từng tính chất và **exit code khác 0 nếu
sai**. Lý do: Phase 0 sắp tới sẽ sửa khá nhiều thứ nền (Kafka error handler, JWT, Flyway) — cần một
lệnh duy nhất trả lời "tôi có vừa làm hỏng luồng tiền không?" trong 30 giây.

---

# PHẦN C — Ngày 2: Dựng vòng lặp phát triển (~4 giờ)

## C1. Script tiện ích `scripts/wf` · 2 giờ

Một CLI nhỏ gói lại những lệnh bạn sẽ gõ hàng trăm lần:

| Lệnh | Việc |
|---|---|
| `wf up` / `wf down` | bật/tắt hạ tầng chế độ A |
| `wf run <service>` | `./mvnw -pl services/<service> -am spring-boot:run -Dspring-boot.run.profiles=local` |
| `wf psql <db>` | vào psql của database đó |
| `wf balance` | in bảng số dư + bất biến SUM |
| `wf produce <topic> <file.json>` | bắn message (tự nén JSON về 1 dòng) |
| `wf consume <topic>` | theo dõi topic |
| `wf topics` | liệt kê topic + consumer lag |
| `wf outbox` | xem hàng outbox chưa gửi |
| `wf reset` | TRUNCATE dữ liệu nghiệp vụ, seed lại (**chỉ dùng ở local**) |
| `wf smoke` | chạy `smoke.sh` |

> Về `wf reset`: nhớ là trigger append-only chặn `DELETE` nhưng **không** chặn `TRUNCATE` — đây là
> chủ ý đã ghi trong `V2__append_only_and_event_id.sql`. Dùng `TRUNCATE ... CASCADE`.

## C2. Dev loop nhanh · 1 giờ

- Thêm `spring-boot-devtools` (scope `runtime`, `optional`) vào service đang code → tự restart khi
  class đổi. **Chỉ thêm cho service nghiệp vụ**, đừng thêm cho gateway.
- Chạy service bằng IntelliJ Run Configuration với profile `local` → debug breakpoint được, đây là
  cách nhanh nhất để hiểu luồng Saga sắp viết ở Phase 1.
- `logging.level.com.walletflow=DEBUG` ở profile `local` — bạn sẽ thấy được nhịp outbox relay 2 giây
  một lần, rất hữu ích để cảm nhận tính bất đồng bộ.

## C3. Seed dữ liệu phong phú hơn · 1 giờ

`scripts/seed-dev.sql`: ~5 ví, mỗi ví có sẵn số dư qua các bút toán TOPUP hợp lệ (**không** INSERT
thẳng vào `postings` — làm vậy là tự phá bất biến, và trigger sẽ cho qua vì nó chỉ chặn UPDATE/DELETE).
Cách đúng: seed bằng cách **bắn command qua Kafka** trong script.

---

# PHẦN D — Tuần 1–2: Làm Phase 0 ngay tại local

Đây là Phase 0 của roadmap, sắp xếp lại theo thứ tự **làm được ở local** và kèm cách kiểm chứng local.
Ưu tiên từ trên xuống; nếu phải cắt thì cắt từ dưới lên.

| # | Việc | Ngày | Kiểm chứng tại local |
|---|---|---|---|
| **D1** | **DLT thật** — `KafkaErrorHandlingConfig` trong `common-lib`, `DefaultErrorHandler` + `DeadLetterPublishingRecoverer`, `addNotRetryableExceptions` cho lỗi parse | 1,5 | Bắn message JSON hỏng → `wf consume txn.command.post-journal.v1.DLT` thấy nó, **và** message hợp lệ gửi ngay sau đó vẫn xử lý được (không kẹt partition) |
| **D2** | **Khai báo topic tường minh** (`KafkaAdmin` + `NewTopic`), tắt `KAFKA_AUTO_CREATE_TOPICS_ENABLE` ở cả 2 file compose | 0,5 | Bắn vào topic gõ sai tên → **bị từ chối**, không âm thầm tạo topic mới |
| **D3** | **CI thật** `.github/workflows/ci.yml` | 2 | Chạy local bằng [`act`](https://github.com/nektos/act) trước khi push, hoặc push lên branch nháp. Gỡ phần README mô tả CI chưa tồn tại |
| **D4** | **JWT bất đối xứng** — RS256, JWKS, refresh token xoay vòng, xoá mọi secret mặc định | 2 | `scripts/gen-keys.sh` sinh keypair vào `.env` (đã gitignore); **bỏ biến môi trường ra → service phải chết lúc khởi động**, không được chạy với default |
| **D5** | **Flyway cho auth-service**, bỏ `ddl-auto: update` | 0,5 | Xoá `authdb`, khởi động lại → schema dựng đủ bằng migration; `grep -r "ddl-auto: update"` rỗng |
| **D6** | **Siết gateway** — strip `X-User-*` trên mọi path, tắt discovery locator ở profile prod, bỏ `ports: 8083` của ledger khỏi compose mặc định | 0,5 | `curl -H 'X-User-Id: 999' localhost:8080/api/auth/...` → downstream **không** thấy giá trị 999 |
| **D7** | **Outbox**: metric `pending`/`oldest_age`, job dọn, partial index | 1 | Tắt Kafka 1 phút, bắn 5 command → `wf outbox` thấy tồn đọng, metric tăng; bật lại → tự gửi hết |
| **D8** | **Dashboard + alert** Grafana/Prometheus commit vào repo | 1,5 | Chạy chế độ B, mở `localhost:3000`, panel có số thật. Gây lỗi chủ động → alert `DltNotEmpty` bắn |
| **D9** | **Backup + diễn tập restore** | 0,5 | `pg_dump` → xoá volume → restore → `SUM(postings) = 0` và số dư khớp. **Ghi lại thời gian thật** vào `docs/runbooks/restore.md` |
| **D10** | **Viết nốt 8 ADR** | 0,5 | — |

> **Thứ tự nếu chỉ có 3 ngày:** D1 → D4 → D3. D1 bịt lỗ mất lệnh tiền, D4 bịt lỗ chiếm quyền, D3
> ngăn cả hai tái diễn.

---

# PHẦN E — Sau Phase 0: khởi động Phase 1 tại local

Khi bắt đầu `wallet-service`, môi trường local cần thêm:

1. **`docker-compose.infra.yml` giữ nguyên** — wallet-service chỉ cần Postgres + Kafka đã có.
2. **Chạy 2 service cùng lúc trên host**: ledger (8083) + wallet (8082). RAM mỗi JVM ~300 MB với
   `-Xmx256m` → vẫn thoải mái ở chế độ A.
3. **Thêm vào `wf`**: lệnh `wf balance-compare` so `wallets.balance_minor` với `SUM(postings)` —
   đây là tiền thân của job đối soát ở Phase 3, và bạn sẽ dùng nó liên tục khi debug projection.
4. **Việc đầu tiên của wallet-service nên là API tạo ví** — gỡ bỏ sự phụ thuộc vào
   `scripts/seed-accounts.sql` ở mục B1.

---

## Definition of Done — môi trường local

- [ ] `docker compose -f docker-compose.infra.yml up -d` → 3 container healthy trong < 60 giây
- [ ] `./mvnw verify` xanh toàn bộ, kể cả Testcontainers IT
- [ ] `wf smoke` chạy trong < 30 giây và in PASS cho cả 6 tính chất (bút toán, SUM=0, số dư, outbox, dedup, append-only)
- [ ] Sửa một dòng code → thấy kết quả trong < 10 giây
- [ ] Chế độ B chạy được ít nhất một lần (chứng minh demo được)
- [ ] `docs/runbooks/restore.md` có thời gian restore **đo thật**
- [ ] `git status` sạch — không có secret, không có file build

---

## Những cái bẫy đã biết (đọc trước khi mất 2 tiếng)

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| Service trên host treo rồi timeout khi kết nối Kafka | `KAFKA_ADVERTISED_LISTENERS` trỏ `kafka:9092`, host không phân giải được | Dùng `localhost:9092` trong `docker-compose.infra.yml` (đã xử lý ở A2) |
| `init.sql` không chạy, thiếu database | Postgres **chỉ** chạy `/docker-entrypoint-initdb.d` khi volume **rỗng** | Volume của bạn đã có đủ 5 DB. Nếu cần làm lại: `docker volume rm walletflow_pgdata` |
| Testcontainers lỗi `client version is too old` | docker-java chào API 1.32 | Pom đã set `docker.api.version=1.44`; override bằng `-Ddocker.api.version=1.47` |
| Message bắn vào Kafka nhưng ledger không phản ứng | JSON bị xuống dòng → console producer cắt thành nhiều message hỏng | Luôn nén JSON về **một dòng** (`jq -c`) |
| Bút toán báo `Account not found` | Ví `USER` chưa được seed | Chạy `scripts/seed-accounts.sql` |
| Gửi lại message mà không thấy gì xảy ra | Đúng như thiết kế — dedup theo `eventId` | Muốn test idempotency tầng ledger thì đổi `eventId`, **giữ** `transactionId` |
| OOM khi `mvn verify` | Chế độ A đang chạy song song với Testcontainers | `docker compose -f docker-compose.infra.yml down` trước khi verify |
| Build trên host khác build trong Docker | Maven 3.6.3 vs 3.9 | Dùng `./mvnw` (mục A4) |

---

*Cập nhật tài liệu này mỗi khi gặp bẫy mới — phần "bẫy đã biết" là thứ tiết kiệm thời gian nhất về lâu dài.*
