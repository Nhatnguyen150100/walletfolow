# CLAUDE.md — WalletFlow

> File này được nạp vào MỌI phiên — giữ ngắn và đúng sự thật. Tài liệu dài nằm ở `docs/`
> và `.claude/skills/`, chỉ đọc khi cần.

## Dự án làm gì
Ví điện tử microservices (fintech). **Sổ cái double-entry (ledger-service) là nguồn sự thật duy nhất
về tiền**; wallet-service là read model số dư (CQRS) dựng lại được từ ledger; transaction-service nhận
lệnh chuyển tiền idempotent rồi điều phối Saga (orchestration) qua Kafka. Client đi qua api-gateway
(verify JWT RS256 qua JWKS của auth-service, gắn header `X-User-*`, rate limit bằng Redis).
Đang ở **Phase 2** (Phase 1 đã đóng — `docs/PR-phase-1.md`); định vị sản phẩm: ADR-015. Trạng thái từng
mục: `docs/walletflow-production-roadmap.md`.

| Module | Cổng | Package gốc |
|---|---|---|
| `common-lib` | — | `com.walletflow.common` (Money, outbox, dedup, Kafka error/DLT, tenant, ProblemDetail) + `com.walletflow.autoconfigure` |
| `infrastructure/{discovery-server,config-server,api-gateway}` | 8761 / 8888 / 8080 | `com.walletflow.{discovery,config,gateway}` |
| `services/auth-service` | 8081 | `com.walletflow.auth` |
| `services/wallet-service` | 8082 | `com.walletflow.wallet` |
| `services/ledger-service` | 8083 | `com.walletflow.ledger` |
| `services/transaction-service` | 8084 | `com.walletflow.transaction` |

## Stack
- Java 21 (CI) · Spring Boot 3.5.0 · Spring Cloud 2025.0.0 · **Maven multi-module** qua `./mvnw` (không có Gradle).
- PostgreSQL 16, **DB-per-service**; Spring Data JPA + native query (`@Modifying` INSERT cho các bước giành khoá idempotency). Flyway riêng từng service: `services/<svc>/src/main/resources/db/migration`. `ddl-auto: validate`, `open-in-view: false`.
- Kafka (apache/kafka 3.9.1), topic khai báo tường minh trong `WalletTopics` (`<domain>.<command|event>.<tên>.v<N>`), auto-create TẮT.
- Lombok được dùng: `@Getter`, `@NoArgsConstructor` (entity), `@RequiredArgsConstructor`, `@Slf4j`, `@Builder`. Không dùng `@Data`/`@EqualsAndHashCode`/`@ToString` trên entity.
- REST: code-first với springdoc (không có `openapi.yaml`). **Hợp đồng event Kafka** là JSON Schema trong `common-lib/src/main/resources/schemas/` + `EventContractTest`.
- Test: JUnit 5, AssertJ, Testcontainers (Postgres 16, Kafka), JaCoCo gate trên **integration test** (ledger/wallet/transaction 85%, auth 70%). Không có Spotless/ArchUnit.

## Lệnh
Máy local chạy JDK 23 → **luôn thêm `-Dmaven.compiler.proc=full`** (không thì Lombok không chạy, hàng trăm lỗi `cannot find symbol`). Integration test cần Docker Desktop đang chạy.

| Việc | Lệnh |
|---|---|
| Compile nhanh 1 service | `./mvnw -B -ntp -q -o "-Dmaven.compiler.proc=full" -pl services/<svc> -am test-compile` |
| Cài common-lib vào local repo (sau khi sửa common-lib) | `./mvnw -B -ntp -q "-Dmaven.compiler.proc=full" -pl common-lib install -DskipTests` |
| Một IT (sau khi đã install common-lib) | `./mvnw -B -ntp "-Dmaven.compiler.proc=full" -pl services/<svc> verify -Dit.test=IdempotencyIT -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false -Djacoco.skip=true` |
| Một method IT | như trên với `-Dit.test=IdempotencyIT#gui_lai_tra_ve_dung_cau_tra_loi_cu` |
| Toàn bộ 1 service (kèm coverage gate) | `./mvnw -B -ntp "-Dmaven.compiler.proc=full" -pl services/<svc> -am verify` |
| Toàn bộ như CI | `./mvnw -B -ntp "-Dmaven.compiler.proc=full" clean verify` |
| Chạy local (Lite) | `docker compose -f docker-compose.infra.yml up -d && set -a && source .env && set +a && ./mvnw -pl services/<svc> spring-boot:run -Dspring-boot.run.profiles=local` |

Chạy test hẹp nhất trước (1 IT → 1 service), chỉ chạy toàn bộ trước khi commit.
Lưu ý: chạy một IT lẻ phải `-Djacoco.skip=true`, không thì gate 85% đánh trượt.

## Cấu trúc trong một service
```
com/walletflow/<svc>/
  controller/      # REST, chỉ map HTTP <-> service; đọc user từ header AuthHeaders.USER_ID
  dto/             # record request/response (+ static of(entity))
  entity/          # JPA entity, không setter nếu hàng chỉ được INSERT
  enums/           # tiền tố E: ETransactionStatus, EWalletStatus...
  repositories/    # Spring Data (auth-service dùng repository/, service/ — giữ nguyên tên hiện có)
  services/        # nghiệp vụ + ranh giới @Transactional
  listener/        # @KafkaListener — mỏng, gọi sang services/
  client/          # HTTP client sang service khác, sau một interface (WalletDirectory, LedgerClient)
  config/
```
Không có tầng domain riêng; logic nằm ở `services/`. Code dùng chung giữa service đặt ở `common-lib`.

## Bất biến (vi phạm = chặn merge — roadmap §5)
1. Ledger là nguồn sự thật duy nhất về tiền. Mọi số dư khác là read model, phải dựng lại được từ ledger.
2. Mọi lệnh ghi tiền idempotent theo khoá tường minh (`Idempotency-Key` ở HTTP, `eventId` ở Kafka).
3. **Mọi dual-write đi qua `OutboxPublisher`.** Cấm `kafkaTemplate.send` trong `services/` (CI guardrail chặn).
4. Mỗi tính chất về tiền phải có một test chứng minh (IT trên Postgres/Kafka thật).
5. Schema chỉ đổi bằng Flyway, expand → backfill → contract.
6. Không bí mật nào trong repo, kể cả default cho dev. Thiếu env thì service phải KHÔNG khởi động.
7. Phân loại lỗi trước khi xử lý: lỗi nghiệp vụ → từ chối dứt khoát; lỗi kỹ thuật → retry → DLT.
8. Mỗi quyết định kiến trúc = 1 ADR trong `docs/adr/` (đánh số tiếp, hiện tới ADR-015).

## Quy ước code
- Tiền: `long` minor unit + value object `Money` (ADR-005), cột `BIGINT`. Không bao giờ `double`/`float`; `BigDecimal` chỉ khi tính tỉ lệ, làm tròn với `RoundingMode` tường minh. Cộng/trừ qua `Money` (`addExact`).
- Multi-tenant: mọi bảng nghiệp vụ có `tenant_id`; lấy tenant bằng `TenantContext.require()`; mọi query lọc theo tenant (ADR-010).
- Danh tính: user lấy từ header gateway (`AuthHeaders`), không bao giờ tin body. Tải theo id phải kiểm quyền sở hữu; "không tồn tại" và "của người khác" trả **cùng** một lỗi (chống dò id).
- Lỗi: ném `AppException` hoặc lớp con (`BadRequestException`, `ResourceNotFoundException`, `ConflictException`...) với `errorCode` ổn định → `GlobalExceptionHandler` (common-lib) trả RFC 7807 `ProblemDetail`. Không trả 200 kèm body lỗi; 500 không lộ message kỹ thuật.
- Kafka consumer phải idempotent: `EventDeduplicator.markProcessed(eventId, consumer)` trong cùng transaction với ghi DB. Message hỏng → `PoisonMessageException` → DLT.
- Đổi event: sửa JSON Schema + fixture trong common-lib; thay đổi phá vỡ → topic/schema `.v2`, không sửa `.v1`.
- Không gọi HTTP/Kafka bên trong transaction DB (xem `TransferService`: kiểm tra ví ngoài tx, rồi `TransactionTemplate`).
- Constructor injection (`final` + `@RequiredArgsConstructor` hoặc constructor tay khi cần `@Value`). DTO là `record`; validate bằng Jakarta Validation + `@Valid`, message tiếng Việt.
- Log SLF4J có placeholder; không log token, mật khẩu, khoá, dữ liệu cá nhân.
- Comment, javadoc, message lỗi, log: **tiếng Việt**, giải thích *tại sao*. Giữ indent của file đang sửa (common-lib 2 dấu cách, services 4 dấu cách).

## Database
- Không bao giờ sửa migration đã commit (checksum). Thêm `V<next>__<snake_case>.sql` trong đúng service.
- Migration phải tương thích ngược với bản app đang chạy (expand/contract).
- FK mới / cột WHERE mới cần index. Bảng ledger là append-only (trigger DB chặn UPDATE/DELETE) — đừng thiết kế quanh việc sửa posting.
- Xem skill `spring-jpa-guardrails` cho N+1, locking, batching.

## Test
- Sửa bug = viết test fail trước, rồi mới sửa. Với đường tiền: kiểm chứng test thật sự FAIL khi phá bất biến (mutation check thủ công).
- Phần lớn test là `*IT` (`@SpringBootTest` + Testcontainers, chạy bởi failsafe). Không H2. Service mới **phải** khai `maven-failsafe-plugin` trong pom, không thì `*IT` bị bỏ qua im lặng.
- Phụ thuộc HTTP sang service khác thay bằng fake trong `src/test/.../support/` (`FakeWalletDirectory`, `FakeLedger`), không mock.
- Đặt tên: method snake_case tiếng Việt + `@DisplayName` tiếng Việt mô tả hành vi, ví dụ `gui_lai_tra_ve_dung_cau_tra_loi_cu`.
- Không bao giờ xoá, nới lỏng, `@Disabled` test đang fail để build xanh. Nếu test sai, nói rõ và giải thích.

## Cách làm việc
- Việc chạm > ~3 file: khám phá trước (subagent `code-explorer`), đề xuất kế hoạch, chờ tôi OK.
- Diff nhỏ, tập trung; không refactor/format code không liên quan.
- Không chắc quy tắc nghiệp vụ thì hỏi, đừng đoán.
- Mỗi mục roadmap = một branch + một PR. Chạy review độc lập (`/review` hoặc subagent `java-reviewer`) TRƯỚC khi commit, sửa xong chạy lại build đầy đủ. Tôi tự xem CI và merge.
- Commit và PR viết tiếng Việt, Conventional Commits: `feat(phase-1): ...`, `fix(ledger): ...`, `docs: ...`.
- Cập nhật roadmap/ADR/runbook trong cùng PR khi thay đổi đụng tới chúng.
- Kết thúc bằng: đã đổi gì, vì sao, kiểm chứng thế nào (lệnh + kết quả), tôi cần kiểm tay gì.

## Bộ công cụ Claude trong `.claude/`
- Commands: `/next-step [mục]` (đọc tiến độ roadmap → đề xuất bước tiếp → xin approve), `/feature`, `/fix-bug`, `/review [base]`, `/explain`, `/migration`. Subagents: `code-explorer`, `test-writer`, `java-reviewer`.
- Hooks: `guard-bash` chặn lệnh phá dữ liệu (force push, reset --hard, flyway clean, `compose down -v`, restore.sh, deploy); `protect-files` chặn sửa migration đã commit, `target/`, secret (`.env.example` thì được sửa); `verify-on-stop` chạy `./mvnw verify` cho module bị đổi khi Claude dừng (Docker tắt → bỏ qua IT và báo).
- Bị hook chặn thì đổi cách làm hoặc hỏi tôi — không lách hook.

## Gotchas
- Không thêm `-am` vào `spring-boot:run` → chạy trên parent pom trước → `Unable to find a suitable main class`.
- Testcontainers + Docker Engine ≥ 25 cần `docker.api.version` (đã đặt 1.44 ở pom cha); Docker cũ: `-Ddocker.api.version=1.41`.
- Service không khởi động khi thiếu `.env` hoặc khoá JWT: `cp .env.example .env && ./scripts/gen-keys.sh` (ADR-009, cố ý không có khoá mặc định).
- transaction-service lưu **nguyên văn** body JSON của lần nhận đầu để replay khớp từng byte — đừng serialize lại khi trả bản replay.
- Không kiểm tra số dư ở transaction-service (read model có thể trễ); chỉ ledger quyết "đủ tiền" dưới `FOR UPDATE`.
- Sau khi sửa `common-lib`, service chạy riêng lẻ (`-pl services/<svc>` không `-am`) sẽ dùng bản cũ trong `~/.m2` cho tới khi `install` lại common-lib.
