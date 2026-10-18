---
name: java-testing-patterns
description: Use when writing, fixing or reviewing Java tests in WalletFlow - unit tests, Testcontainers *IT integration tests (Postgres, Kafka), contract tests for event JSON Schemas, test fakes, flaky tests.
---
# Java testing patterns (WalletFlow)

## Choose the narrowest test
| What you verify | Test type |
|---|---|
| Pure logic (`Money`, envelope parsing, filters) | Plain JUnit 5 in `src/test`, class name `*Test` (surefire) |
| Error → ProblemDetail mapping | Plain JUnit on `GlobalExceptionHandler` (see `GlobalExceptionHandlerTest`) |
| Event payload vs JSON Schema | `EventContractTest` / `EventSchemas` (common-lib test-jar) + fixture in `src/test/resources/contract/fixtures` |
| SQL, locking, idempotency, outbox, dedup, tenant isolation, HTTP API | `@SpringBootTest` + Testcontainers, class name `*IT` (failsafe) |
| Full Kafka flow (command → handler → event) | `*IT` with Postgres + `KafkaContainer("apache/kafka:3.9.1")` |

Most value in this repo lives in `*IT`: the JaCoCo gate (85% ledger/wallet/transaction, 70% auth) is measured on IT data only.

## Testcontainers (follow the existing style)
- `@Testcontainers` + `static @Container PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16")` + `@DynamicPropertySource` to wire `spring.datasource.*` / `spring.kafka.bootstrap-servers`.
- Real Flyway runs on startup: the IT is also the migration test. Never H2.
- Override timings via `@SpringBootTest(properties = "walletflow.transaction.saga-deadline=PT2S")` instead of waiting real time.
- Docker Engine ≥ 25 needs the `docker.api.version` system property (already wired through failsafe in each service pom). A new service must declare `maven-failsafe-plugin`, otherwise its `*IT` silently never run.

## Dependencies between services
Replace HTTP clients with fakes behind the interface, registered by a test config: `FakeWalletDirectory` + `FakeWalletDirectoryConfig` (transaction-service), `FakeLedger` + `FakeLedgerConfig` (wallet-service). Don't mock them with Mockito.

## Style
- AssertJ: `assertThat(...)`, `assertThatThrownBy(...)`. For HTTP, assert status AND `errorCode` of the ProblemDetail.
- Names: Vietnamese snake_case method + Vietnamese `@DisplayName` stating the behavior/invariant, e.g. `gui_trung_song_song_chi_mot_lenh` / `@DisplayName("⭐ 20 request cùng key ĐỒNG THỜI: đúng một lệnh ghi sổ")`.
- Helpers in `src/test/java/.../support/` (`SagaFixture`, `TransferApi`, `Events`) instead of repeating setup.
- Time: the code uses DB `now()` and `Instant.now()`; to test deadlines, move the data (`UPDATE saga_state SET deadline_at = now() - interval '1 second'`) rather than sleeping.
- Each test sets its own tenant/user ids (random UUIDs) so tests don't depend on order or on each other's data.

## Money properties need proof
Every money invariant (SUM(postings)=0, no double-charge on retry, no negative balance under concurrency, append-only ledger, tenant isolation, exactly-once effect of duplicate events) needs a test that fails when the invariant is broken. Patterns used here:
- Concurrency: N threads + `CountDownLatch` start gate + `ExecutorService`, then assert exact counts (see `IdempotencyIT`, `LedgerServiceIT`).
- Duplicates: send the same request / same `eventId` twice, assert one effect.
- Mutation check: temporarily break the invariant in production code, see the test go red, restore.

## Async and messaging
- No Awaitility in the repo: poll with a deadline (`while (System.nanoTime() < deadline) { consumer.poll(...) }`) as in `SagaFlowIT`; never a bare `Thread.sleep` as synchronization.
- Assert consumer idempotency by publishing the same event twice; assert poison messages land on the DLT unchanged without blocking later messages (`KafkaDltIT`).

## Running
- One IT: `./mvnw -B -ntp "-Dmaven.compiler.proc=full" -pl services/<svc> verify -Dit.test=<Class>#<method> -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false -Djacoco.skip=true`
- After changing common-lib: `./mvnw -B -ntp -q "-Dmaven.compiler.proc=full" -pl common-lib install -DskipTests` first.

## Anti-patterns to refuse
- Mocking the class under test, mocking value objects, verifying every interaction.
- Asserting only "not null" or "no exception".
- `@Disabled` or deleting tests to get green; loosening assertions to match buggy output.
- Shared mutable static state between tests; order-dependent tests.

## Flaky test checklist
Shared DB data between tests (same tenant/user ids), Kafka consumer group/offset reuse across tests, timing assumptions without a deadline loop, Docker API version, port conflicts with a running `docker-compose.infra.yml`.
