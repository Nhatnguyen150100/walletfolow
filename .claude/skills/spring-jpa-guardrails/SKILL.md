---
name: spring-jpa-guardrails
description: Use when writing or reviewing code that touches JPA/Hibernate entities, Spring Data repositories, @Transactional, queries, or database performance (N+1, lazy loading, locking, batching, slow endpoints).
---
# Spring + JPA guardrails

## In this repo (read first)
- `ddl-auto: validate` + Flyway per service; `open-in-view: false` everywhere. Entity must match the migration exactly.
- Ids are `UUID` assigned in code, so IDENTITY-batching concerns don't apply.
- Race-sensitive writes use native SQL, not load-modify-save: idempotent inserts via `@Modifying` native `INSERT ... ON CONFLICT` (0 rows affected = lost the race; see `IdempotencyRepository.claim`, `AccountRepository`, `WalletEntryRepository`), row locks via `@Lock(PESSIMISTIC_WRITE)` / `SELECT ... FOR UPDATE` (ledger accounts, wallets, saga) taken in a stable order (by id) to avoid deadlocks, and `FOR UPDATE SKIP LOCKED` for multi-instance workers (outbox relay, stuck-saga scanner). Keep that style for new money paths.
- Entities that are only INSERTed have no setters (`Transaction`); don't add setters to "make JPA happy".
- When a step must call HTTP before writing (e.g. wallet check), keep the HTTP call outside and run the write in a `TransactionTemplate` (see `TransferService`).
- `EventDeduplicator.markProcessed` is `Propagation.MANDATORY`: call it inside the listener's transaction.
- Every query on a business table filters by `tenant_id`; include `tenant_id` in new indexes/unique keys.

## Transactions
- `@Transactional` works through the Spring proxy: calling a transactional method from the SAME class (self-invocation) does nothing. Move it to another bean.
- Only public methods are intercepted. Private/protected `@Transactional` is silently ignored.
- Checked exceptions do NOT roll back by default. Use `rollbackFor = Exception.class` or unchecked domain exceptions.
- Keep transactions short: no HTTP calls, no Kafka sends, no sleeping inside them. Publish events after commit (`@TransactionalEventListener(phase = AFTER_COMMIT)` or an outbox table).
- `readOnly = true` on read paths: skips dirty checking and can route to replicas.

## Fetching and N+1
- Default `@ManyToOne` is EAGER: set `fetch = LAZY` explicitly on every `@ManyToOne` / `@OneToOne`.
- Load what the use case needs in one query: `JOIN FETCH`, `@EntityGraph`, or a DTO projection (`select new ...Dto(...)` / interface projection). Prefer projections for read-only endpoints.
- Never `JOIN FETCH` two collections in one query (cartesian product / MultipleBagFetchException). Fetch the second collection in a separate query or use `@BatchSize` / `hibernate.default_batch_fetch_size`.
- Pagination + `JOIN FETCH` of a collection paginates in memory (HHH90003004 warning). Page IDs first, then fetch by IDs.
- `spring.jpa.open-in-view=false`. Lazy loading in controllers/serializers is a bug.
- Detect N+1 in tests: enable Hibernate statistics or use a query-count assertion library and assert the expected number of statements.

## Writes
- Bulk changes: `@Modifying @Query("update ...")` + `clearAutomatically = true`; remember it bypasses entity callbacks, auditing and optimistic locking.
- Batch inserts: `hibernate.jdbc.batch_size`, `order_inserts=true`; `IDENTITY` id generation disables insert batching (use `SEQUENCE` with allocationSize).
- Concurrent updates of the same row: `@Version` optimistic locking + retry, or `@Lock(PESSIMISTIC_WRITE)` for short critical sections.

## Entities
- `equals/hashCode`: based on a stable business key or id with null-safe handling; never include lazy collections. Avoid Lombok `@Data` / `@EqualsAndHashCode` / `@ToString` on entities (lazy loading, recursion).
- Collections: initialize to empty, expose unmodifiable views, add helper methods that keep both sides of a bidirectional relation in sync.
- Enums: `@Enumerated(EnumType.STRING)`, never ORDINAL.

## Queries
- Never build JPQL/SQL by string concatenation with user input. Use parameters, Specifications, Criteria or jOOQ.
- Every new `WHERE`/`ORDER BY`/FK column on a large table needs an index; verify with `EXPLAIN (ANALYZE, BUFFERS)` on realistic data.
- `findAll()` without paging on a growing table is a production incident waiting to happen.
