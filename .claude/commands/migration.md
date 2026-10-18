---
description: Create a safe, backward-compatible Flyway migration
argument-hint: <service> <schema change description>
---
Schema change: $ARGUMENTS

1. Identify the owning service (DB-per-service: authdb, ledgerdb, walletdb, txndb — PostgreSQL 16). Read its existing migrations in `services/<svc>/src/main/resources/db/migration` to find the next version number and the style used (comments in Vietnamese explaining why).
2. Decide whether the change is backward compatible with the currently deployed app version. If not, split it into expand/contract steps and explain which step ships in which release. `ddl-auto` stays `validate`: the entity must match the schema after the migration.
3. Write `V<next>__<snake_case>.sql`. Consider: `tenant_id UUID NOT NULL` on every new business table and in its indexes/unique keys (ADR-010); money as `BIGINT` minor unit (ADR-005); locks on large tables (`CREATE INDEX CONCURRENTLY` must be alone in its own migration because it cannot run inside a transaction); defaults on big tables; indexes for new foreign keys and new WHERE columns; backfill in batches. Ledger tables are append-only (trigger) — never plan an UPDATE/DELETE of postings.
4. Update JPA entities / queries accordingly and make sure an existing or new `*IT` (Testcontainers, real Flyway run) exercises the new schema.
5. Report: the SQL, estimated risk on a large production table, rollback approach (Flyway has no undo here: describe the forward fix).
