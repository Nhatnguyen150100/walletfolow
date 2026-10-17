---
description: Implement a feature end-to-end with plan-first, contract-first workflow
argument-hint: <feature description, roadmap item (e.g. "1.4 notification-service") or ticket text>
---
Feature request: $ARGUMENTS

Work in these phases and do not skip ahead:

1. **Explore** — read the matching section of `docs/walletflow-production-roadmap.md` and related ADRs in `docs/adr/`, then use the `code-explorer` subagent to map the code this feature touches.
2. **Plan** — write a short plan: REST changes (endpoint, request/response records, error codes), Kafka contract changes (topic in `WalletTopics`, JSON Schema in `common-lib/src/main/resources/schemas/`), Flyway migration(s) per service, classes to add/change per package (entity → repositories → services → listener/controller), money invariants touched and the IT that will prove each one, whether a new ADR/runbook is needed, risks and open questions. Then STOP and wait for my approval.
3. **Contract first** — after approval: add/update the JSON Schema + fixture and `EventContractTest`, the event record in `common-lib`, and the migration. Install common-lib (`./mvnw -B -ntp -q "-Dmaven.compiler.proc=full" -pl common-lib install -DskipTests`) and make sure the services compile.
4. **Implement** package by package: entity/repositories → services (transactions, outbox, dedup, tenant) → listener/controller. Keep each step compiling.
5. **Test** — delegate to the `test-writer` subagent: `*IT` with Testcontainers for every money/idempotency/tenant property, plus plain unit tests for pure logic. For money paths, confirm the key test FAILS when the invariant is broken.
6. **Review** — run the `java-reviewer` subagent on the diff and fix BLOCKER/MAJOR findings, then re-run `./mvnw -B -ntp "-Dmaven.compiler.proc=full" -pl <modules> -am verify`.
7. **Report** — summary of changes, how it was verified (commands + results), manual checks I should do, roadmap/ADR updates, and a suggested commit message in Vietnamese (Conventional Commits, e.g. `feat(phase-1): ...`).
