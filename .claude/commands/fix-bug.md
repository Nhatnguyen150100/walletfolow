---
description: Fix a bug test-first, from a stack trace, log, trace id, or description
argument-hint: <stack trace | log excerpt | Jaeger trace id | bug description>
---
Bug: $ARGUMENTS

1. Gather evidence: stack trace / logs (`docker compose logs <svc>`), and for Kafka flows check the relevant topic, DLT and runbook in `docs/runbooks/` (e.g. `saga-stuck.md`, `dlt-not-empty.md`, `outbox-stalled.md`).
2. Locate the failing code path (use `code-explorer` if the path is not obvious). State the root-cause hypothesis and the evidence for it. If there are multiple plausible causes, list them and say how you will distinguish them.
3. Write a test that reproduces the bug and FAILS (usually a `*IT` with Testcontainers). Run only it and show the failure:
   `./mvnw -B -ntp "-Dmaven.compiler.proc=full" -pl services/<svc> verify -Dit.test=<Class>#<method> -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false -Djacoco.skip=true`
4. Apply the smallest fix that addresses the root cause, not the symptom. No unrelated refactors.
5. Run the test again (must pass), then the module: `./mvnw -B -ntp "-Dmaven.compiler.proc=full" -pl services/<svc> -am verify`.
6. Check for the same bug pattern elsewhere (`rg`), especially in the other services that copy the same pattern (outbox, dedup, tenant filter, idempotency), and list other occurrences without fixing them unless trivial.
7. Report: root cause in 2-3 sentences, the fix, the test, and whether money/data already affected needs a repair (ledger is append-only: repairs are compensating journal entries, never UPDATE).
