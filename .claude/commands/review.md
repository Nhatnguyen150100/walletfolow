---
description: Review current changes (or a branch vs base) with the java-reviewer subagent
argument-hint: [base-branch, default master]
---
Base branch: $ARGUMENTS (if empty, use master)

Use the `java-reviewer` subagent to review the diff between the current working tree/branch and the base branch above (`git diff <base>...HEAD` plus uncommitted changes).
Tell it which roadmap item / ADR this change implements (see `docs/walletflow-production-roadmap.md`) so it can check intent, not just code.
Then present its findings. For each BLOCKER and MAJOR, propose the concrete code change but do not apply it until I say which ones to fix.
