#!/usr/bin/env bash
# PreToolUse (Bash): block commands that can destroy data or reach production.
# Exit 2 blocks the call and tells Claude why. Add rows for your team:
# one rule per line, "<extended regex><TAB><reason>". Works on bash 3.2 (macOS).
set -uo pipefail
source "$(dirname "$0")/lib.sh"

cmd="$(json_get ".tool_input.command")"
[[ -n "$cmd" ]] || exit 0

while IFS=$'\t' read -r re reason; do
  [[ -z "$re" || "$re" == \#* ]] && continue
  if grep -Eiq -- "$re" <<<"$cmd"; then
    printf "Blocked by guard-bash.sh: %s\nCommand: %s\n" "$reason" "$cmd" >&2
    exit 2
  fi
done <<'RULES'
git[[:space:]]+push.*(--force|[[:space:]]-f([[:space:]]|$))	Force push is not allowed.
git[[:space:]]+reset[[:space:]]+--hard	git reset --hard discards work; ask the user first.
mvnw?(\.cmd)?[^|;&]*[[:space:]]deploy	Publishing artifacts must be done by CI.
flyway[^|;&]*clean|flywayClean	flyway clean wipes the database schema.
scripts/restore\.sh	restore.sh overwrites a database from a backup; a human must run it (docs/runbooks/restore.md).
docker[[:space:]-]+compose[^|;&]*[[:space:]]down[^|;&]*([[:space:]]-v|--volumes)	docker compose down -v deletes the Postgres/Kafka volumes; ask the user first.
(DROP|TRUNCATE)[[:space:]]+(TABLE|DATABASE|SCHEMA)	Destructive SQL is blocked; write a migration and let the user review it.
rm[[:space:]]+-[a-zA-Z]*r[a-zA-Z]*[[:space:]]+(/|~|\$HOME|\.)([[:space:]]|$)	Recursive delete of a root, home or project directory is blocked.
spring\.profiles\.active=prod|SPRING_PROFILES_ACTIVE=prod	Running with the prod profile is not allowed from Claude.
kubectl[^|;&]*(prod|production)	kubectl against production is not allowed.
RULES
exit 0
