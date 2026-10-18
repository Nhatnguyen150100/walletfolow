#!/usr/bin/env bash
# PreToolUse (Edit|MultiEdit|Write): protect files Claude must never change.
set -uo pipefail
source "$(dirname "$0")/lib.sh"

file="$(json_get '.tool_input.file_path')"
[[ -n "$file" ]] || exit 0
cd "$(project_dir)" || exit 0
rel="$(rel_path "$file")"

block() { printf 'Blocked by protect-files.sh: %s\nFile: %s\n' "$1" "$rel" >&2; exit 2; }

# 1. Flyway migrations that are already committed are immutable (checksum!).
#    Each service has its own: services/<svc>/src/main/resources/db/migration/V<n>__*.sql
if [[ "$rel" =~ db/migration/V[0-9_.]+__.*\.sql$ ]] && git ls-files --error-unmatch -- "$rel" >/dev/null 2>&1; then
  block "This Flyway migration is already committed. Changing it breaks checksums on every environment. Create a NEW migration (next version number in the same service) instead."
fi

# 2. Generated code is overwritten on the next build.
if [[ "$rel" =~ (^|/)target/ ]]; then
  block "Build output under target/. Change the source that produces it instead."
fi

# 3. Secrets and production config. .env.example is the committed template and MAY be edited
#    (add new variables there with a placeholder value, never a real secret).
if [[ "$rel" =~ (^|/)\.env(\..*)?$ && ! "$rel" =~ (^|/)\.env\.example$ ]]; then
  block "Secrets must be changed by a human. Add new variables to .env.example instead."
fi
if [[ "$rel" =~ application-prod.*\.(ya?ml|properties)$ || "$rel" =~ \.(jks|p12|pem|key)$ || "$rel" =~ (^|/)(secrets|backups)/ ]]; then
  block "Secrets / production configuration / DB backups must be changed by a human."
fi

# 4. Maven wrapper: version bumps should be deliberate.
if [[ "$rel" =~ (^|/)\.mvn/wrapper/ ]]; then
  block "Maven wrapper changes must be done intentionally by the user (./mvnw wrapper:wrapper -Dmaven=...)."
fi
exit 0
