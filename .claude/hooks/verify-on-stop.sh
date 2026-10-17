#!/usr/bin/env bash
# Stop hook: Claude may only finish when the Maven modules it changed build and pass tests.
#
#  - Runs only when build-relevant files inside a Maven module changed (git diff vs HEAD +
#    untracked). Changes to docs/, deploy/, docker-compose, CI and .claude/ never trigger it.
#  - Builds only the affected modules: -pl <changed> -am -amd (common-lib change => all services).
#  - Default goal is `verify`: almost every test here is a *IT (failsafe + Testcontainers), so
#    `test` alone would prove little more than "it compiles". Includes the JaCoCo IT gate, like CI.
#  - Docker not running => falls back to `verify -DskipITs` and says so; ITs run on the next stop
#    once Docker is up.
#  - Skips the run if the exact same changes already passed (no re-running on every reply).
#  - On failure: exit 2 -> Claude sees a trimmed failure report and keeps working.
#  - Gives up after CLAUDE_VERIFY_MAX_ATTEMPTS failed rounds so it can never loop forever.
#
# Env knobs: CLAUDE_VERIFY_SKIP=1 (disable), CLAUDE_VERIFY_MAX_ATTEMPTS (default 3),
#            CLAUDE_VERIFY_GOAL (default verify; "test" for a quick compile+unit check),
#            CLAUDE_VERIFY_EXTRA_ARGS (e.g. "-Dmaven.compiler.proc=full" on JDK 23+).
set -uo pipefail
source "$(dirname "$0")/lib.sh"

[[ "${CLAUDE_VERIFY_SKIP:-0}" == "1" ]] && exit 0
cd "$(project_dir)" || exit 0
git rev-parse --is-inside-work-tree >/dev/null 2>&1 || exit 0
[[ "$(build_tool)" == "none" ]] && exit 0

session="$(json_get '.session_id')"; session="${session:-default}"
max_attempts="${CLAUDE_VERIFY_MAX_ATTEMPTS:-3}"
state_dir=".claude/.state"; mkdir -p "$state_dir"
counter_file="$state_dir/attempts-$session"
green_file="$state_dir/last-green"
log_file="$state_dir/last-verify.log"

# Closest directory owning a pom.xml ("." = root reactor).
module_of() {
  local d; d="$(dirname "$1")"
  while [[ "$d" != "." && "$d" != "/" ]]; do
    [[ -f "$d/pom.xml" ]] && { echo "$d"; return; }
    d="$(dirname "$d")"
  done
  echo "."
}

changed="$( { git diff --name-only HEAD 2>/dev/null; git ls-files --others --exclude-standard; } \
  | grep -E '\.(java|xml|ya?ml|properties|sql|json)$' \
  | grep -vE '(^|/)(\.claude|\.idea|\.github|docs|deploy|target)/' | sort -u )"

# Keep only files that belong to a module (root-level files count only if it is the root pom).
modules=""; relevant=""
while read -r f; do
  [[ -z "$f" ]] && continue
  m="$(module_of "$f")"
  [[ "$m" == "." && "$f" != "pom.xml" ]] && continue
  modules+="$m"$'\n'; relevant+="$f"$'\n'
done <<<"$changed"
modules="$(printf '%s' "$modules" | sort -u)"
if [[ -z "$modules" ]]; then rm -f "$counter_file"; exit 0; fi

goal="${CLAUDE_VERIFY_GOAL:-verify}"
mode="$goal"; skip_its=""
if [[ "$goal" == "verify" ]] && ! docker info >/dev/null 2>&1; then
  mode="verify-no-it"; skip_its="-DskipITs -Djacoco.skip=true"
fi

# Fingerprint of the current change set + mode; identical to the last green run -> nothing to do.
fingerprint="$( { echo "$mode"; git diff HEAD 2>/dev/null; printf '%s' "$relevant" | while read -r f; do [[ -f "$f" ]] && cat "$f"; done; } \
  | { sha1sum 2>/dev/null || shasum; } | cut -c1-40 )"
if [[ -f "$green_file" && "$(cat "$green_file")" == "$fingerprint" ]]; then exit 0; fi

attempts="$(cat "$counter_file" 2>/dev/null || echo 0)"
if (( attempts >= max_attempts )); then
  printf '{"systemMessage":"verify-on-stop: build still failing after %s attempts. Stopped auto-retrying; see %s"}\n' \
    "$attempts" "$log_file"
  exit 0
fi

if grep -qx '.' <<<"$modules"; then pl=""
else pl="-pl $(printf '%s\n' "$modules" | paste -sd, -) -am -amd"; fi
extra="${CLAUDE_VERIFY_EXTRA_ARGS:-}"

# shellcheck disable=SC2086
$(maven_cmd) -B -ntp $pl $goal $skip_its $extra >"$log_file" 2>&1; rc=$?

if [[ $rc -eq 0 ]]; then
  echo "$fingerprint" >"$green_file"; rm -f "$counter_file"
  if [[ -n "$skip_its" ]]; then
    printf '{"systemMessage":"verify-on-stop: compile + unit tests passed, but Docker is not running so integration tests (*IT) were SKIPPED. Start Docker Desktop; they will run on the next stop."}\n'
  fi
  exit 0
fi

echo $((attempts + 1)) >"$counter_file"
{
  echo "Build/tests FAILED for: $(printf '%s ' $modules)(goal: $mode, attempt $((attempts + 1))/$max_attempts)."
  echo "Fix the root cause, do not delete or @Disable tests to make them pass. Key lines:"
  echo "----"
  grep -E '(\.java:\[?[0-9]+|ERROR|FAILED|FAILURE|Tests run:.*(Failures|Errors): [1-9]|Caused by:|expected:|but was:|AssertionFailedError|Coverage checks have not been met)' "$log_file" \
    | grep -vE '^\s*at (org\.(junit|apache\.maven)|java\.base|jdk\.internal|sun\.)' | head -n 60
  echo "----"
  echo "Last lines of the build log:"; tail -n 20 "$log_file"
  echo "Full log: $log_file"
} >&2
exit 2
