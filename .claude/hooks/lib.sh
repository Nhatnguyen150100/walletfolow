#!/usr/bin/env bash
# Shared helpers for hook scripts. Sourced, not executed.

# Read the hook's JSON payload from stdin exactly once.
HOOK_INPUT="$(cat)"

# First interpreter that actually runs. On Windows `python3` is often the Microsoft Store
# alias stub: it exists on PATH but only prints "Python was not found" — so probe, don't trust.
_json_runner() {
  if [[ -n "${_JSON_RUNNER:-}" ]]; then echo "$_JSON_RUNNER"; return; fi
  local c
  for c in jq python3 python py node; do
    command -v "$c" >/dev/null 2>&1 || continue
    case "$c" in
      jq)   jq -n 1 >/dev/null 2>&1 || continue ;;
      node) node -e '' >/dev/null 2>&1 || continue ;;
      *)    "$c" -c 'import json' >/dev/null 2>&1 || continue ;;
    esac
    _JSON_RUNNER="$c"; echo "$c"; return
  done
  echo none
}
_JSON_RUNNER="$(_json_runner)"   # probe once per hook run, not once per json_get

# json_get '.tool_input.file_path'  -> prints value or empty string.
# Payload goes through stdin (not argv: Write payloads can exceed the Windows command-line limit)
# and is decoded as UTF-8. CR is stripped because Windows interpreters print CRLF.
json_get() {
  local path="$1" runner
  runner="$(_json_runner)"
  case "$runner" in
    jq)
      jq -r "${path} // empty" <<<"$HOOK_INPUT" 2>/dev/null | tr -d '\r' ;;
    node)
      node -e '
        let raw = ""; process.stdin.setEncoding("utf8");
        process.stdin.on("data", d => raw += d).on("end", () => {
          try {
            let node = JSON.parse(raw);
            for (const k of process.argv[1].split(".").filter(Boolean)) node = node == null ? null : node[k];
            if (node !== null && node !== undefined) process.stdout.write(String(node));
          } catch (e) {}
        });' "$path" <<<"$HOOK_INPUT" 2>/dev/null | tr -d '\r' ;;
    none)
      echo "lib.sh: no working jq/python/node found; hook guards are DISABLED" >&2 ;;
    *)
      "$runner" -c '
import json, sys
try:
    node = json.loads(sys.stdin.buffer.read().decode("utf-8"))
    for key in [k for k in sys.argv[1].split(".") if k]:
        node = node.get(key) if isinstance(node, dict) else None
    if node is not None:
        out = str(node).lower() if isinstance(node, bool) else str(node)
        sys.stdout.buffer.write(out.encode("utf-8"))
except Exception:
    pass
' "$path" <<<"$HOOK_INPUT" 2>/dev/null | tr -d '\r' ;;
  esac
}

# Normalize a path so Windows (`D:\a\b`, `/d/a/b`) and POSIX paths compare the same way:
# forward slashes, upper-case drive letter, no trailing slash.
norm_path() {
  local p="${1//\\//}"
  if [[ "$p" =~ ^/([a-zA-Z])/(.*)$ ]]; then p="${BASH_REMATCH[1]}:/${BASH_REMATCH[2]}"; fi
  if [[ "$p" =~ ^([a-zA-Z]):(.*)$ ]]; then
    p="$(printf '%s' "${BASH_REMATCH[1]}" | tr '[:lower:]' '[:upper:]'):${BASH_REMATCH[2]}"
  fi
  echo "${p%/}"
}

project_dir() { norm_path "${CLAUDE_PROJECT_DIR:-$(pwd)}"; }

# Path relative to the project root (unchanged if outside the project).
rel_path() {
  local f d; f="$(norm_path "$1")"; d="$(project_dir)"
  echo "${f#"$d"/}"
}

# Prefer wrappers so the build uses the project's pinned tool versions.
build_tool() {
  local dir; dir="$(project_dir)"
  if [[ -f "$dir/mvnw" ]]; then echo "maven-wrapper"
  elif [[ -f "$dir/pom.xml" ]]; then echo "maven"
  else echo "none"
  fi
}

maven_cmd() { [[ -f "$(project_dir)/mvnw" ]] && echo "./mvnw" || echo "mvn"; }
