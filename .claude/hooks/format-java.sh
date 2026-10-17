#!/usr/bin/env bash
# PostToolUse (Edit|MultiEdit|Write): format the Java file Claude just touched — ONLY if the
# project configures Spotless. Exit 2 sends stderr back to Claude, so a formatter failure
# (usually a syntax error) becomes immediate feedback instead of a surprise at build time.
#
# Deliberately no google-java-format fallback: this repo mixes 2-space (common-lib) and
# 4-space (services) indentation without a formatter, and reformatting a whole file on every
# edit would bury the real change in a huge diff.
set -uo pipefail
source "$(dirname "$0")/lib.sh"

file="$(norm_path "$(json_get '.tool_input.file_path')")"
[[ "$file" == *.java && -f "$file" ]] || exit 0
cd "$(project_dir)" || exit 0

grep -ls 'spotless' pom.xml */pom.xml */*/pom.xml 2>/dev/null | grep -q . || exit 0

# spotlessFiles takes a regex; escape the absolute path.
regex="$(printf '%s' "$file" | sed 's/[][.*^$()+?{}|\\]/\\&/g')"
out="$($(maven_cmd) -q -B -o spotless:apply -DspotlessFiles="$regex" 2>&1)"; rc=$?

if [[ $rc -ne 0 ]]; then
  printf 'Formatter failed on %s (often a syntax error). Fix it before continuing:\n%s\n' \
    "$file" "$(printf '%s' "$out" | tail -n 25)" >&2
  exit 2
fi
exit 0
