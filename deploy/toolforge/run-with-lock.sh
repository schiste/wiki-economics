#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 4 ]; then
  echo "Usage: run-with-lock.sh LOCK_DIR SCOPE STALE_SECONDS COMMAND [ARG ...]" >&2
  exit 2
fi

lock_dir=$1
scope=$2
stale_seconds=$3
shift 3

case "$(basename "$lock_dir")" in
  *.lock) ;;
  *) echo "Refusing unsafe lock path (basename must end in .lock): $lock_dir" >&2; exit 2 ;;
esac
if [[ ! "$stale_seconds" =~ ^[1-9][0-9]*$ ]]; then
  echo "Lock stale interval must be a positive integer: $stale_seconds" >&2
  exit 2
fi

run_id="${WIKI_ECON_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
heartbeat_seconds="${WIKI_ECON_LOCK_HEARTBEAT_SECS:-60}"
recheck_seconds="${WIKI_ECON_LOCK_RECHECK_SECS:-2}"
owner_token="$run_id-$$-$(date +%s)"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
main_pid=$$
heartbeat_pid=""
lock_owned=0
case "$run_id" in
  *[!A-Za-z0-9._-]*|'') echo "Unsafe lock run ID: $run_id" >&2; exit 2 ;;
esac
if [[ ! "$heartbeat_seconds" =~ ^[1-9][0-9]*$ ]] ||
   [[ ! "$recheck_seconds" =~ ^[0-9]+$ ]]; then
  echo "Lock heartbeat and recheck intervals must be integers" >&2
  exit 2
fi

write_owner() {
  local directory=$1
  WIKI_ECON_LOCK_RUN_ID="$run_id" \
  WIKI_ECON_LOCK_SCOPE="$scope" \
  WIKI_ECON_LOCK_TOKEN="$owner_token" \
  WIKI_ECON_LOCK_PID="$main_pid" \
  WIKI_ECON_LOCK_HEARTBEAT_EPOCH="$(date +%s)" \
  WIKI_ECON_LOCK_STARTED_AT="$started_at" \
    node - "$directory/owner.json" <<'NODE'
const fs = require("node:fs");
const output = process.argv[2];
const value = {
  schema_version: 1,
  run_id: process.env.WIKI_ECON_LOCK_RUN_ID,
  scope: process.env.WIKI_ECON_LOCK_SCOPE,
  pid: Number(process.env.WIKI_ECON_LOCK_PID),
  job_identity: process.env.TOOLFORGE_JOB_NAME || process.env.JOB_NAME || null,
  process_identity: process.env.HOSTNAME || null,
  owner_token: process.env.WIKI_ECON_LOCK_TOKEN,
  started_at: process.env.WIKI_ECON_LOCK_STARTED_AT,
  heartbeat_epoch: Number(process.env.WIKI_ECON_LOCK_HEARTBEAT_EPOCH),
};
const temporary = `${output}.tmp.${process.pid}`;
fs.writeFileSync(temporary, `${JSON.stringify(value)}\n`, {mode: 0o600});
fs.renameSync(temporary, output);
NODE
}

lock_is_stale() {
  WIKI_ECON_LOCK_NOW="$(date +%s)" WIKI_ECON_LOCK_STALE="$stale_seconds" \
    node - "$1" <<'NODE'
const fs = require("node:fs");
const path = require("node:path");
const directory = process.argv[2];
const now = Number(process.env.WIKI_ECON_LOCK_NOW);
const stale = Number(process.env.WIKI_ECON_LOCK_STALE);
try {
  const owner = JSON.parse(fs.readFileSync(path.join(directory, "owner.json"), "utf8"));
  process.exit(Number.isSafeInteger(owner.heartbeat_epoch) && now - owner.heartbeat_epoch > stale ? 0 : 1);
} catch {
  const age = now - Math.floor(fs.statSync(directory).mtimeMs / 1000);
  process.exit(age > stale ? 0 : 1);
}
NODE
}

remove_known_lock_dir() {
  local directory=$1
  rm -f -- "$directory/owner.json" "$directory/owner-token"
  rmdir -- "$directory" 2>/dev/null || true
}

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  if [ -n "$heartbeat_pid" ]; then
    kill "$heartbeat_pid" 2>/dev/null || true
    wait "$heartbeat_pid" 2>/dev/null || true
  fi
  if [ "$lock_owned" -eq 1 ] && [ -f "$lock_dir/owner-token" ] &&
     [ "$(<"$lock_dir/owner-token")" = "$owner_token" ]; then
    remove_known_lock_dir "$lock_dir"
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir -p "$(dirname "$lock_dir")"
for attempt in 1 2 3; do
  if mkdir "$lock_dir" 2>/dev/null; then
    chmod 700 "$lock_dir"
    printf '%s\n' "$owner_token" > "$lock_dir/owner-token"
    write_owner "$lock_dir"
    lock_owned=1
    break
  fi
  if ! lock_is_stale "$lock_dir"; then
    echo "Lock is active; refusing concurrent scope $scope" >&2
    [ ! -f "$lock_dir/owner.json" ] || tr -d '\n' < "$lock_dir/owner.json" >&2
    echo >&2
    exit 75
  fi
  token_before="$(cat "$lock_dir/owner-token" 2>/dev/null || true)"
  sleep "$recheck_seconds"
  token_after="$(cat "$lock_dir/owner-token" 2>/dev/null || true)"
  if [ "$token_before" != "$token_after" ] || ! lock_is_stale "$lock_dir"; then
    echo "Lock changed during stale recheck; refusing concurrent scope $scope" >&2
    exit 75
  fi
  stale_dir="$lock_dir.stale.$run_id.$attempt"
  if mv "$lock_dir" "$stale_dir" 2>/dev/null; then
    remove_known_lock_dir "$stale_dir"
  fi
done
if [ "$lock_owned" -ne 1 ]; then
  echo "Unable to acquire lock after stale recovery: $lock_dir" >&2
  exit 75
fi

(
  while sleep "$heartbeat_seconds"; do
    [ -f "$lock_dir/owner-token" ] || exit 0
    [ "$(<"$lock_dir/owner-token")" = "$owner_token" ] || exit 0
    if ! write_owner "$lock_dir"; then
      kill -TERM "$$"
      exit 1
    fi
    if [ -n "${WIKI_ECON_RUN_RECORD_HELPER:-}" ] &&
       [ -f "${WIKI_ECON_RUN_EVENTS_FILE:-}" ]; then
      node "$WIKI_ECON_RUN_RECORD_HELPER" write || {
        kill -TERM "$$"
        exit 1
      }
    fi
  done
) &
heartbeat_pid=$!

echo "==> Acquired $scope lock for run $run_id"
command_status=0
"$@" || command_status=$?
exit "$command_status"
