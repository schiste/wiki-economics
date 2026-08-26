#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck disable=SC1091
. "$ROOT/scripts/lib/wiki_econ.sh"
wiki_econ_init_runtime

: "${WIKI_ECON_BIN:?Ready-candidate publication requires WIKI_ECON_BIN}"
: "${WIKI_ECON_RUN_ID:?Ready-candidate publication requires WIKI_ECON_RUN_ID}"
selection_active=0
site_published=0
failure_error=""

: > "$WIKI_ECON_RUN_EVENTS_FILE"
: > "$WIKI_ECON_RUN_SNAPSHOT_FILE"
printf '%s\n' starting > "$WIKI_ECON_RUN_STATE_FILE"
node "$WIKI_ECON_RUN_RECORD_HELPER" write
printf '%s\n' running > "$WIKI_ECON_RUN_STATE_FILE"

rollback_on_failure() {
  local status=$?
  trap - EXIT
  if [ "$status" -ne 0 ] && [ "$selection_active" -eq 1 ] && [ "$site_published" -eq 0 ]; then
    echo "==> Publication failed before site switch; restoring prior wiki generations" >&2
    wiki_econ_run_cli publication-rollback-ready \
      --lifecycle "$WIKI_ECON_WIKI_LIFECYCLE_FILE" || true
  elif [ "$status" -ne 0 ] && [ "$site_published" -eq 1 ]; then
    echo "Publication commit failed after the atomic site switch; data and site remain on the selected candidates. Retry publication-commit-ready with run ID $WIKI_ECON_RUN_ID." >&2
  fi
  if [ "$status" -ne 0 ]; then
    WIKI_ECON_RUN_ERROR="${failure_error:-publication transaction exited with status $status}" \
      node "$WIKI_ECON_RUN_RECORD_HELPER" finish "$status" || true
  else
    node "$WIKI_ECON_RUN_RECORD_HELPER" finish 0
  fi
  exit "$status"
}
trap rollback_on_failure EXIT
trap 'failure_error="command failed: $BASH_COMMAND"' ERR

echo "==> Auditing and recovering interrupted publication transactions"
recovery_report="${WIKI_ECON_RUN_STATE_FILE%.state}.recovery.json"
wiki_econ_run_cli publication-recover \
  --all \
  --lifecycle "$WIKI_ECON_WIKI_LIFECYCLE_FILE" \
  --site-dist-dir "$WIKI_ECON_SITE_DIST_DIR" \
  --report "$recovery_report"
if node -e '
  const fs = require("node:fs");
  const report = JSON.parse(fs.readFileSync(process.argv[1], "utf8"));
  process.exit(report.schema_version === 1 && report.site_rebuild_required === true ? 0 : 1);
' "$recovery_report"; then
  echo "==> Recovery restored an earlier data generation; rebuilding its matching site"
  export WIKI_ECON_REQUIRE_PUBLICATION_GATE=1
  "$ROOT/scripts/build-site.sh" \
    --output-dir "$WIKI_ECON_OUTPUT_DIR" \
    --dist-dir "$WIKI_ECON_SITE_DIST_DIR"
fi

wiki_econ_run_cli publication-prepare-ready \
  --lifecycle "$WIKI_ECON_WIKI_LIFECYCLE_FILE"
selection_file="$WIKI_ECON_OUTPUT_DIR/_publication_transactions/$WIKI_ECON_RUN_ID/selection.json"
selection_state="$(node -e '
  const fs = require("node:fs");
  const selection = JSON.parse(fs.readFileSync(process.argv[1], "utf8"));
  if (selection.schema_version !== 1 || selection.run_id !== process.argv[2]) process.exit(2);
  process.stdout.write(String(selection.state || ""));
' "$selection_file" "$WIKI_ECON_RUN_ID")"
case "$selection_state" in
  no_op)
    echo "==> No changed ready candidates; publication is a recorded no-op"
    exit 0
    ;;
  selected)
    selection_active=1
    ;;
  *)
    echo "Unexpected publication selection state: $selection_state" >&2
    exit 1
    ;;
esac

export WIKI_ECON_REQUIRE_PUBLICATION_GATE=1
"$ROOT/scripts/build-site.sh" \
  --output-dir "$WIKI_ECON_OUTPUT_DIR" \
  --dist-dir "$WIKI_ECON_SITE_DIST_DIR"
site_published=1

wiki_econ_run_cli publication-commit-ready
selection_active=0
echo "==> Ready candidates published and committed"
