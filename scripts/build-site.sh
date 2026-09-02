#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export OBSERVABLE_TELEMETRY_DISABLE="${OBSERVABLE_TELEMETRY_DISABLE:-true}"
# shellcheck disable=SC1091
. "$ROOT/scripts/lib/wiki_econ.sh"

usage() {
  cat <<'EOF'
Usage: ./scripts/build-site.sh [options]

Builds the production Observable site against the current output artifact set.

Options:
  --output-dir PATH   Override the output artifact directory
  --dist-dir PATH     Override the site build output directory
  -h, --help          Show this help message
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --output-dir)
      shift
      WIKI_ECON_OUTPUT_DIR="${1:-}"
      ;;
    --dist-dir)
      shift
      WIKI_ECON_SITE_DIST_DIR="${1:-}"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
  shift
done

wiki_econ_init_runtime
wiki_econ_ensure_local_dirs
SITE_STAGE_STARTED_EPOCH="$(date +%s)"
SITE_STAGE_FINISHED=0
DASHBOARD_DEFAULTS_STAGE_ACTIVE=0
wiki_econ_record_stage_event started site

verify_publication_receipt() {
  if [ "${WIKI_ECON_REQUIRE_PUBLICATION_GATE:-0}" = "1" ]; then
    if [ -z "${WIKI_ECON_RUN_ID:-}" ]; then
      echo "Publication gate is required but WIKI_ECON_RUN_ID is empty" >&2
      exit 1
    fi
    wiki_econ_run_cli publication-verify
  fi
}

verify_publication_receipt

if [ "${WIKI_ECON_REQUIRE_PUBLICATION_GATE:-0}" = "1" ]; then
  # The operational manifest is read by the authenticated admin API as well
  # as copied into the static site. A site-source-only release can change its
  # lifecycle interpretation without running the Rust merge generator, so
  # refresh it before checking whether the site itself is reusable.
  manifest_path="$WIKI_ECON_OUTPUT_DIR/manifest.json"
  manifest_generator="$WIKI_ECON_SITE_DIR/data-build/manifest.json.sh"
  expected_manifest_commit="${WIKI_ECON_SITE_SOURCE_COMMIT:-${WIKI_ECON_SOURCE_COMMIT:-}}"
  current_manifest_commit="$(node -e '
    const fs = require("node:fs");
    try {
      const manifest = JSON.parse(fs.readFileSync(process.argv[1], "utf8"));
      process.stdout.write(manifest?.provenance?.generating_commit || "");
    } catch {}
  ' "$manifest_path")"

  if [ -n "$expected_manifest_commit" ] && [ "$current_manifest_commit" != "$expected_manifest_commit" ]; then
    [ -x "$manifest_generator" ] || {
      echo "Operational manifest generator is missing or not executable: $manifest_generator" >&2
      exit 1
    }
    manifest_generated_at="$(node -e '
      const fs = require("node:fs");
      const manifest = JSON.parse(fs.readFileSync(process.argv[1], "utf8"));
      const value = manifest.generated_at;
      if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(value)) process.exit(1);
      process.stdout.write(value);
    ' "$manifest_path")" || {
      echo "Existing operational manifest has no valid deterministic timestamp: $manifest_path" >&2
      exit 1
    }
    WIKI_ECON_MANIFEST_GENERATED_AT="$manifest_generated_at" \
      wiki_econ_run_cli manifest-materialize --generator-dir "$(dirname "$manifest_generator")"
    echo "==> Refreshed operational manifest for site source ${expected_manifest_commit:-unknown}"
  fi

  if wiki_econ_run_cli site-fingerprint-check \
    --site-dir "$WIKI_ECON_SITE_DIR" \
    --dist-dir "$WIKI_ECON_SITE_DIST_DIR"; then
    wiki_econ_record_stage_event reused site
    wiki_econ_record_stage_event completed site "" "$(( ($(date +%s) - SITE_STAGE_STARTED_EPOCH) * 1000 ))"
    SITE_STAGE_FINISHED=1
    echo "==> Site inputs unchanged; reusing published site"
    exit 0
  fi
fi

wiki_econ_ensure_site_deps
site_vendor_cache="${WIKI_ECON_SITE_VENDOR_CACHE_DIR:-$WIKI_ECON_ROOT/site/vendor/observable-cache}"

dist_dir="$WIKI_ECON_SITE_DIST_DIR"
dist_parent="$(dirname "$dist_dir")"
dist_name="$(basename "$dist_dir")"

case "$dist_name" in
  ''|.|..|/)
    echo "Refusing unsafe site distribution path: $dist_dir" >&2
    exit 1
    ;;
esac

mkdir -p "$dist_parent"
site_build_run_id="${WIKI_ECON_RUN_ID:-local-$$}"
case "$site_build_run_id" in
  *[!A-Za-z0-9._-]*|'')
    echo "Refusing unsafe site build run ID: $site_build_run_id" >&2
    exit 1
    ;;
esac
build_dir="$(mktemp -d "$dist_parent/.${dist_name}.build.${site_build_run_id}.XXXXXX")"
source_dir="$(mktemp -d "$dist_parent/.${dist_name}.source.${site_build_run_id}.XXXXXX")"
# prepare-site-source requires a destination that does not yet exist.
rmdir "$source_dir"
dashboard_dir=""
dashboard_staging_dir=""
defaults_dir="${WIKI_ECON_SITE_DEFAULTS_DIR:-$dist_parent/${dist_name}-defaults}"
defaults_parent="$(dirname "$defaults_dir")"
defaults_name="$(basename "$defaults_dir")"
next_link="$(mktemp "$dist_parent/.${dist_name}.next.XXXXXX")"
rm -f -- "$next_link"
legacy_dir="$dist_parent/.${dist_name}.previous.$$"
defaults_next_link=""
defaults_legacy_dir="$defaults_parent/.${defaults_name}.previous.$$"

case "$defaults_name" in
  ''|.|..|/)
    echo "Refusing unsafe dashboard defaults path: $defaults_dir" >&2
    exit 1
    ;;
esac

cleanup_site_build() {
  local exit_code=$? current_target=""

  rm -f -- "$next_link"
  if [ -n "$defaults_next_link" ]; then
    rm -f -- "$defaults_next_link"
  fi
  rm -rf -- "$source_dir"
  if [ -n "$dashboard_staging_dir" ]; then
    rm -rf -- "$dashboard_staging_dir"
  fi
  if [ -L "$dist_dir" ]; then
    current_target="$(readlink "$dist_dir")"
  fi
  if [ "$current_target" != "$(basename "$build_dir")" ]; then
    rm -rf -- "$build_dir"
  fi
  if [ -d "$legacy_dir" ] && [ ! -e "$dist_dir" ] && [ ! -L "$dist_dir" ]; then
    mv -- "$legacy_dir" "$dist_dir"
  fi
  if [ "$exit_code" -ne 0 ] && [ "$SITE_STAGE_FINISHED" -eq 0 ]; then
    if [ "$DASHBOARD_DEFAULTS_STAGE_ACTIVE" -eq 1 ]; then
      wiki_econ_record_stage_event failed dashboard_defaults "" \
        "$(( ($(date +%s) - DASHBOARD_DEFAULTS_STAGE_STARTED_EPOCH) * 1000 ))" \
        "dashboard defaults exited with status $exit_code"
    fi
    wiki_econ_record_stage_event failed site "" \
      "$(( ($(date +%s) - SITE_STAGE_STARTED_EPOCH) * 1000 ))" \
      "site build exited with status $exit_code"
  fi
}
trap cleanup_site_build EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "==> Building Observable site"
echo "    output dir: $WIKI_ECON_OUTPUT_DIR"
echo "    dist dir:   $WIKI_ECON_SITE_DIST_DIR"
echo "    staging:    $build_dir"

if [ "${WIKI_ECON_VERIFY_SITE_CLOSURE:-1}" = "1" ]; then
  if [ "${WIKI_ECON_REQUIRE_PUBLICATION_GATE:-0}" = "1" ]; then
    DASHBOARD_DEFAULTS_STAGE_STARTED_EPOCH="$(date +%s)"
    DASHBOARD_DEFAULTS_STAGE_ACTIVE=1
    wiki_econ_record_stage_event started dashboard_defaults
    if [ -d "$defaults_dir" ] && wiki_econ_run_cli dashboard-defaults-fingerprint-check \
      --workspace-dir "$ROOT" \
      --defaults-dir "$defaults_dir"; then
      dashboard_dir="$defaults_dir"
      wiki_econ_record_stage_event reused dashboard_defaults
      echo "==> Dashboard defaults unchanged; reusing validated bundle"
    else
      mkdir -p "$defaults_parent"
      dashboard_staging_dir="$(mktemp -d "$defaults_parent/.${defaults_name}.build.${site_build_run_id}.XXXXXX")"
      wiki_econ_run_cli dashboard-materialize --destination-dir "$dashboard_staging_dir"
      cp "$WIKI_ECON_OUTPUT_DIR/manifest.json" "$dashboard_staging_dir/manifest.json"
      wiki_econ_run_cli dashboard-defaults-fingerprint-record \
        --workspace-dir "$ROOT" \
        --defaults-dir "$dashboard_staging_dir"

      old_defaults_release=""
      defaults_next_link="$(mktemp "$defaults_parent/.${defaults_name}.next.XXXXXX")"
      rm -f -- "$defaults_next_link"
      if [ -L "$defaults_dir" ]; then
        old_defaults_release="$(readlink "$defaults_dir")"
        ln -s "$(basename "$dashboard_staging_dir")" "$defaults_next_link"
        node -e 'require("node:fs").renameSync(process.argv[1], process.argv[2])' \
          "$defaults_next_link" "$defaults_dir"
      elif [ -e "$defaults_dir" ]; then
        if [ -e "$defaults_legacy_dir" ] || [ -L "$defaults_legacy_dir" ]; then
          echo "Refusing to overwrite dashboard-defaults recovery path: $defaults_legacy_dir" >&2
          exit 1
        fi
        mv -- "$defaults_dir" "$defaults_legacy_dir"
        ln -s "$(basename "$dashboard_staging_dir")" "$defaults_next_link"
        if ! node -e 'require("node:fs").renameSync(process.argv[1], process.argv[2])' \
          "$defaults_next_link" "$defaults_dir"; then
          mv -- "$defaults_legacy_dir" "$defaults_dir"
          exit 1
        fi
      else
        ln -s "$(basename "$dashboard_staging_dir")" "$defaults_next_link"
        node -e 'require("node:fs").renameSync(process.argv[1], process.argv[2])' \
          "$defaults_next_link" "$defaults_dir"
      fi
      defaults_next_link=""
      dashboard_staging_dir=""
      dashboard_dir="$defaults_dir"

      if [ -d "$defaults_legacy_dir" ]; then
        rm -rf -- "$defaults_legacy_dir"
      fi
      case "$old_defaults_release" in
        ".${defaults_name}.build."*)
          rm -rf -- "${defaults_parent:?}/${old_defaults_release:?}"
          ;;
      esac
      echo "==> Dashboard defaults generated and published: $defaults_dir -> $(readlink "$defaults_dir")"
    fi
    wiki_econ_record_stage_event completed dashboard_defaults "" \
      "$(( ($(date +%s) - DASHBOARD_DEFAULTS_STAGE_STARTED_EPOCH) * 1000 ))"
    DASHBOARD_DEFAULTS_STAGE_ACTIVE=0
  else
    dashboard_staging_dir="$(mktemp -d "$dist_parent/.${dist_name}.dashboard.${site_build_run_id}.XXXXXX")"
    wiki_econ_run_cli dashboard-materialize --destination-dir "$dashboard_staging_dir"
    cp "$WIKI_ECON_OUTPUT_DIR/manifest.json" "$dashboard_staging_dir/manifest.json"
    dashboard_dir="$dashboard_staging_dir"
  fi

  node "$ROOT/scripts/prepare-site-source.cjs" \
    "$WIKI_ECON_SITE_DIR/src" \
    "$source_dir" \
    "$dashboard_dir" \
    "$site_vendor_cache" \
    "$WIKI_ECON_OUTPUT_DIR/manifest.json"

  offline_guard="$ROOT/scripts/deny-network.cjs"
  (cd "$WIKI_ECON_ROOT" && \
    NODE_OPTIONS="--require=$offline_guard${NODE_OPTIONS:+ $NODE_OPTIONS}" \
    WIKI_ECON_SITE_SOURCE_DIR="$source_dir" \
    WIKI_ECON_SITE_DIST_DIR="$build_dir" \
    "$WIKI_ECON_ROOT/node_modules/.bin/observable" build --config "$WIKI_ECON_SITE_DIR/observablehq.config.js")
  node "$ROOT/scripts/publish-browser-data.cjs" "$WIKI_ECON_OUTPUT_DIR" "$build_dir"
  node "$ROOT/scripts/verify-site-dependencies.cjs" "$build_dir"
else
  (cd "$WIKI_ECON_ROOT" && WIKI_ECON_SITE_DIST_DIR="$build_dir" \
    "$WIKI_ECON_ROOT/node_modules/.bin/observable" build --config "$WIKI_ECON_SITE_DIR/observablehq.config.js")
  node "$ROOT/scripts/publish-browser-data.cjs" "$WIKI_ECON_OUTPUT_DIR" "$build_dir"
fi

if [ ! -f "$build_dir/inequality.html" ]; then
  echo "Observable build did not produce $build_dir/inequality.html" >&2
  exit 1
fi

# Close the validation-to-publication race: generators must not change any
# artifact while Observable is building the staged release.
verify_publication_receipt

old_release=""
if [ -L "$dist_dir" ]; then
  old_release="$(readlink "$dist_dir")"
  ln -s "$(basename "$build_dir")" "$next_link"
  node -e 'require("node:fs").renameSync(process.argv[1], process.argv[2])' \
    "$next_link" "$dist_dir"
elif [ -e "$dist_dir" ]; then
  if [ -e "$legacy_dir" ] || [ -L "$legacy_dir" ]; then
    echo "Refusing to overwrite site publication recovery path: $legacy_dir" >&2
    exit 1
  fi
  mv -- "$dist_dir" "$legacy_dir"
  ln -s "$(basename "$build_dir")" "$next_link"
  if ! node -e 'require("node:fs").renameSync(process.argv[1], process.argv[2])' \
    "$next_link" "$dist_dir"; then
    mv -- "$legacy_dir" "$dist_dir"
    exit 1
  fi
else
  ln -s "$(basename "$build_dir")" "$next_link"
  node -e 'require("node:fs").renameSync(process.argv[1], process.argv[2])' \
    "$next_link" "$dist_dir"
fi

if [ -d "$legacy_dir" ]; then
  rm -rf -- "$legacy_dir"
fi
case "$old_release" in
  ".${dist_name}.build."*)
    rm -rf -- "${dist_parent:?}/${old_release:?}"
    ;;
esac

if [ "${WIKI_ECON_REQUIRE_PUBLICATION_GATE:-0}" = "1" ]; then
  wiki_econ_run_cli site-fingerprint-record \
    --site-dir "$WIKI_ECON_SITE_DIR" \
    --dist-dir "$WIKI_ECON_SITE_DIST_DIR"
fi

wiki_econ_record_stage_event completed site "" "$(( ($(date +%s) - SITE_STAGE_STARTED_EPOCH) * 1000 ))"
SITE_STAGE_FINISHED=1
echo "==> Site build complete: $dist_dir -> $(readlink "$dist_dir")"
