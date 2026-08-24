#!/usr/bin/env bash

wiki_econ_repo_root() {
  local root
  root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
  while [ "$root" != "/" ] && [ ! -f "$root/Cargo.toml" ]; do
    root="$(dirname "$root")"
  done
  printf '%s\n' "$root"
}

wiki_econ_abs_path() {
  local root="$1"
  local value="$2"
  case "$value" in
    /*) printf '%s\n' "$value" ;;
    *) printf '%s\n' "$root/$value" ;;
  esac
}

wiki_econ_init_runtime() {
  WIKI_ECON_ROOT="${WIKI_ECON_ROOT:-$(wiki_econ_repo_root)}"
  WIKI_ECON_ENV="${WIKI_ECON_ENV:-local}"
  WIKI_ECON_DATA_DIR="$(wiki_econ_abs_path "$WIKI_ECON_ROOT" "${WIKI_ECON_DATA_DIR:-data}")"
  WIKI_ECON_OUTPUT_DIR="$(wiki_econ_abs_path "$WIKI_ECON_ROOT" "${WIKI_ECON_OUTPUT_DIR:-output}")"
  WIKI_ECON_GENERATOR_DIR="$(wiki_econ_abs_path "$WIKI_ECON_ROOT" "${WIKI_ECON_GENERATOR_DIR:-site/data-build}")"
  WIKI_ECON_WIKI_LIFECYCLE_FILE="$(wiki_econ_abs_path "$WIKI_ECON_ROOT" "${WIKI_ECON_WIKI_LIFECYCLE_FILE:-config/wiki-lifecycle.json}")"
  WIKI_ECON_SITE_DIR="$(wiki_econ_abs_path "$WIKI_ECON_ROOT" "${WIKI_ECON_SITE_DIR:-site}")"
  WIKI_ECON_SITE_DIST_DIR="$(wiki_econ_abs_path "$WIKI_ECON_ROOT" "${WIKI_ECON_SITE_DIST_DIR:-site/dist}")"
  WIKI_ECON_SITE_PORT="${WIKI_ECON_SITE_PORT:-3000}"
  WIKI_ECON_ADMIN_PORT="${WIKI_ECON_ADMIN_PORT:-3001}"

  export WIKI_ECON_ROOT
  export WIKI_ECON_ENV
  export WIKI_ECON_DATA_DIR
  export WIKI_ECON_OUTPUT_DIR
  export WIKI_ECON_GENERATOR_DIR
  export WIKI_ECON_WIKI_LIFECYCLE_FILE
  export WIKI_ECON_SITE_DIR
  export WIKI_ECON_SITE_DIST_DIR
  export WIKI_ECON_SITE_PORT
  export WIKI_ECON_ADMIN_PORT

  wiki_econ_init_binary_provenance
}

wiki_econ_init_binary_provenance() {
  if [ -z "${WIKI_ECON_BIN:-}" ]; then
    return 0
  fi
  if [ ! -x "$WIKI_ECON_BIN" ]; then
    if [ "$WIKI_ECON_ENV" = "production" ]; then
      echo "Production binary is missing or not executable: $WIKI_ECON_BIN" >&2
      return 1
    fi
    return 0
  fi

  local binary_dir provenance_file source_commit recorded_binary_sha actual_binary_sha
  binary_dir="$(cd "$(dirname "$WIKI_ECON_BIN")" && pwd -P)"
  provenance_file="$binary_dir/release-provenance.json"
  if [ ! -f "$provenance_file" ]; then
    if [ "$WIKI_ECON_ENV" = "production" ]; then
      echo "Production binary has no release provenance: $provenance_file" >&2
      return 1
    fi
    return 0
  fi

  # The JavaScript template expression is intentionally evaluated by Node,
  # not expanded by the shell.
  # shellcheck disable=SC2016
  read -r source_commit recorded_binary_sha < <(node -e '
    const fs = require("node:fs");
    const value = JSON.parse(fs.readFileSync(process.argv[1], "utf8"));
    process.stdout.write(`${value.source_commit || ""} ${value.binary?.sha256 || ""}\n`);
  ' "$provenance_file")
  if [[ ! "$source_commit" =~ ^[0-9a-f]{40}$ || ! "$recorded_binary_sha" =~ ^[0-9a-f]{64}$ ]]; then
    echo "Production release provenance has invalid commit or binary checksum" >&2
    return 1
  fi

  if command -v sha256sum >/dev/null 2>&1; then
    actual_binary_sha="$(sha256sum "$WIKI_ECON_BIN" | awk '{print $1}')"
  else
    actual_binary_sha="$(shasum -a 256 "$WIKI_ECON_BIN" | awk '{print $1}')"
  fi
  [ "$actual_binary_sha" = "$recorded_binary_sha" ] || {
    echo "Production binary checksum does not match release provenance" >&2
    return 1
  }
  [ -z "${WIKI_ECON_SOURCE_COMMIT:-}" ] || [ "$WIKI_ECON_SOURCE_COMMIT" = "$source_commit" ] || {
    echo "Configured source commit does not match binary provenance" >&2
    return 1
  }
  [ -z "${WIKI_ECON_BINARY_SHA256:-}" ] || [ "$WIKI_ECON_BINARY_SHA256" = "$actual_binary_sha" ] || {
    echo "Configured binary checksum does not match the deployed binary" >&2
    return 1
  }
  [ -z "${WIKI_ECON_IMAGE_SOURCE_COMMIT:-}" ] || [ "$WIKI_ECON_IMAGE_SOURCE_COMMIT" = "$source_commit" ] || {
    echo "Binary and image source commits disagree" >&2
    return 1
  }

  WIKI_ECON_SOURCE_COMMIT="$source_commit"
  WIKI_ECON_BINARY_SHA256="$actual_binary_sha"
  export WIKI_ECON_SOURCE_COMMIT WIKI_ECON_BINARY_SHA256
}

wiki_econ_print_runtime() {
  cat <<EOF
Environment:  $WIKI_ECON_ENV
Repo root:    $WIKI_ECON_ROOT
Data dir:     $WIKI_ECON_DATA_DIR
Output dir:   $WIKI_ECON_OUTPUT_DIR
Generators:   $WIKI_ECON_GENERATOR_DIR
Wiki registry: $WIKI_ECON_WIKI_LIFECYCLE_FILE
Site dir:     $WIKI_ECON_SITE_DIR
Site dist:    $WIKI_ECON_SITE_DIST_DIR
EOF
}

wiki_econ_ensure_output_mount() {
  local repo_output="$WIKI_ECON_ROOT/output"

  mkdir -p "$WIKI_ECON_OUTPUT_DIR"

  if [ "$WIKI_ECON_OUTPUT_DIR" = "$repo_output" ]; then
    mkdir -p "$repo_output"
    return 0
  fi

  if [ -L "$repo_output" ]; then
    ln -sfn "$WIKI_ECON_OUTPUT_DIR" "$repo_output"
    return 0
  fi

  if [ -d "$repo_output" ] && [ -z "$(find "$repo_output" -mindepth 1 -maxdepth 1 2>/dev/null)" ]; then
    rmdir "$repo_output"
  elif [ -e "$repo_output" ]; then
    echo "Refusing to replace existing non-empty $repo_output; either use the default output dir or clear that path first." >&2
    return 1
  fi

  ln -s "$WIKI_ECON_OUTPUT_DIR" "$repo_output"
}

wiki_econ_ensure_local_dirs() {
  mkdir -p \
    "$WIKI_ECON_DATA_DIR/raw" \
    "$WIKI_ECON_DATA_DIR/parquet" \
    "$WIKI_ECON_DATA_DIR/warehouse" \
    "$WIKI_ECON_DATA_DIR/patrol" \
    "$WIKI_ECON_OUTPUT_DIR"
  wiki_econ_ensure_output_mount
}

wiki_econ_ensure_site_deps() {
  if [ -x "$WIKI_ECON_ROOT/node_modules/.bin/observable" ]; then
    return 0
  fi
  if [ "$WIKI_ECON_ENV" = "production" ]; then
    echo "Observable dependencies are missing from the production image; rebuild the Toolforge image instead of installing during refresh." >&2
    return 1
  fi
  (cd "$WIKI_ECON_ROOT" && npm ci)
}

wiki_econ_cli_label() {
  if [ -n "${WIKI_ECON_BIN:-}" ]; then
    printf '%s' "$WIKI_ECON_BIN"
  else
    printf '%s' "cargo run --release --locked --"
  fi
}

wiki_econ_run_cli() {
  local -a cmd

  if [ -n "${WIKI_ECON_BIN:-}" ]; then
    cmd=(
      "$WIKI_ECON_BIN"
      --data-dir "$WIKI_ECON_DATA_DIR"
      --output-dir "$WIKI_ECON_OUTPUT_DIR"
    )
  else
    cmd=(
      cargo run --release --locked --
      --data-dir "$WIKI_ECON_DATA_DIR"
      --output-dir "$WIKI_ECON_OUTPUT_DIR"
    )
  fi

  if [ -n "${WIKI_ECON_RUN_ID:-}" ]; then
    cmd+=(--run-id "$WIKI_ECON_RUN_ID")
  fi
  cmd+=("$@")

  printf '==> %s' "${cmd[0]}"
  for arg in "${cmd[@]:1}"; do
    printf ' %q' "$arg"
  done
  printf '\n'
  "${cmd[@]}"
}

wiki_econ_record_stage_event() {
  local event=$1 stage=$2 wiki=${3:-} duration_ms=${4:-} error=${5:-}
  if [ -z "${WIKI_ECON_RUN_EVENTS_FILE:-}" ]; then
    return 0
  fi
  node "${WIKI_ECON_RUN_RECORD_HELPER:-$WIKI_ECON_ROOT/deploy/toolforge/run-record.cjs}" \
    event "$event" "$stage" "$wiki" "$duration_ms" "$error"
}
