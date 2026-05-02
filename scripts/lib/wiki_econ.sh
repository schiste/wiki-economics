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
  WIKI_ECON_SITE_DIR="$(wiki_econ_abs_path "$WIKI_ECON_ROOT" "${WIKI_ECON_SITE_DIR:-site}")"
  WIKI_ECON_SITE_DIST_DIR="$(wiki_econ_abs_path "$WIKI_ECON_ROOT" "${WIKI_ECON_SITE_DIST_DIR:-site/dist}")"
  WIKI_ECON_SITE_PORT="${WIKI_ECON_SITE_PORT:-3000}"
  WIKI_ECON_ADMIN_PORT="${WIKI_ECON_ADMIN_PORT:-3001}"

  export WIKI_ECON_ROOT
  export WIKI_ECON_ENV
  export WIKI_ECON_DATA_DIR
  export WIKI_ECON_OUTPUT_DIR
  export WIKI_ECON_GENERATOR_DIR
  export WIKI_ECON_SITE_DIR
  export WIKI_ECON_SITE_DIST_DIR
  export WIKI_ECON_SITE_PORT
  export WIKI_ECON_ADMIN_PORT
}

wiki_econ_print_runtime() {
  cat <<EOF
Environment:  $WIKI_ECON_ENV
Repo root:    $WIKI_ECON_ROOT
Data dir:     $WIKI_ECON_DATA_DIR
Output dir:   $WIKI_ECON_OUTPUT_DIR
Generators:   $WIKI_ECON_GENERATOR_DIR
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
  if [ ! -d "$WIKI_ECON_SITE_DIR/node_modules" ]; then
    (cd "$WIKI_ECON_SITE_DIR" && npm ci)
  fi
}

wiki_econ_cli_label() {
  if [ -n "${WIKI_ECON_BIN:-}" ]; then
    printf '%s' "$WIKI_ECON_BIN"
  else
    printf '%s' "cargo run --release --"
  fi
}

wiki_econ_run_cli() {
  local -a cmd

  if [ -n "${WIKI_ECON_BIN:-}" ]; then
    cmd=(
      "$WIKI_ECON_BIN"
      --data-dir "$WIKI_ECON_DATA_DIR"
      --output-dir "$WIKI_ECON_OUTPUT_DIR"
      "$@"
    )
  else
    cmd=(
      cargo run --release --
      --data-dir "$WIKI_ECON_DATA_DIR"
      --output-dir "$WIKI_ECON_OUTPUT_DIR"
      "$@"
    )
  fi

  printf '==> %s' "${cmd[0]}"
  for arg in "${cmd[@]:1}"; do
    printf ' %q' "$arg"
  done
  printf '\n'
  "${cmd[@]}"
}
