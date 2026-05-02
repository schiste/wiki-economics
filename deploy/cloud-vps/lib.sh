#!/usr/bin/env bash

wiki_econ_cloud_load_env() {
  WIKI_ECON_ENV_FILE="${WIKI_ECON_ENV_FILE:-/etc/wiki-economics.env}"
  if [ -f "$WIKI_ECON_ENV_FILE" ]; then
    # shellcheck disable=SC1090
    . "$WIKI_ECON_ENV_FILE"
  fi

  WIKI_ECON_REPO_URL="${WIKI_ECON_REPO_URL:-https://github.com/schiste/wiki-economics.git}"
  WIKI_ECON_REPO_REF="${WIKI_ECON_REPO_REF:-main}"
  WIKI_ECON_APP_ROOT="${WIKI_ECON_APP_ROOT:-/srv/wiki-economics/app}"
  WIKI_ECON_APP_RELEASES="${WIKI_ECON_APP_RELEASES:-$WIKI_ECON_APP_ROOT/releases}"
  WIKI_ECON_APP_CURRENT="${WIKI_ECON_APP_CURRENT:-$WIKI_ECON_APP_ROOT/current}"
  WIKI_ECON_DATA_DIR="${WIKI_ECON_DATA_DIR:-/srv/wiki-economics/data}"
  WIKI_ECON_OUTPUT_ROOT="${WIKI_ECON_OUTPUT_ROOT:-/srv/wiki-economics/output}"
  WIKI_ECON_OUTPUT_RELEASES="${WIKI_ECON_OUTPUT_RELEASES:-$WIKI_ECON_OUTPUT_ROOT/releases}"
  WIKI_ECON_OUTPUT_CURRENT="${WIKI_ECON_OUTPUT_CURRENT:-$WIKI_ECON_OUTPUT_ROOT/current}"
  WIKI_ECON_SITE_ROOT="${WIKI_ECON_SITE_ROOT:-/srv/wiki-economics/site}"
  WIKI_ECON_SITE_RELEASES="${WIKI_ECON_SITE_RELEASES:-$WIKI_ECON_SITE_ROOT/releases}"
  WIKI_ECON_SITE_CURRENT="${WIKI_ECON_SITE_CURRENT:-$WIKI_ECON_SITE_ROOT/current}"
  WIKI_ECON_STATE_ROOT="${WIKI_ECON_STATE_ROOT:-/srv/wiki-economics/state}"
  WIKI_ECON_ENABLED_WIKIS_FILE="${WIKI_ECON_ENABLED_WIKIS_FILE:-/etc/wiki-economics/wikis.txt}"
  WIKI_ECON_SERVICE_USER="${WIKI_ECON_SERVICE_USER:-wiki-econ}"
  WIKI_ECON_ENV="${WIKI_ECON_ENV:-production}"

  export WIKI_ECON_ENV_FILE
  export WIKI_ECON_REPO_URL
  export WIKI_ECON_REPO_REF
  export WIKI_ECON_APP_ROOT
  export WIKI_ECON_APP_RELEASES
  export WIKI_ECON_APP_CURRENT
  export WIKI_ECON_DATA_DIR
  export WIKI_ECON_OUTPUT_ROOT
  export WIKI_ECON_OUTPUT_RELEASES
  export WIKI_ECON_OUTPUT_CURRENT
  export WIKI_ECON_SITE_ROOT
  export WIKI_ECON_SITE_RELEASES
  export WIKI_ECON_SITE_CURRENT
  export WIKI_ECON_STATE_ROOT
  export WIKI_ECON_ENABLED_WIKIS_FILE
  export WIKI_ECON_SERVICE_USER
  export WIKI_ECON_ENV
}

wiki_econ_cloud_require_service_user() {
  local script_path="$1"
  shift || true

  if [ "$(id -un)" = "$WIKI_ECON_SERVICE_USER" ]; then
    return 0
  fi

  if [ "$(id -u)" -eq 0 ]; then
    case "$script_path" in
      /*) ;;
      *) script_path="$(pwd)/$script_path" ;;
    esac
    exec runuser -u "$WIKI_ECON_SERVICE_USER" -- "$script_path" "$@"
  fi

  echo "Run this command as $WIKI_ECON_SERVICE_USER (for example: sudo -u $WIKI_ECON_SERVICE_USER -H $script_path $*)." >&2
  exit 1
}

wiki_econ_cloud_prepare_toolchain_env() {
  local service_home
  service_home="$(getent passwd "$WIKI_ECON_SERVICE_USER" | cut -d: -f6)"
  export HOME="${HOME:-$service_home}"
  export PATH="$service_home/.cargo/bin:$PATH"
  if [ -f "$service_home/.cargo/env" ]; then
    # shellcheck disable=SC1090
    . "$service_home/.cargo/env"
  fi
}

wiki_econ_cloud_require_file() {
  local path="$1"
  if [ ! -f "$path" ]; then
    echo "Required file is missing: $path" >&2
    exit 1
  fi
}

wiki_econ_cloud_release_stamp() {
  date -u +%Y%m%dT%H%M%SZ
}

wiki_econ_cloud_switch_symlink() {
  local link_path="$1"
  local target_path="$2"
  local tmp_link="${link_path}.new"

  mkdir -p "$(dirname "$link_path")"
  rm -f "$tmp_link"
  ln -s "$target_path" "$tmp_link"
  mv -Tf "$tmp_link" "$link_path"
}

wiki_econ_cloud_has_merged_artifacts() {
  local root="$1"
  find "$root" -maxdepth 1 -type f -name '*.parquet' | grep -q .
}

wiki_econ_cloud_write_status() {
  # Build the status JSON via printf to avoid the trap of a heredoc that
  # silently embeds unescaped quotes if release_name ever contains them.
  local output_path="$1"
  local release_name="$2"
  local timestamp
  timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  if command -v jq >/dev/null 2>&1; then
    jq -n --arg release "$release_name" --arg updated_at "$timestamp" \
      '{release: $release, updated_at: $updated_at}' >"$output_path"
  else
    # jq is not installed on every operator host; fall back to a printf
    # that escapes embedded double quotes in release_name. This branch is
    # the historical heredoc behavior with the bare-minimum hardening.
    local escaped="${release_name//\"/\\\"}"
    printf '{"release":"%s","updated_at":"%s"}\n' "$escaped" "$timestamp" \
      >"$output_path"
  fi
}
