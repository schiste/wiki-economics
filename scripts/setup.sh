#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
. "$ROOT/scripts/lib/wiki_econ.sh"

YES=0
SKIP_SYSTEM_PACKAGES=0
SKIP_QUALITY_TOOLS=0
SKIP_BUILD=0
APT_UPDATED=0

if [ -t 1 ]; then
  BOLD="$(printf '\033[1m')"
  BLUE="$(printf '\033[34m')"
  GREEN="$(printf '\033[32m')"
  YELLOW="$(printf '\033[33m')"
  RED="$(printf '\033[31m')"
  RESET="$(printf '\033[0m')"
else
  BOLD=""
  BLUE=""
  GREEN=""
  YELLOW=""
  RED=""
  RESET=""
fi

usage() {
  cat <<'EOF'
Usage: ./scripts/setup.sh [options]

Bootstraps a local wiki-economics development environment.

Options:
  -y, --yes                 Run non-interactively where possible
      --skip-system-packages
                            Do not attempt to install missing OS-level packages
      --skip-quality-tools  Skip cargo-llvm-cov, cargo-deny, and cargo-audit
      --skip-build          Skip cargo build and the dashboard build
  -h, --help                Show this help message
EOF
}

say() {
  printf "%s==>%s %s\n" "$BLUE" "$RESET" "$1"
}

celebrate() {
  printf "%s[ok]%s %s\n" "$GREEN" "$RESET" "$1"
}

warn() {
  printf "%s[warn]%s %s\n" "$YELLOW" "$RESET" "$1"
}

die() {
  printf "%s[error]%s %s\n" "$RED" "$RESET" "$1" >&2
  exit 1
}

banner() {
  cat <<EOF
${BOLD}wiki-economics local setup${RESET}
Consulting the librarians, dusting the stacks, and lining up the toolchain.
EOF
}

confirm() {
  local prompt="$1"
  if [ "$YES" -eq 1 ]; then
    return 0
  fi

  printf "%s [y/N] " "$prompt"
  read -r reply || true
  case "$reply" in
    [Yy]|[Yy][Ee][Ss]) return 0 ;;
    *) return 1 ;;
  esac
}

have() {
  command -v "$1" >/dev/null 2>&1
}

run_with_sudo() {
  if [ "$(id -u)" -eq 0 ]; then
    "$@"
  else
    sudo "$@"
  fi
}

detect_package_manager() {
  if have brew; then
    printf "brew"
    return
  fi
  if have apt-get; then
    printf "apt-get"
    return
  fi
  printf "none"
}

apt_install() {
  if [ "$APT_UPDATED" -eq 0 ]; then
    run_with_sudo apt-get update
    APT_UPDATED=1
  fi
  run_with_sudo apt-get install -y "$@"
}

brew_install() {
  brew install "$@"
}

source_cargo_env() {
  export PATH="$HOME/.cargo/bin:$PATH"
  if [ -f "$HOME/.cargo/env" ]; then
    # shellcheck disable=SC1090
    . "$HOME/.cargo/env"
  fi
}

install_missing_system_packages() {
  local manager="$1"
  local -a packages=()

  if ! have curl; then
    case "$manager" in
      brew) packages+=("curl") ;;
      apt-get) packages+=("curl") ;;
    esac
  fi

  if ! have python3; then
    case "$manager" in
      brew) packages+=("python") ;;
      apt-get) packages+=("python3") ;;
    esac
  fi

  if ! have node || ! have npm; then
    case "$manager" in
      brew) packages+=("node") ;;
      apt-get) packages+=("nodejs" "npm") ;;
    esac
  fi

  if ! have cc; then
    case "$manager" in
      apt-get) packages+=("build-essential") ;;
    esac
  fi

  if ! have duckdb; then
    case "$manager" in
      brew) packages+=("duckdb") ;;
      apt-get) packages+=("duckdb") ;;
    esac
  fi

  if [ "${#packages[@]}" -eq 0 ]; then
    celebrate "System packages already look complete."
    return
  fi

  say "Missing system tools detected: ${packages[*]}"

  if [ "$SKIP_SYSTEM_PACKAGES" -eq 1 ]; then
    warn "Skipping system package installation by request."
    return
  fi

  if ! confirm "Install missing system packages with ${manager}?"; then
    warn "Skipping automatic system package installation."
    return
  fi

  case "$manager" in
    brew) brew_install "${packages[@]}" ;;
    apt-get)
      if ! apt_install "${packages[@]}"; then
        warn "Some apt packages could not be installed automatically."
      fi
      ;;
    *)
      warn "No supported package manager available for automatic installs."
      ;;
  esac
}

ensure_rust_toolchain() {
  source_cargo_env

  if ! have rustup || ! have cargo; then
    say "Rust stable is missing. We'll bring in rustup."

    if ! have curl; then
      die "curl is required to install rustup automatically."
    fi

    if ! confirm "Install Rust stable with rustup?"; then
      die "Rust is required to build wiki-economics."
    fi

    curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain stable --profile minimal
    source_cargo_env
  fi

  say "Ensuring the Rust toolchain matches the repo."
  rustup default stable >/dev/null
  rustup component add rustfmt clippy >/dev/null
  celebrate "Rust stable, rustfmt, and clippy are ready."
}

ensure_command() {
  local command_name="$1"
  local label="$2"
  if have "$command_name"; then
    celebrate "$label is available."
    return
  fi
  die "$label is still missing. Install it and rerun ./scripts/setup.sh."
}

ensure_cargo_tool() {
  local binary="$1"
  local crate="$2"

  if have "$binary"; then
    celebrate "$crate is available."
    return
  fi

  say "Installing $crate with cargo."
  cargo install "$crate"
  celebrate "$crate is ready."
}

install_site_dependencies() {
  if [ -d "$WIKI_ECON_SITE_DIR/node_modules" ]; then
    celebrate "Dashboard dependencies are already installed."
  else
    say "Installing dashboard dependencies."
    (cd "$WIKI_ECON_SITE_DIR" && npm ci)
    celebrate "Dashboard dependencies are installed."
  fi
}

prepare_local_directories() {
  say "Preparing local data directories."
  wiki_econ_ensure_local_dirs
  celebrate "Local data directories are ready."
}

build_project() {
  if [ "$SKIP_BUILD" -eq 1 ]; then
    warn "Skipping builds by request."
    return
  fi

  say "Building the Rust CLI."
  cargo build --release
  celebrate "Rust CLI build completed."

  say "Building the Observable dashboard."
  "$ROOT/scripts/build-site.sh" \
    --output-dir "$WIKI_ECON_OUTPUT_DIR" \
    --dist-dir "$WIKI_ECON_SITE_DIST_DIR"
  celebrate "Dashboard build completed."
}

print_next_steps() {
  cat <<EOF

${BOLD}Setup complete.${RESET}
Next useful commands:

  scripts/dev.sh
  scripts/refresh.sh frwiki
  scripts/build-site.sh
  cargo run --release -- fetch frwiki
  cargo run --release -- ingest frwiki
  cargo run --release -- compute frwiki
  cargo run --release -- merge

Notes:
- The admin picker lives at http://127.0.0.1:3000/admin once scripts/dev.sh is running.
- This setup installs the software stack, not Wikimedia datasets.
- If you want the full contributor toolchain, rerun without --skip-quality-tools.
EOF
}

main() {
  local package_manager

  while [ "$#" -gt 0 ]; do
    case "$1" in
      -y|--yes)
        YES=1
        ;;
      --skip-system-packages)
        SKIP_SYSTEM_PACKAGES=1
        ;;
      --skip-quality-tools)
        SKIP_QUALITY_TOOLS=1
        ;;
      --skip-build)
        SKIP_BUILD=1
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        usage
        die "Unknown option: $1"
        ;;
    esac
    shift
  done

  banner

  package_manager="$(detect_package_manager)"
  wiki_econ_init_runtime
  if [ "$package_manager" = "none" ]; then
    warn "No supported package manager detected. Automatic OS-level installs are disabled."
  else
    say "Detected package manager: $package_manager"
    install_missing_system_packages "$package_manager"
  fi

  ensure_rust_toolchain
  ensure_command python3 "Python 3"
  ensure_command node "Node.js"
  ensure_command npm "npm"
  ensure_command duckdb "DuckDB CLI"

  if [ "$SKIP_QUALITY_TOOLS" -eq 0 ]; then
    say "Installing contributor quality tools."
    ensure_cargo_tool cargo-llvm-cov cargo-llvm-cov
    ensure_cargo_tool cargo-deny cargo-deny
    ensure_cargo_tool cargo-audit cargo-audit
  else
    warn "Skipping contributor quality tools by request."
  fi

  install_site_dependencies
  prepare_local_directories
  build_project
  print_next_steps
}

main "$@"
