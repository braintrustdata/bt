#!/bin/bash
set -euo pipefail

REPO="braintrustdata/bt"
BASE_URL="https://github.com/${REPO}/releases"

usage() {
  cat <<EOF
install.sh — install the Braintrust CLI (bt)

Usage:
  curl -fsSL https://raw.githubusercontent.com/${REPO}/main/install.sh | bash
  curl -fsSL ... | bash -s -- [OPTIONS]

Options:
  --canary             Install the latest canary build (latest main)
  --version <VERSION>  Install a specific version (e.g. 0.1.2)
  --help               Show this help message

Environment variables:
  BT_CHANNEL=canary    Same as --canary
  BT_VERSION=0.1.2     Same as --version 0.1.2

All other flags are passed through to the underlying installer.

Examples:
  curl -fsSL ... | bash                          # latest stable
  curl -fsSL ... | bash -s -- --canary           # latest canary
  curl -fsSL ... | bash -s -- --version 0.1.2    # pinned version
EOF
  exit 0
}

# --- Parse arguments ----------------------------------------------------------

channel="${BT_CHANNEL:-stable}"
version="${BT_VERSION:-}"
passthrough_args=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --canary) channel="canary"; shift ;;
    --version=*) version="${1#--version=}"; shift ;;
    --version)
      if [[ -z "${2:-}" ]]; then
        echo "Error: --version requires a value" >&2
        exit 1
      fi
      version="$2"; shift 2
      ;;
    --help) usage ;;
    *) passthrough_args="${passthrough_args} $1"; shift ;;
  esac
done

# Strip v prefix (accept both "0.1.2" and "v0.1.2")
version="${version#v}"

if [[ -n "$version" && "$channel" == "canary" ]]; then
  echo "Error: --canary and --version are mutually exclusive" >&2
  exit 1
fi

# --- Build installer URL ------------------------------------------------------

if [[ -n "$version" ]]; then
  installer_url="${BASE_URL}/download/v${version}/bt-installer.sh"
elif [[ "$channel" == "canary" ]]; then
  installer_url="${BASE_URL}/download/canary/bt-installer.sh"
else
  installer_url="${BASE_URL}/latest/download/bt-installer.sh"
fi

# --- Require curl -------------------------------------------------------------

if ! command -v curl >/dev/null 2>&1; then
  echo "Error: curl is required to install bt." >&2
  exit 1
fi

# --- Pre-flight check (pinned versions only) ----------------------------------

if [[ -n "$version" ]]; then
  status=$(curl -sSL -o /dev/null -w '%{http_code}' --head "$installer_url")
  if [[ "$status" == "404" ]]; then
    echo "Error: bt version ${version} not found." >&2
    echo "See available releases: ${BASE_URL}" >&2
    exit 1
  elif [[ "$status" != "200" ]]; then
    echo "Error: failed to verify version ${version} (HTTP ${status})" >&2
    exit 1
  fi
fi

# --- First-install detection --------------------------------------------------

cargo_bin="${CARGO_HOME:-$HOME/.cargo}/bin"
is_first_install=false
if [[ ! -f "${cargo_bin}/bt" ]]; then
  is_first_install=true
fi

# --- Install ------------------------------------------------------------------

label="stable"
if [[ -n "$version" ]]; then
  label="v${version}"
elif [[ "$channel" == "canary" ]]; then
  label="canary"
fi

cat <<'LOGO'

  ███  ███
███      ███
  ███  ███
███      ███
  ███  ███

LOGO
echo "Installing bt (${label})..."
echo ""

tmpfile=$(mktemp)
trap 'rm -f "$tmpfile"' EXIT

if ! curl -fsSL "$installer_url" -o "$tmpfile"; then
  echo "Error: failed to download installer" >&2
  exit 1
fi

# shellcheck disable=SC2086
bash "$tmpfile" $passthrough_args

# --- Post-install -------------------------------------------------------------

if [[ "$is_first_install" == true ]]; then
  echo ""
  echo "Run 'bt setup' to get started."
  echo ""
fi
