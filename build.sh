#!/usr/bin/env bash
# Build this folder and install it into the hyprCore / KvNix trees for the next ISO.
# Rust, Python, and Meson are picked from the folder. Flags: --local  --no-local
set -euo pipefail
APP_ROOT="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=../share/build-install.sh
source "$APP_ROOT/../share/build-install.sh"
gtk_app_build_install "$APP_ROOT" "$@"
