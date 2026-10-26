#!/usr/bin/env bash
# Remove GTK-Sync server and/or client install so you can start over.
set -euo pipefail

die() { echo "$*" >&2; exit 1; }

have_zenity() { command -v zenity >/dev/null 2>&1; }

ask_what() {
  if have_zenity && [[ -n "${DISPLAY:-}${WAYLAND_DISPLAY:-}" ]]; then
    local choice
    choice="$(zenity --list --checklist --title="GTK-Sync Uninstall" --width=420 --height=240 \
      --text="What should be removed?" \
      --column="Pick" --column="Component" \
      TRUE "Server (systemd + binary + /etc/gtk-sync)" \
      TRUE "Client (user service + binary + config)" \
      FALSE "CouchDB container (gtk-sync-couchdb)" \
      FALSE "Client sync folders (only config; not your files)")" || die "Cancelled"
    echo "$choice"
  else
    echo "1) Server + client (keep storage / CouchDB)"
    echo "2) Server + client + remove CouchDB container"
    echo "3) Server only"
    echo "4) Client only"
    read -r -p "Choice [1-4]: " c
    case "$c" in
      1) echo "Server (systemd + binary + /etc/gtk-sync)|Client (user service + binary + config)" ;;
      2) echo "Server (systemd + binary + /etc/gtk-sync)|Client (user service + binary + config)|CouchDB container (gtk-sync-couchdb)" ;;
      3) echo "Server (systemd + binary + /etc/gtk-sync)" ;;
      4) echo "Client (user service + binary + config)" ;;
      *) die "Invalid choice" ;;
    esac
  fi
}

remove_client() {
  echo "== Removing client =="
  # User-level server instance (used during troubleshooting / same-machine tests)
  systemctl --user disable --now gtk-sync 2>/dev/null || true
  rm -f "$HOME/.config/systemd/user/gtk-sync.service"
  systemctl --user disable --now gtk-sync-client 2>/dev/null || true
  rm -f "$HOME/.config/systemd/user/gtk-sync-client.service"
  systemctl --user daemon-reload 2>/dev/null || true
  rm -f "$HOME/.local/bin/gtk-sync-client"
  [[ $EUID -eq 0 ]] && rm -f /usr/local/bin/gtk-sync-client || sudo rm -f /usr/local/bin/gtk-sync-client 2>/dev/null || true
  rm -rf "$HOME/.config/gtk-sync"
  # Stale busy status left gtk-files stuck on "Syncing…"
  if [[ -n "${XDG_RUNTIME_DIR:-}" ]]; then
    rm -f "$XDG_RUNTIME_DIR/gtk-sync/status.json"
    rmdir "$XDG_RUNTIME_DIR/gtk-sync" 2>/dev/null || true
  fi
  echo "Client removed (sync folder files left in place)."
}

remove_server() {
  local wipe_data="$1"
  echo "== Removing server =="
  if [[ $EUID -ne 0 ]]; then
    echo "Need administrator privileges…"
    if have_zenity && [[ -n "${DISPLAY:-}${WAYLAND_DISPLAY:-}" ]]; then
      local pw
      pw="$(zenity --password --title="sudo password" \
        --text="Enter your sudo (login) password to uninstall the GTK-Sync server:")" \
        || die "Cancelled"
      printf '%s\n' "$pw" | sudo -S env MIMIC_WIPE_DATA="$wipe_data" bash "$0" --server-as-root
      return $?
    fi
    exec sudo env MIMIC_WIPE_DATA="$wipe_data" bash "$0" --server-as-root
  fi
  remove_server_root "$wipe_data"
}

remove_server_root() {
  local wipe_data="$1"
  systemctl disable --now gtk-sync 2>/dev/null || true
  rm -f /etc/systemd/system/gtk-sync.service
  systemctl daemon-reload
  rm -f /usr/local/bin/gtk-sync
  rm -rf /etc/gtk-sync
  if [[ "$wipe_data" == "1" || "$wipe_data" == "true" || "$wipe_data" == "yes" ]]; then
    docker rm -f gtk-sync-couchdb 2>/dev/null || true
    echo "Removed CouchDB container gtk-sync-couchdb (if present)."
  fi
  echo "Server removed."
}

if [[ "${1:-}" == "--server-as-root" ]]; then
  [[ $EUID -eq 0 ]] || die "must be root"
  remove_server_root "${MIMIC_WIPE_DATA:-0}"
  exit 0
fi

# Non-interactive / gtk-files helpers
if [[ "${1:-}" == "--client-only" ]]; then
  remove_client
  exit 0
fi
if [[ "${1:-}" == "--server-only" ]]; then
  # MIMIC_WIPE_DATA=1 also removes the CouchDB container
  remove_server "${MIMIC_WIPE_DATA:-0}"
  exit 0
fi

if [[ "${1:-}" == "--all" ]]; then
  remove_client
  remove_server 1
  exit 0
fi

SELECTION="$(ask_what)"
DO_SERVER=0
DO_CLIENT=0
WIPE=0
[[ "$SELECTION" == *Server\ \(systemd* ]] && DO_SERVER=1
[[ "$SELECTION" == *Client\ \(user* ]] && DO_CLIENT=1
[[ "$SELECTION" == *CouchDB* ]] && WIPE=1

[[ "$DO_CLIENT" -eq 1 ]] && remove_client
[[ "$DO_SERVER" -eq 1 ]] && remove_server "$WIPE"

echo
echo "Done. Reinstall with: ./install.sh"
