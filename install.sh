#!/usr/bin/env bash
# GTK-Sync installer — pick server or client, sync/storage folder, password, server address.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
PREFIX="${PREFIX:-/usr/local}"
DEFAULT_PORT=8443
# System storage for the sync server (versions + TLS). Client sync roots stay user-chosen.
DEFAULT_SERVER_FOLDER="/var/lib/gtk-sync"

die() { echo "$*" >&2; exit 1; }

have_zenity() { command -v zenity >/dev/null 2>&1; }

find_cargo() {
  if command -v cargo >/dev/null 2>&1; then
    command -v cargo
    return
  fi
  local u="${SUDO_USER:-$USER}"
  local home
  home="$(getent passwd "$u" | cut -d: -f6)"
  if [[ -x "$home/.cargo/bin/cargo" ]]; then
    echo "$home/.cargo/bin/cargo"
    return
  fi
  return 1
}

parse_host_port() {
  local raw="$1"
  if [[ "$raw" =~ ^(.+):([0-9]+)$ ]]; then
    HOST="${BASH_REMATCH[1]}"
    PORT="${BASH_REMATCH[2]}"
  else
    HOST="$raw"
    PORT="$DEFAULT_PORT"
  fi
  [[ -n "$HOST" ]] || return 1
}

prompt_cli() {
  echo "GTK-Sync setup"
  echo "1) Setup server"
  echo "2) Setup client"
  read -r -p "Choice [1/2]: " mode
  case "$mode" in
    1|server|s|S) MODE=server ;;
    2|client|c|C) MODE=client ;;
    *) die "Invalid choice" ;;
  esac
  if [[ "$MODE" == server ]]; then
    read -r -p "Storage folder [${DEFAULT_SERVER_FOLDER}]: " folder
    folder="${folder:-$DEFAULT_SERVER_FOLDER}"
  else
    read -r -p "Folder to sync: " folder
    [[ -n "$folder" ]] || die "Folder required"
  fi
  FOLDER="$folder"
  read -r -p "Username: " username
  USERNAME="$username"
  [[ -n "$USERNAME" ]] || die "Username required"
  read -r -s -p "Password: " password
  echo
  PASSWORD="$password"
  [[ -n "$PASSWORD" ]] || die "Password required"
  if [[ "$MODE" == client ]]; then
    read -r -p "Server IP or domain (optional :port) [127.0.0.1:${DEFAULT_PORT}]: " server
    server="${server:-127.0.0.1:$DEFAULT_PORT}"
    parse_host_port "$server" || die "Bad server address"
    SERVER_HOST="$HOST"
    SERVER_PORT="$PORT"
  else
    read -r -p "Listen port [${DEFAULT_PORT}]: " port
    SERVER_PORT="${port:-$DEFAULT_PORT}"
  fi
}

prompt_zenity_role() {
  # Prefer Neuronix Settings-style card dialog when available.
  local choice=""
  if [[ -f /usr/share/neuronix/neuronix_choice_dialog.py ]]; then
    choice="$(
      python3 /usr/share/neuronix/neuronix_choice_dialog.py \
        "Setup Sync" \
        "Choose how this computer will use Sync — each option continues setup." \
        "server|Server|Host the sync library on this machine" \
        "client|Client|Sync a folder to an existing server" \
        2>/dev/null || true
    )"
    case "$choice" in
      server) MODE=server; return 0 ;;
      client) MODE=client; return 0 ;;
      "") die "Cancelled" ;;
    esac
  fi

  choice="$(zenity --list --radiolist \
    --title="Setup Sync" \
    --width=520 --height=320 \
    --text="Choose how this computer will use Sync.

A server keeps the shared library. A client syncs a folder to that server." \
    --hide-header \
    --column="Pick" --column="Role" --column=" " \
    TRUE "Server" "Host the sync library on this machine" \
    FALSE "Client" "Sync a folder to an existing server")" || die "Cancelled"
  case "$choice" in
    Server|"Setup server") MODE=server ;;
    Client|"Setup client") MODE=client ;;
    *) die "Cancelled" ;;
  esac
}

prompt_zenity() {
  local folder username password server port

  if [[ -n "${GTK_SYNC_MODE:-}" ]]; then
    case "${GTK_SYNC_MODE}" in
      server|client) MODE="$GTK_SYNC_MODE" ;;
      *) die "Invalid GTK_SYNC_MODE (use server or client)" ;;
    esac
  elif [[ -z "${MODE:-}" ]]; then
    prompt_zenity_role
  fi

  # Server defaults to system storage; client picks a user folder.
  if [[ "$MODE" == server ]]; then
    local use_default=1
    # Explicit start folder from the environment overrides the default location.
    if [[ -n "${GTK_SYNC_START_FOLDER:-}" ]]; then
      use_default=0
    elif have_zenity; then
      if zenity --question --title="Setup Sync" --width=480 \
        --text="Store the sync library in the system folder?

${DEFAULT_SERVER_FOLDER}

Recommended for a Sync server (versions + TLS certs).
Choose No to pick a different location."; then
        use_default=1
      else
        use_default=0
      fi
    fi
    if [[ "$use_default" -eq 1 ]]; then
      FOLDER="$DEFAULT_SERVER_FOLDER"
    else
      local start="${GTK_SYNC_START_FOLDER:-/var/lib}"
      [[ -d "$start" ]] || start="/"
      folder="$(zenity --file-selection --directory \
        --title="Choose a storage folder for the sync library" \
        --width=640 --height=480 \
        --filename="${start}/")" || die "Cancelled"
      FOLDER="$folder"
    fi
  else
    local start="${GTK_SYNC_START_FOLDER:-$HOME}"
    [[ -d "$start" ]] || start="$HOME"
    folder="$(zenity --file-selection --directory \
      --title="Choose the folder you want to sync" \
      --width=640 --height=480 \
      --filename="${start}/")" || die "Cancelled"
    FOLDER="$folder"
  fi

  username="$(zenity --entry --title="Setup Sync" --width=440 \
    --text="Username for this sync account:" \
    --entry-text="${USER:-}")" || die "Cancelled"
  USERNAME="$username"
  [[ -n "$USERNAME" ]] || die "Username required"

  password="$(zenity --password --title="Setup Sync" --width=440 \
    --text="Shared sync password

Use the same password on the server and every client.")" || die "Cancelled"
  PASSWORD="$password"
  [[ -n "$PASSWORD" ]] || die "Password required"

  if [[ "$MODE" == client ]]; then
    server="$(zenity --entry --title="Setup Sync" --width=440 \
      --text="Server address

IP or hostname. Add :port if it is not ${DEFAULT_PORT}." \
      --entry-text="192.168.1.10")" || die "Cancelled"
    parse_host_port "$server" || die "Bad server address"
    SERVER_HOST="$HOST"
    SERVER_PORT="$PORT"
  else
    port="$(zenity --entry --title="Setup Sync" --width=440 \
      --text="Port for the sync server to listen on:" \
      --entry-text="$DEFAULT_PORT")" || die "Cancelled"
    SERVER_PORT="${port:-$DEFAULT_PORT}"
  fi
}

build_as_user() {
  local pkg="$1"
  local bin="$ROOT/target/release/$pkg"
  # Neuronix / syn-to-devices ships prebuilt release binaries next to install.sh.
  if [[ -x "$bin" && "${GTK_SYNC_FORCE_BUILD:-0}" != "1" ]]; then
    echo "Using prebuilt $bin"
    return 0
  fi
  local cargo
  cargo="$(find_cargo)" || die "cargo not found — install Rust via https://rustup.rs first, then re-run ./install.sh (or ship target/release/$pkg)"
  echo "Building $pkg with $cargo …"
  if [[ $EUID -eq 0 && -n "${SUDO_USER:-}" ]]; then
    sudo -u "$SUDO_USER" -H env PATH="$(dirname "$cargo"):$PATH" \
      "$cargo" build --release -p "$pkg" --manifest-path "$ROOT/Cargo.toml"
  else
    "$cargo" build --release -p "$pkg" --manifest-path "$ROOT/Cargo.toml"
  fi
}

# --- Docker + CouchDB (server metadata) ---

ensure_docker() {
  [[ $EUID -eq 0 ]] || die "ensure_docker needs root"
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    echo "Docker: OK"
    return 0
  fi

  echo "Installing Docker (needed for CouchDB metadata store)…"
  if command -v apt-get >/dev/null 2>&1; then
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq
    apt-get install -y -qq ca-certificates curl gnupg >/dev/null
    # Prefer distro package for simplicity on Debian/Ubuntu
    if apt-get install -y -qq docker.io docker-compose-v2 2>/dev/null \
      || apt-get install -y -qq docker.io 2>/dev/null; then
      :
    else
      # Fallback: official convenience script
      curl -fsSL https://get.docker.com | sh
    fi
  elif command -v dnf >/dev/null 2>&1; then
    dnf install -y docker
  else
    curl -fsSL https://get.docker.com | sh
  fi

  systemctl enable --now docker 2>/dev/null || service docker start 2>/dev/null || true

  # Let the installing user use docker without sudo next time
  if [[ -n "${SUDO_USER:-}" && "${SUDO_USER}" != root ]]; then
    usermod -aG docker "$SUDO_USER" 2>/dev/null || true
  fi

  # Brief wait for daemon
  for _ in $(seq 1 30); do
    if docker info >/dev/null 2>&1; then
      echo "Docker: installed and running"
      return 0
    fi
    sleep 0.5
  done
  die "Docker installed but daemon is not responding. Try: systemctl status docker"
}

ensure_couchdb() {
  # Must run as root (uses docker). Uses mesh password as CouchDB admin password.
  [[ $EUID -eq 0 ]] || die "ensure_couchdb needs root"
  local container="${MIMIC_COUCH_CONTAINER:-gtk-sync-couchdb}"
  local port="${MIMIC_COUCH_PORT:-5984}"
  local user="${MIMIC_COUCH_USER:-admin}"
  local pass="${MIMIC_COUCH_PASSWORD:-$PASSWORD}"
  local db="${MIMIC_COUCH_DB:-gtk-sync}"

  [[ -n "$pass" ]] || die "CouchDB password empty"

  export MIMIC_COUCH_URL="http://127.0.0.1:${port}"
  export MIMIC_COUCH_USER="$user"
  export MIMIC_COUCH_PASSWORD="$pass"
  export MIMIC_COUCH_DB="$db"

  couch_up() {
    curl -sf -u "$user:$pass" "http://127.0.0.1:${port}/" >/dev/null 2>&1
  }

  if couch_up; then
    curl -sf -u "$user:$pass" -X PUT "http://127.0.0.1:${port}/${db}" >/dev/null 2>&1 || true
    echo "CouchDB: already running at ${MIMIC_COUCH_URL} (db=${db})"
    return 0
  fi

  ensure_docker

  if docker ps -a --format '{{.Names}}' | grep -qx "$container"; then
    echo "Starting CouchDB container ($container)…"
    docker start "$container" >/dev/null || true
    for _ in $(seq 1 40); do
      couch_up && break
      sleep 0.5
    done
    if couch_up; then
      curl -sf -u "$user:$pass" -X PUT "http://127.0.0.1:${port}/${db}" >/dev/null 2>&1 || true
      echo "CouchDB: ${MIMIC_COUCH_URL}  user=${user}  db=${db}"
      return 0
    fi
    echo "Existing CouchDB rejected this password — recreating container…"
    docker rm -f "$container" >/dev/null 2>&1 || true
  fi

  echo "Creating CouchDB container ($container) on port ${port}…"
  docker pull couchdb:3 >/dev/null
  docker run -d \
    --name "$container" \
    --restart unless-stopped \
    -p "127.0.0.1:${port}:5984" \
    -e "COUCHDB_USER=$user" \
    -e "COUCHDB_PASSWORD=$pass" \
    couchdb:3 >/dev/null

  echo -n "Waiting for CouchDB"
  local ok=0
  for _ in $(seq 1 90); do
    if couch_up; then
      echo " ready."
      ok=1
      break
    fi
    echo -n "."
    sleep 0.5
  done
  [[ "$ok" -eq 1 ]] || die "CouchDB did not become ready. Check: docker logs $container"

  curl -sf -u "$user:$pass" -X PUT "http://127.0.0.1:${port}/${db}" >/dev/null 2>&1 || true
  echo "CouchDB: ${MIMIC_COUCH_URL}  user=${user}  db=${db}"
}

install_server() {
  if [[ $EUID -ne 0 ]]; then
    elevate_as_root server
    exit $?
  fi

  echo "== GTK-Sync server install =="
  build_as_user gtk-sync
  install -Dm755 "$ROOT/target/release/gtk-sync" "$PREFIX/bin/gtk-sync"

  local run_user run_group
  run_user="${SUDO_USER:-${MIMIC_RUN_USER:-}}"
  [[ -n "$run_user" && "$run_user" != root ]] || die "Server install must be run via sudo from a normal user session (./install.sh)"
  run_group="$(id -gn "$run_user")"

  mkdir -p "$FOLDER" /etc/gtk-sync
  chown -R "$run_user:$run_group" "$FOLDER"

  echo "== Docker + CouchDB =="
  # Same mesh password unlocks CouchDB admin (streamlined; one password to remember)
  MIMIC_COUCH_PASSWORD="${MIMIC_COUCH_PASSWORD:-$PASSWORD}"
  MIMIC_COUCH_USER="${MIMIC_COUCH_USER:-admin}"
  MIMIC_COUCH_DB="${MIMIC_COUCH_DB:-gtk-sync}"
  ensure_couchdb

  CFG=/etc/gtk-sync/server.toml
  if [[ -f "$CFG" ]]; then
    echo "Config exists: $CFG (not overwritten)."
    if ! grep -q '^couch_url' "$CFG" 2>/dev/null; then
      {
        echo "couch_url = \"${MIMIC_COUCH_URL}\""
        echo "couch_db = \"${MIMIC_COUCH_DB}\""
        echo "couch_user = \"${MIMIC_COUCH_USER}\""
        echo "couch_password = \"${MIMIC_COUCH_PASSWORD}\""
      } >>"$CFG"
      echo "Appended CouchDB settings to $CFG"
    fi
  else
    local tmp_cfg
    tmp_cfg="$(sudo -u "$run_user" -H mktemp /tmp/gtk-sync-server.XXXXXX.toml)"
    sudo -u "$run_user" -H env \
      MIMIC_COUCH_URL="$MIMIC_COUCH_URL" \
      MIMIC_COUCH_DB="$MIMIC_COUCH_DB" \
      MIMIC_COUCH_USER="$MIMIC_COUCH_USER" \
      MIMIC_COUCH_PASSWORD="$MIMIC_COUCH_PASSWORD" \
      "$PREFIX/bin/gtk-sync" install \
      --non-interactive \
      --root "$FOLDER" \
      --config "$tmp_cfg" \
      --username "$USERNAME" \
      --password "$PASSWORD" \
      --port "$SERVER_PORT" \
      --retention-hours 24
    install -o root -g root -m 644 "$tmp_cfg" "$CFG"
    rm -f "$tmp_cfg"
    chown -R "$run_user:$run_group" "$FOLDER"
  fi

  sed -e "s|__MIMIC_USER__|$run_user|g" -e "s|__MIMIC_GROUP__|$run_group|g" \
    "$ROOT/host/systemd/gtk-sync.service" >/etc/systemd/system/gtk-sync.service

  systemctl daemon-reload
  systemctl enable --now docker 2>/dev/null || true
  docker start "${MIMIC_COUCH_CONTAINER:-gtk-sync-couchdb}" 2>/dev/null || true
  sleep 1
  systemctl enable --now gtk-sync

  echo
  echo "Server installed successfully."
  echo "  User:     $run_user"
  echo "  Storage:  $FOLDER  (versions/ + TLS certs)"
  echo "  Metadata: CouchDB ${MIMIC_COUCH_URL} / ${MIMIC_COUCH_DB}"
  echo "  Config:   $CFG"
  echo "  Listen:   https://0.0.0.0:${SERVER_PORT}"
  echo "  Status:   systemctl status gtk-sync"
  echo "  Logs:     journalctl -u gtk-sync -f"
  if [[ -n "${SUDO_USER:-}" ]]; then
    echo
    echo "Note: $SUDO_USER was added to the docker group (log out/in for docker without sudo)."
  fi
}

# Re-exec this script as root (prompts for sudo / admin password).
elevate_as_root() {
  local role="$1"
  echo "Administrator privileges required for ${role} install…"
  local env_args=(
    MIMIC_MODE="$MODE"
    MIMIC_FOLDER="$FOLDER"
    MIMIC_USERNAME="$USERNAME"
    MIMIC_PASSWORD="$PASSWORD"
    MIMIC_SERVER_HOST="${SERVER_HOST:-}"
    MIMIC_SERVER_PORT="${SERVER_PORT:-$DEFAULT_PORT}"
    PREFIX="$PREFIX"
    DISPLAY="${DISPLAY:-}"
    WAYLAND_DISPLAY="${WAYLAND_DISPLAY:-}"
    XAUTHORITY="${XAUTHORITY:-}"
  )

  if have_zenity && [[ -n "${DISPLAY:-}${WAYLAND_DISPLAY:-}" ]]; then
    local pw
    pw="$(zenity --password --title="sudo password" \
      --text="Enter your sudo (login) password to install the GTK-Sync server.\nThis is not the sync username/password from the previous step.")" \
      || die "Cancelled (sudo password required)"
    # shellcheck disable=SC2086
    printf '%s\n' "$pw" | sudo -S -E env "${env_args[@]}" bash "$ROOT/install.sh" --from-env
    return $?
  fi

  # Terminal: sudo prompts for password interactively
  exec sudo -E env "${env_args[@]}" bash "$ROOT/install.sh" --from-env
}

install_client() {
  if [[ $EUID -eq 0 ]]; then
    [[ -n "${SUDO_USER:-}" ]] || die "Do not run client install as root without sudo from a user session"
    exec sudo -u "$SUDO_USER" -H env \
      MIMIC_MODE=client \
      MIMIC_FOLDER="$FOLDER" \
      MIMIC_USERNAME="$USERNAME" \
      MIMIC_PASSWORD="$PASSWORD" \
      MIMIC_SERVER_HOST="$SERVER_HOST" \
      MIMIC_SERVER_PORT="$SERVER_PORT" \
      PREFIX="$PREFIX" \
      bash "$ROOT/install.sh" --from-env
  fi

  build_as_user gtk-sync-client

  local bin
  if [[ -w "$PREFIX/bin" ]] || [[ $EUID -eq 0 ]]; then
    install -Dm755 "$ROOT/target/release/gtk-sync-client" "$PREFIX/bin/gtk-sync-client"
    bin="$PREFIX/bin/gtk-sync-client"
  else
    mkdir -p "$HOME/.local/bin"
    install -Dm755 "$ROOT/target/release/gtk-sync-client" "$HOME/.local/bin/gtk-sync-client"
    bin="$HOME/.local/bin/gtk-sync-client"
    echo "Installed to $bin (add ~/.local/bin to PATH if needed)"
  fi

  mkdir -p "$FOLDER" "$HOME/.config/gtk-sync"
  CFG="$HOME/.config/gtk-sync/client.toml"
  "$bin" setup --non-interactive \
    --root "$FOLDER" \
    --config "$CFG" \
    --username "$USERNAME" \
    --password "$PASSWORD" \
    --peers "${SERVER_HOST}:${SERVER_PORT}" \
    --no-auto-discover

  UNIT_DIR="$HOME/.config/systemd/user"
  mkdir -p "$UNIT_DIR"
  sed "s|/usr/local/bin/gtk-sync-client|$bin|g" \
    "$ROOT/client/systemd/gtk-sync-client.service" >"$UNIT_DIR/gtk-sync-client.service"
  systemctl --user daemon-reload
  systemctl --user enable --now gtk-sync-client || true
  echo
  echo "Client installed."
  echo "  Sync folder: $FOLDER"
  echo "  Server:      ${SERVER_HOST}:${SERVER_PORT}"
  echo "  Config:      $CFG"
  echo "  Status:      systemctl --user status gtk-sync-client"
}

# --- entry ---
MODE=""
FOLDER=""
USERNAME=""
PASSWORD=""
SERVER_HOST=""
SERVER_PORT="$DEFAULT_PORT"

if [[ "${1:-}" == "--from-env" ]]; then
  MODE="${MIMIC_MODE:?}"
  FOLDER="${MIMIC_FOLDER:?}"
  USERNAME="${MIMIC_USERNAME:?}"
  PASSWORD="${MIMIC_PASSWORD:?}"
  SERVER_HOST="${MIMIC_SERVER_HOST:-}"
  SERVER_PORT="${MIMIC_SERVER_PORT:-$DEFAULT_PORT}"
elif [[ "${1:-}" == "--server" || "${1:-}" == "--client" ]]; then
  [[ "${1}" == "--server" ]] && MODE=server || MODE=client
  shift
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --folder) FOLDER="$2"; shift 2 ;;
      --username) USERNAME="$2"; shift 2 ;;
      --password) PASSWORD="$2"; shift 2 ;;
      --server-host) SERVER_HOST="$2"; shift 2 ;;
      --port) SERVER_PORT="$2"; shift 2 ;;
      *) die "Unknown arg: $1" ;;
    esac
  done
  if [[ "$MODE" == server ]]; then
    FOLDER="${FOLDER:-$DEFAULT_SERVER_FOLDER}"
  fi
  [[ -n "$FOLDER" && -n "$PASSWORD" ]] || die "Need --password (and --folder for client)"
  if [[ "$MODE" == client ]]; then
    [[ -n "$SERVER_HOST" ]] || die "Client needs --server-host"
  fi
  USERNAME="${USERNAME:-${SUDO_USER:-${USER:-}}}"
  [[ -n "$USERNAME" ]] || die "Need --username"
else
  if have_zenity && [[ -n "${DISPLAY:-}${WAYLAND_DISPLAY:-}" ]]; then
    prompt_zenity
  else
    prompt_cli
  fi
fi

case "$MODE" in
  server) install_server ;;
  client) install_client ;;
  *) die "No mode selected" ;;
esac
