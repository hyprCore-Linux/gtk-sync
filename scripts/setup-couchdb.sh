#!/usr/bin/env bash
# Standalone CouchDB helper. Prefer: ./install.sh (server) which does this automatically.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# Re-use install.sh ensure path by sourcing isn't easy; keep a thin docker-only helper.
CONTAINER="${MIMIC_COUCH_CONTAINER:-gtk-sync-couchdb}"
PORT="${MIMIC_COUCH_PORT:-5984}"
USER="${MIMIC_COUCH_USER:-admin}"
PASS="${MIMIC_COUCH_PASSWORD:-mimicadmin}"
DB="${MIMIC_COUCH_DB:-gtk-sync}"

if [[ $EUID -ne 0 ]]; then
  echo "Re-run with sudo (or use ./install.sh for a full server install)." >&2
  exec sudo env MIMIC_COUCH_PASSWORD="$PASS" MIMIC_COUCH_USER="$USER" \
    MIMIC_COUCH_DB="$DB" MIMIC_COUCH_PORT="$PORT" bash "$0"
fi

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  echo "Installing Docker…"
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  apt-get install -y -qq docker.io || curl -fsSL https://get.docker.com | sh
  systemctl enable --now docker
fi

if docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  docker start "$CONTAINER"
else
  docker pull couchdb:3
  docker run -d --name "$CONTAINER" --restart unless-stopped \
    -p "127.0.0.1:${PORT}:5984" \
    -e "COUCHDB_USER=$USER" -e "COUCHDB_PASSWORD=$PASS" \
    couchdb:3
fi

echo -n "Waiting for CouchDB"
for _ in $(seq 1 90); do
  curl -sf -u "$USER:$PASS" "http://127.0.0.1:${PORT}/" >/dev/null 2>&1 && echo " ready." && break
  echo -n "."; sleep 0.5
done
curl -sf -u "$USER:$PASS" -X PUT "http://127.0.0.1:${PORT}/${DB}" >/dev/null 2>&1 || true
echo "CouchDB at http://127.0.0.1:${PORT}  db=$DB  user=$USER"
