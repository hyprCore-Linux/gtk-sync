#!/usr/bin/env bash
# Local smoke test: 2 servers + 1 client on loopback.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORKDIR="${TMPDIR:-/tmp}/mimic-smoke-$$"
mkdir -p "$WORKDIR"
cleanup() {
  kill $(jobs -p) 2>/dev/null || true
  wait 2>/dev/null || true
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

echo "== Building =="
cargo build -q --manifest-path "$ROOT/Cargo.toml" -p gtk-sync -p gtk-sync-client
SRV="$ROOT/target/debug/gtk-sync"
CLI="$ROOT/target/debug/gtk-sync-client"

echo "== CouchDB =="
MIMIC_COUCH_PASSWORD=secret MIMIC_COUCH_USER=admin bash "$ROOT/scripts/setup-couchdb.sh"
export MIMIC_COUCH_URL=http://127.0.0.1:5984
export MIMIC_COUCH_USER=admin
export MIMIC_COUCH_PASSWORD=secret
export MIMIC_COUCH_DB=mimicfs

A_ROOT="$WORKDIR/server-a"
B_ROOT="$WORKDIR/server-b"
C_ROOT="$WORKDIR/client"
mkdir -p "$A_ROOT" "$B_ROOT" "$C_ROOT"

echo "== Installing servers =="
MIMIC_COUCH_URL="$MIMIC_COUCH_URL" MIMIC_COUCH_USER="$MIMIC_COUCH_USER" \
  MIMIC_COUCH_PASSWORD="$MIMIC_COUCH_PASSWORD" MIMIC_COUCH_DB="$MIMIC_COUCH_DB" \
  "$SRV" install --non-interactive --root "$A_ROOT" --config "$WORKDIR/a.toml" \
  --username mimic --password secret --port 18443 --retention-hours 24 \
  --instance-name smoke-a
MIMIC_COUCH_URL="$MIMIC_COUCH_URL" MIMIC_COUCH_USER="$MIMIC_COUCH_USER" \
  MIMIC_COUCH_PASSWORD="$MIMIC_COUCH_PASSWORD" MIMIC_COUCH_DB=mimicfs-b \
  "$SRV" install --non-interactive --root "$B_ROOT" --config "$WORKDIR/b.toml" \
  --username mimic --password secret --port 18444 --retention-hours 24 \
  --instance-name smoke-b
# ensure second db exists
curl -sf -u admin:secret -X PUT http://127.0.0.1:5984/mimicfs-b >/dev/null 2>&1 || true

echo "== Starting servers =="
"$SRV" run --config "$WORKDIR/a.toml" &
"$SRV" run --config "$WORKDIR/b.toml" &
sleep 1

for port in 18443 18444; do
  ok=0
  for _ in $(seq 1 40); do
    if curl -sk "https://127.0.0.1:$port/v1/health" | grep -q '"ok":true'; then
      echo "server :$port up"
      ok=1
      break
    fi
    sleep 0.15
  done
  [[ "$ok" -eq 1 ]] || { echo "server :$port failed"; exit 1; }
done

echo "== Client setup =="
"$CLI" setup --non-interactive --root "$C_ROOT" --config "$WORKDIR/client.toml" \
  --username mimic --password secret \
  --peers "127.0.0.1:18443,127.0.0.1:18444" \
  --no-auto-discover

echo "== Push todo.txt =="
echo "hello v1" >"$C_ROOT/todo.txt"
RUST_LOG=gtk_sync_client=info "$CLI" run --config "$WORKDIR/client.toml" &
CLIENT_PID=$!

AUTH=$(printf 'mimic:secret' | base64 -w0)
wait_index() {
  local port=$1
  for _ in $(seq 1 50); do
    IDX=$(curl -sk -H "Authorization: Basic $AUTH" "https://127.0.0.1:$port/v1/index" || true)
    if echo "$IDX" | grep -q todo.txt; then
      echo "index :$port ok: $IDX"
      return 0
    fi
    sleep 0.2
  done
  echo "timeout waiting for index on :$port ($IDX)"
  return 1
}
wait_index 18443
wait_index 18444

# Confirm version files live under storage
ls "$A_ROOT/versions"/todo.txt-* >/dev/null

echo "== Edit and re-sync =="
echo "hello v2" >"$C_ROOT/todo.txt"
V2_HASH=$(printf 'hello v2\n' | sha256sum | awk '{print $1}')
for _ in $(seq 1 50); do
  VERS=$(curl -sk -H "Authorization: Basic $AUTH" \
    "https://127.0.0.1:18443/v1/versions?path=todo.txt")
  if echo "$VERS" | grep -q "$V2_HASH"; then
    echo "versions: $VERS"
    break
  fi
  sleep 0.25
done
echo "$VERS" | grep -q "$V2_HASH" || { echo "v2 hash not found in versions: $VERS"; exit 1; }
COUNT=$(echo "$VERS" | grep -o '"ts"' | wc -l)
[[ "$COUNT" -ge 2 ]] || { echo "expected >=2 versions, got $COUNT"; exit 1; }

TS1=$(echo "$VERS" | python3 -c "import sys,json; v=json.load(sys.stdin)['versions']; print(sorted(v,key=lambda x:x['ts'])[0]['ts'])")

echo "== Restore older version ts=$TS1 =="
kill "$CLIENT_PID" 2>/dev/null || true
wait "$CLIENT_PID" 2>/dev/null || true
"$CLI" restore --config "$WORKDIR/client.toml" todo.txt "$TS1"
grep -q "hello v1" "$C_ROOT/todo.txt"

echo "== Delete file =="
RUST_LOG=gtk_sync_client=info "$CLI" run --config "$WORKDIR/client.toml" &
CLIENT_PID=$!
sleep 1
rm -f "$C_ROOT/todo.txt"
for _ in $(seq 1 40); do
  IDX_A=$(curl -sk -H "Authorization: Basic $AUTH" "https://127.0.0.1:18443/v1/index")
  if echo "$IDX_A" | python3 -c "import sys,json; d=json.load(sys.stdin); raise SystemExit(0 if d.get('tombstones') or not any(f['path']=='todo.txt' for f in d.get('files',[])) else 1)"; then
    echo "index after delete: $IDX_A"
    break
  fi
  sleep 0.25
done

echo "SMOKE OK"
