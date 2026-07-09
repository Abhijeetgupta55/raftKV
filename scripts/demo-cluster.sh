#!/usr/bin/env bash
# Scripted 3-node raftkv demo: launch a cluster, write through the leader,
# kill a node, watch the CLI keep working across failover, then restart the
# node. Runs the whole thing in one terminal (good for a README GIF); the
# manual 3-terminal version is printed at the top for copy-paste.
#
# Usage: scripts/demo-cluster.sh
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=bin
mkdir -p "$BIN"
go build -o "$BIN/kvserver" ./cmd/server
go build -o "$BIN/kvcli" ./cmd/cli

PEERS="1@127.0.0.1:5501,2@127.0.0.1:5502,3@127.0.0.1:5503"
ADDRS="127.0.0.1:5501,127.0.0.1:5502,127.0.0.1:5503"
DATA="$(mktemp -d)"
PIDS=()

cat <<EOF
# ---- manual 3-terminal version -------------------------------------------
# Terminal 1: $BIN/kvserver --id 1 --listen 127.0.0.1:5501 --peers $PEERS --data-dir d/1
# Terminal 2: $BIN/kvserver --id 2 --listen 127.0.0.1:5502 --peers $PEERS --data-dir d/2
# Terminal 3: $BIN/kvserver --id 3 --listen 127.0.0.1:5503 --peers $PEERS --data-dir d/3
# Client:     $BIN/kvcli -addr $ADDRS put hello world
#             $BIN/kvcli -addr $ADDRS get hello
# --------------------------------------------------------------------------
EOF

cleanup() { for p in "${PIDS[@]:-}"; do kill -9 "$p" 2>/dev/null || true; done; rm -rf "$DATA"; }
trap cleanup EXIT

start() { # start <id>
  "$BIN/kvserver" --id "$1" --listen "127.0.0.1:550$1" --peers "$PEERS" --data-dir "$DATA/$1" \
    >"$DATA/$1.log" 2>&1 &
  PIDS[$1]=$!
}

echo "== starting 3 nodes =="
for i in 1 2 3; do start "$i"; done
sleep 2

echo "== writing 5 keys through the leader (CLI follows hints) =="
for i in 1 2 3 4 5; do "$BIN/kvcli" -addr "$ADDRS" -client 1 -serial "$i" put "key$i" "val$i"; done

echo "== reading them back =="
for i in 1 2 3 4 5; do printf 'key%s = ' "$i"; "$BIN/kvcli" -addr "$ADDRS" get "key$i"; done

echo "== killing node 1 (kill -9) — cluster must keep serving =="
kill -9 "${PIDS[1]}" 2>/dev/null || true
sleep 1

echo "== writing 3 more keys during the outage =="
for i in 6 7 8; do "$BIN/kvcli" -addr "$ADDRS" -client 1 -serial "$i" put "key$i" "val$i"; done

echo "== all 8 keys still readable (zero loss) =="
for i in 1 2 3 4 5 6 7 8; do printf 'key%s = ' "$i"; "$BIN/kvcli" -addr "$ADDRS" get "key$i"; done

echo "== restarting node 1 — it rejoins and catches up =="
start 1
sleep 2
"$BIN/kvcli" -addr "$ADDRS" -client 1 -serial 9 put afterrejoin yes
printf 'afterrejoin = '; "$BIN/kvcli" -addr "$ADDRS" get afterrejoin

echo "== demo complete: 3-node cluster survived a kill -9 with no data loss =="
