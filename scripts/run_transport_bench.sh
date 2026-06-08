#!/usr/bin/env zsh
set -euo pipefail
cd "$(dirname "$0")/.."

SSH_USER="${SSH_USER:-ubuntu}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/ssh-key-1770215555284}"
ITERATIONS="${ITERATIONS:-100}"
RESULTS_DIR="${RESULTS_DIR:-results/transport}"
INFRA_DIR="infra"
SIG_PORT=8300

mkdir -p "$RESULTS_DIR"

SIGNAL_IP=$(cd $INFRA_DIR && tofu output -raw signal_server_public_ip)
NODE_IPS_JSON=$(cd $INFRA_DIR && tofu output -json node_internal_ips)
NODE_IPS=($(echo $NODE_IPS_JSON | python3 -c "import json,sys; print('\n'.join(json.load(sys.stdin)))"))
SERVER=${NODE_IPS[1]}
CLIENT=${NODE_IPS[2]}

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
          -o LogLevel=ERROR -o ConnectTimeout=30 -o BatchMode=yes \
          -i "$SSH_KEY")
JUMP_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
           -o LogLevel=ERROR -o ConnectTimeout=30 -o BatchMode=yes \
           -i "$SSH_KEY" -o "ProxyJump=$SSH_USER@$SIGNAL_IP")

echo "server=$SERVER  client=$CLIENT  (via jump $SIGNAL_IP)"
echo "iterations=$ITERATIONS  results=$RESULTS_DIR"

echo "=== deploying bench_transport ==="
scp "${JUMP_OPTS[@]}" dist/bench_transport $SSH_USER@$SERVER:~/bench_transport
scp "${JUMP_OPTS[@]}" dist/bench_transport $SSH_USER@$CLIENT:~/bench_transport
ssh "${JUMP_OPTS[@]}" $SSH_USER@$SERVER "chmod +x ~/bench_transport"
ssh "${JUMP_OPTS[@]}" $SSH_USER@$CLIENT "chmod +x ~/bench_transport"

ssh "${JUMP_OPTS[@]}" $SSH_USER@$SERVER "pkill -f bench_transport || true" 2>/dev/null || true
ssh "${JUMP_OPTS[@]}" $SSH_USER@$CLIENT "pkill -f bench_transport || true" 2>/dev/null || true
sleep 1

echo "=== starting server on $SERVER ==="
ssh "${JUMP_OPTS[@]}" $SSH_USER@$SERVER \
  "nohup ~/bench_transport -role=server -sig=0.0.0.0:${SIG_PORT} \
   > /tmp/bench_transport_server.log 2>&1 &"

sleep 2

echo "=== running client on $CLIENT (iters=$ITERATIONS) ==="
OUT_REMOTE="/tmp/result_transport.json"

ssh "${JUMP_OPTS[@]}" $SSH_USER@$CLIENT \
  "~/bench_transport \
     -role=client \
     -sig=http://${SERVER}:${SIG_PORT} \
     -iters=${ITERATIONS} \
     -out=${OUT_REMOTE}"

echo "=== collecting results ==="
scp "${JUMP_OPTS[@]}" $SSH_USER@$CLIENT:${OUT_REMOTE} \
    "${RESULTS_DIR}/result_transport_webrtc.json"

echo ""
echo "=== quick summary ==="
python3 -c "
import json, numpy as np, sys

path = '${RESULTS_DIR}/result_transport_webrtc.json'
with open(path) as f:
    data = json.load(f)

from collections import defaultdict
by_size = defaultdict(list)
for r in data:
    by_size[r['size_bytes']].append(r)

for sz in sorted(by_size.keys()):
    rows = by_size[sz]
    mbps = [(r['transferred_bytes'] / (r['duration_ns']/1e9)) / 1e6 for r in rows]
    label = f'{sz//1024}KB' if sz < 1048576 else f'{sz//1048576}MB'
    print(f'  {label:6s}  n={len(mbps):3d}  '
          f'median={np.median(mbps):6.1f} MB/s  '
          f'p5={np.percentile(mbps,5):5.1f}  '
          f'p95={np.percentile(mbps,95):5.1f}')
"

ssh "${JUMP_OPTS[@]}" $SSH_USER@$SERVER "pkill -f bench_transport || true" 2>/dev/null || true

echo ""
echo "done. results: ${RESULTS_DIR}/result_transport_webrtc.json"
