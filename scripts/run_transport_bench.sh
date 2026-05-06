#!/usr/bin/env zsh
set -euo pipefail
cd "$(dirname "$0")/.."

SSH_USER="${SSH_USER:-ubuntu}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/ssh-key-1770215555284}"
ITERATIONS="${ITERATIONS:-1000}"
INFRA_DIR="infra"

mkdir -p results/transport

SIGNAL_IP=$(cd $INFRA_DIR && tofu output -raw signal_server_public_ip)
NODE_INTERNAL_JSON=$(cd $INFRA_DIR && tofu output -json node_internal_ips)
NODE_IPS=($(echo $NODE_INTERNAL_JSON | python3 -c "import json,sys; print('\n'.join(json.load(sys.stdin)))"))
SERVER=${NODE_IPS[1]}
CLIENT=${NODE_IPS[2]}

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=30 -i "$SSH_KEY")
JUMP_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=30 -i "$SSH_KEY" -o "ProxyJump=$SSH_USER@$SIGNAL_IP")

echo "server=$SERVER  client=$CLIENT  (via jump $SIGNAL_IP)"

BLOB_DIR="/tmp/bench_blobs"

run_bench() {
  local transport=$1
  local mode=$2
  local payload=$3
  local tag="${transport}_${mode}_${payload}"

  echo "=== $tag ==="

  ssh "${JUMP_OPTS[@]}" $SSH_USER@$SERVER "pkill -f bench_transport || true" 2>/dev/null || true

  SERVER_PORT=7000
  SIG_PORT=8100

  if [[ $transport == "webrtc" ]]; then
    ssh "${JUMP_OPTS[@]}" $SSH_USER@$SERVER \
      "nohup ~/bench_transport -role=server -transport=webrtc -addr=0.0.0.0 -port=$SERVER_PORT -sig-port=$SIG_PORT -mode=$mode -payload=$payload -blob-dir=$BLOB_DIR > /tmp/bench_server.log 2>&1 &"
  else
    ssh "${JUMP_OPTS[@]}" $SSH_USER@$SERVER \
      "nohup ~/bench_transport -role=server -transport=udp -addr=0.0.0.0 -port=$SERVER_PORT -mode=$mode -payload=$payload -blob-dir=$BLOB_DIR > /tmp/bench_server.log 2>&1 &"
  fi

  sleep 2

  SERVER_INTERNAL=$(cd $INFRA_DIR && tofu output -json node_internal_ips | python3 -c "import json,sys; print(json.load(sys.stdin)[0])")

  if [[ $transport == "webrtc" ]]; then
    ssh "${JUMP_OPTS[@]}" $SSH_USER@$CLIENT \
      "~/bench_transport -role=client -transport=webrtc -addr=0.0.0.0 -port=7001 -remote-addr=$SERVER_INTERNAL -remote-port=$SERVER_PORT -sig-port=8101 -remote-sig-port=$SIG_PORT -mode=$mode -payload=$payload -iterations=$ITERATIONS -out=/tmp/result_${tag}.json"
  else
    ssh "${JUMP_OPTS[@]}" $SSH_USER@$CLIENT \
      "~/bench_transport -role=client -transport=udp -addr=0.0.0.0 -port=7001 -remote-addr=$SERVER_INTERNAL -remote-port=$SERVER_PORT -mode=$mode -payload=$payload -iterations=$ITERATIONS -out=/tmp/result_${tag}.json"
  fi

  scp "${JUMP_OPTS[@]}" $SSH_USER@$CLIENT:/tmp/result_${tag}.json results/transport/

  ssh "${JUMP_OPTS[@]}" $SSH_USER@$SERVER "pkill -f bench_transport || true" 2>/dev/null || true
  sleep 1
}

for transport in udp webrtc; do
  run_bench $transport ping 0
  for payload in 64 256 1024; do
    run_bench $transport store $payload
    run_bench $transport findvalue $payload
  done
  for payload in 65536 524288 1048576 4194304; do
    run_bench $transport blob $payload
  done
done

echo "transport benchmark done. results in results/transport/"
ls results/transport/

