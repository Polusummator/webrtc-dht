#!/usr/bin/env zsh
set -euo pipefail
cd "$(dirname "$0")/.."

SSH_USER="${SSH_USER:-ubuntu}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/ssh-key-1770215555284}"
INFRA_DIR="infra"
RUNS="${RUNS:-1}"
ITERS="${ITERS:-50}"
PAYLOAD="${PAYLOAD:-256}"
COORD_PORT=9200

mkdir -p results/dht

SIGNAL_IP=$(cd $INFRA_DIR && tofu output -raw signal_server_public_ip)
SIGNAL_INTERNAL=$(cd $INFRA_DIR && tofu output -raw signal_server_internal_ip)
BOOTSTRAP_IP=$(cd $INFRA_DIR && tofu output -raw bootstrap_public_ip)
BOOTSTRAP_INTERNAL=$(cd $INFRA_DIR && tofu output -raw bootstrap_internal_ip)
NODE_INTERNAL_JSON=$(cd $INFRA_DIR && tofu output -json node_internal_ips)
NODE_IPS=($(echo $NODE_INTERNAL_JSON | python3 -c "import json,sys; print('\n'.join(json.load(sys.stdin)))"))

SSH_BASE="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o GlobalKnownHostsFile=/dev/null -o BatchMode=yes -o ConnectTimeout=15 -o LogLevel=ERROR -o ServerAliveInterval=10 -o ServerAliveCountMax=3"
SSH_OPTS=($=SSH_BASE -i "$SSH_KEY")
JUMP_OPTS=($=SSH_BASE -i "$SSH_KEY" -o "ProxyCommand=ssh $SSH_BASE -i $SSH_KEY -W %h:%p $SSH_USER@$SIGNAL_IP")

ssh_start() {
  local retries=4 delay=3 i=0
  until ssh "$@"; do
    i=$(( i + 1 ))
    if (( i >= retries )); then echo "ssh_start failed after $retries attempts" >&2; return 1; fi
    sleep $delay
  done
}

ssh_once() { ssh "$@" 2>/dev/null; }

scp_retry() {
  local retries=5 delay=3 i=0
  until scp "$@"; do
    i=$(( i + 1 ))
    if (( i >= retries )); then echo "scp failed after $retries attempts" >&2; return 1; fi
    sleep $delay
  done
}

N_VMS=${#NODE_IPS[@]}
BOOTSTRAP_ADDR="${BOOTSTRAP_INTERNAL}:7100"

NODE_COUNTS=(2 5 10 20 50)

MAX_NODES=$(( N_VMS * 20 ))
NODE_COUNTS=(${NODE_COUNTS[@]:#*})
for n in 2 5 10 20 50 100 200; do
  if (( n <= MAX_NODES )); then
    NODE_COUNTS+=($n)
  fi
done

cleanup_all() {
  for ip in $NODE_IPS; do
    timeout 15 ssh "${JUMP_OPTS[@]}" $SSH_USER@$ip \
      "pkill -f bench_dht 2>/dev/null; true" 2>/dev/null || true &
  done
  timeout 15 ssh "${SSH_OPTS[@]}" $SSH_USER@$SIGNAL_IP \
    "pkill -f bench_dht 2>/dev/null; pkill -f signal_server 2>/dev/null; true" 2>/dev/null || true &
  timeout 15 ssh "${SSH_OPTS[@]}" $SSH_USER@$BOOTSTRAP_IP \
    "pkill -f bench_dht 2>/dev/null; true" 2>/dev/null || true &
  wait
}

run_dht_bench() {
  local tr=$1
  local n_nodes=$2
  local run_idx=$3
  local tag="${tr}_n${n_nodes}_run${run_idx}"

  echo ""
  echo "=== transport=$tr nodes=$n_nodes run=$run_idx ==="

  cleanup_all
  sleep 2

  if [[ $tr == "webrtc" ]]; then
    ssh_start "${SSH_OPTS[@]}" $SSH_USER@$SIGNAL_IP \
      "nohup ~/signal_server -addr=:9000 > /tmp/signal_server.log 2>&1 &" || \
      echo "WARNING: could not start signal_server" >&2
    sleep 1
  fi

  ssh_start "${SSH_OPTS[@]}" $SSH_USER@$BOOTSTRAP_IP \
    "nohup ~/bench_dht -role=bootstrap -addr=0.0.0.0 -dht-port=7100 \
      > /tmp/bootstrap.log 2>&1 &" || \
    echo "WARNING: could not start bootstrap" >&2
  sleep 2

  local coord_timeout=$(( 60 + n_nodes * 5 ))
  local out_remote="/tmp/dht_${tag}.json"
  local coord_dht_port=7500
  local coord_sig_port=8500

  ssh_start "${SSH_OPTS[@]}" $SSH_USER@$SIGNAL_IP \
    "nohup ~/bench_dht \
      -role=coordinator \
      -transport=$tr \
      -addr=$SIGNAL_INTERNAL \
      -dht-port=$coord_dht_port \
      -sig-port=$coord_sig_port \
      -bootstrap-addr=$BOOTSTRAP_ADDR \
      -sig-server=http://${SIGNAL_INTERNAL}:9000 \
      -coord-addr=$SIGNAL_INTERNAL \
      -coord-port=$COORD_PORT \
      -node-count=$n_nodes \
      -iters=$ITERS \
      -payload=$PAYLOAD \
      -timeout=${coord_timeout}s \
      -out=$out_remote \
      > /tmp/coord_${tag}.log 2>&1 &" || \
    { echo "ERROR: could not start coordinator" >&2; return 1; }

  echo "waiting for coordinator..."
  local coord_ready=0
  for i in $(seq 1 30); do
    if ssh_once "${SSH_OPTS[@]}" $SSH_USER@$SIGNAL_IP \
        "ss -tlnp | grep -q :${COORD_PORT}"; then
      coord_ready=1
      echo "coordinator ready after ${i}s"
      break
    fi
    sleep 1
  done
  if (( coord_ready == 0 )); then
    echo "ERROR: coordinator not ready" >&2
    ssh_once "${SSH_OPTS[@]}" $SSH_USER@$SIGNAL_IP "cat /tmp/coord_${tag}.log" || true
    return 1
  fi

  for i in $(seq 1 $n_nodes); do
    local vm_idx=$(( (i - 1) % N_VMS + 1 ))
    local host=${NODE_IPS[$vm_idx]}
    local dht_port=$(( 7200 + i ))
    local sig_port=$(( 8200 + i ))

    ssh_start "${JUMP_OPTS[@]}" $SSH_USER@$host \
      "nohup ~/bench_dht \
        -role=node \
        -transport=$tr \
        -addr=$host \
        -dht-port=$dht_port \
        -sig-port=$sig_port \
        -bootstrap-addr=$BOOTSTRAP_ADDR \
        -sig-server=http://${SIGNAL_INTERNAL}:9000 \
        -coord-addr=$SIGNAL_INTERNAL \
        -coord-port=$COORD_PORT \
        > /tmp/dht_node_${i}.log 2>&1 &" &
  done
  wait
  echo "all $n_nodes nodes started (across $N_VMS VMs, ~$(( (n_nodes + N_VMS - 1) / N_VMS )) per VM)"

  local waited=0
  local poll_limit=$(( coord_timeout + 30 ))
  while (( waited < poll_limit )); do
    sleep 5
    (( waited += 5 ))
    if ssh_once "${SSH_OPTS[@]}" $SSH_USER@$SIGNAL_IP \
        "grep -qE 'wrote .* results' /tmp/coord_${tag}.log"; then
      echo "coordinator finished after ${waited}s"
      break
    fi
    echo "  waiting... ${waited}/${poll_limit}s"
  done

  scp_retry "${SSH_OPTS[@]}" $SSH_USER@$SIGNAL_IP:$out_remote \
    results/dht/${tag}.json || echo "warning: could not fetch result file"
  ssh_once "${SSH_OPTS[@]}" $SSH_USER@$SIGNAL_IP "tail -5 /tmp/coord_${tag}.log" || true
  echo "done: $tag"

  cleanup_all
  sleep 3
}

for run in $(seq 1 $RUNS); do
  for n in $NODE_COUNTS; do
    for tr in udp webrtc; do
      run_dht_bench $tr $n $run
    done
  done
done

echo ""
echo "DHT benchmark done. Results in results/dht/"
ls results/dht/
