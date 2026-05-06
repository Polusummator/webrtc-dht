#!/usr/bin/env zsh
set -euo pipefail
cd "$(dirname "$0")/.."

SSH_USER="${SSH_USER:-ubuntu}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/ssh-key-1770215555284}"
INFRA_DIR="infra"
RUNS="${RUNS:-3}"

mkdir -p results/signaling

SIGNAL_IP=$(cd $INFRA_DIR && tofu output -raw signal_server_public_ip)
SIGNAL_INTERNAL=$(cd $INFRA_DIR && tofu output -raw signal_server_internal_ip)
BOOTSTRAP_IP=$(cd $INFRA_DIR && tofu output -raw bootstrap_public_ip)
BOOTSTRAP_INTERNAL=$(cd $INFRA_DIR && tofu output -raw bootstrap_internal_ip)
NODE_INTERNAL_JSON=$(cd $INFRA_DIR && tofu output -json node_internal_ips)
NODE_IPS=($(echo $NODE_INTERNAL_JSON | python3 -c "import json,sys; print('\n'.join(json.load(sys.stdin)))"))

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=30 -o ServerAliveInterval=10 -o ServerAliveCountMax=3 -i "$SSH_KEY")
JUMP_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=30 -o ServerAliveInterval=10 -o ServerAliveCountMax=3 -i "$SSH_KEY" \
  -o "ProxyJump=$SSH_USER@$SIGNAL_IP" \
  -o ControlMaster=auto \
  -o "ControlPath=/tmp/ssh-ctl-jump-%h-%p-%r" \
  -o ControlPersist=120)

ssh_retry() {
  local retries=5
  local delay=3
  local i=0
  until ssh "$@"; do
    i=$(( i + 1 ))
    if (( i >= retries )); then
      echo "ssh failed after $retries attempts: $*" >&2
      return 1
    fi
    echo "ssh connection reset, retry $i/$retries in ${delay}s..." >&2
    sleep $delay
  done
}

scp_retry() {
  local retries=5
  local delay=3
  local i=0
  until scp "$@"; do
    i=$(( i + 1 ))
    if (( i >= retries )); then
      echo "scp failed after $retries attempts" >&2
      return 1
    fi
    echo "scp failed, retry $i/$retries in ${delay}s..." >&2
    sleep $delay
  done
}

N_VMS=${#NODE_IPS[@]}
COORD_PORT=9100
COORD_HOST=$SIGNAL_INTERNAL
BOOTSTRAP_ADDR="${BOOTSTRAP_INTERNAL}:7100"

PAIR_COUNTS=(2 5 10)
if (( N_VMS >= 10 )); then
  PAIR_COUNTS=(2 5 10 20)
fi

run_signaling_bench() {
  local signaling=$1
  local pairs=$2
  local run_idx=$3
  local tag="${signaling}_n${pairs}_run${run_idx}"

  echo ""
  echo "=== signaling=$signaling pairs=$pairs run=$run_idx ==="

  for ip in $NODE_IPS; do
    ssh_retry "${JUMP_OPTS[@]}" $SSH_USER@$ip "pkill -f bench_signaling || true" 2>/dev/null || true
  done
  ssh_retry "${SSH_OPTS[@]}" $SSH_USER@$SIGNAL_IP \
    "pkill -f bench_signaling || true; pkill -f signal_server || true" 2>/dev/null || true
  ssh_retry "${SSH_OPTS[@]}" $SSH_USER@$BOOTSTRAP_IP "pkill -f bench_signaling || true" 2>/dev/null || true
  sleep 2

  if [[ $signaling == "central" ]]; then
    ssh_retry "${SSH_OPTS[@]}" $SSH_USER@$SIGNAL_IP \
      "nohup ~/signal_server -addr=:9000 > /tmp/signal_server.log 2>&1 &"
    sleep 1
  fi

  ssh_retry "${SSH_OPTS[@]}" $SSH_USER@$BOOTSTRAP_IP \
    "nohup ~/bench_signaling -role=bootstrap -addr=0.0.0.0 -dht-port=7100 > /tmp/bootstrap.log 2>&1 &"
  sleep 2

  local out_remote="/tmp/signaling_${tag}.json"
  ssh_retry "${SSH_OPTS[@]}" $SSH_USER@$SIGNAL_IP \
    "nohup ~/bench_signaling \
      -role=coordinator \
      -signaling=$signaling \
      -pairs=$pairs \
      -coord-port=$COORD_PORT \
      -out=$out_remote \
      > /tmp/coord_${tag}.log 2>&1 &"
  sleep 1

  local half=$(( N_VMS / 2 ))

  for i in $(seq 1 $pairs); do
    local callee_vm=$(( (i - 1) % N_VMS + 1 ))
    local host=${NODE_IPS[$callee_vm]}
    local dht_port=$(( 7200 + i ))
    local extra_args=""
    [[ $signaling == "dht" ]] && extra_args="-bootstrap-addr=$BOOTSTRAP_ADDR"

    sleep 0.3
    ssh_retry "${JUMP_OPTS[@]}" $SSH_USER@$host \
      "nohup ~/bench_signaling \
        -role=callee \
        -pair-id=$i \
        -signaling=$signaling \
        -addr=0.0.0.0 \
        -dht-port=$dht_port \
        -sig-server=http://${SIGNAL_INTERNAL}:9000 \
        -coord-addr=$COORD_HOST \
        -coord-port=$COORD_PORT \
        $extra_args \
        > /tmp/callee_${i}.log 2>&1 &" &
  done
  wait
  echo "callees started, waiting 6s for registration..."
  sleep 6

  for i in $(seq 1 $pairs); do
    local caller_vm=$(( (i - 1 + half) % N_VMS + 1 ))
    local host=${NODE_IPS[$caller_vm]}
    local dht_port=$(( 7300 + i ))
    local extra_args=""
    [[ $signaling == "dht" ]] && extra_args="-bootstrap-addr=$BOOTSTRAP_ADDR"

    sleep 0.3
    ssh_retry "${JUMP_OPTS[@]}" $SSH_USER@$host \
      "nohup ~/bench_signaling \
        -role=caller \
        -pair-id=$i \
        -signaling=$signaling \
        -addr=0.0.0.0 \
        -dht-port=$dht_port \
        -sig-server=http://${SIGNAL_INTERNAL}:9000 \
        -coord-addr=$COORD_HOST \
        -coord-port=$COORD_PORT \
        $extra_args \
        > /tmp/caller_${i}.log 2>&1 &" &
  done
  wait
  echo "all callers started simultaneously"

  local waited=0
  while (( waited < 300 )); do
    sleep 5
    (( waited += 5 ))
    if ssh_retry "${SSH_OPTS[@]}" $SSH_USER@$SIGNAL_IP \
        "grep -qE 'wrote results|timeout:' /tmp/coord_${tag}.log 2>/dev/null"; then
      echo "coordinator finished after ${waited}s"
      break
    fi
  done

  scp_retry "${SSH_OPTS[@]}" $SSH_USER@$SIGNAL_IP:$out_remote results/signaling/${tag}.json 2>/dev/null || \
    echo "warning: could not fetch result file"
  ssh_retry "${SSH_OPTS[@]}" $SSH_USER@$SIGNAL_IP "tail -5 /tmp/coord_${tag}.log 2>/dev/null" || true
  echo "done: $tag"

  for ip in $NODE_IPS; do
    ssh_retry "${JUMP_OPTS[@]}" $SSH_USER@$ip "pkill -f bench_signaling || true" 2>/dev/null || true
  done
  ssh_retry "${SSH_OPTS[@]}" $SSH_USER@$SIGNAL_IP \
    "pkill -f bench_signaling || true; pkill -f signal_server || true" 2>/dev/null || true
  ssh_retry "${SSH_OPTS[@]}" $SSH_USER@$BOOTSTRAP_IP "pkill -f bench_signaling || true" 2>/dev/null || true
  sleep 3
}

for run in $(seq 1 $RUNS); do
  for pairs in $PAIR_COUNTS; do
    for signaling in central dht; do
      run_signaling_bench $signaling $pairs $run
    done
  done
done

echo ""
echo "signaling benchmark done. results in results/signaling/"
ls results/signaling/

