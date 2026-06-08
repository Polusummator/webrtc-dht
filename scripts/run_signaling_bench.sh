#!/usr/bin/env zsh
set -euo pipefail
cd "$(dirname "$0")/.."

SSH_USER="${SSH_USER:-ubuntu}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/ssh-key-1770215555284}"
INFRA_DIR="infra"
RUNS="${RUNS:-1}"

mkdir -p results/signaling

SIGNAL_IP=$(cd $INFRA_DIR && tofu output -raw signal_server_public_ip)
SIGNAL_INTERNAL=$(cd $INFRA_DIR && tofu output -raw signal_server_internal_ip)
BOOTSTRAP_IP=$(cd $INFRA_DIR && tofu output -raw bootstrap_public_ip)
BOOTSTRAP_INTERNAL=$(cd $INFRA_DIR && tofu output -raw bootstrap_internal_ip)
NODE_INTERNAL_JSON=$(cd $INFRA_DIR && tofu output -json node_internal_ips)
NODE_IPS=($(echo $NODE_INTERNAL_JSON | python3 -c "import json,sys; print('\n'.join(json.load(sys.stdin)))"))

SSH_BASE="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o GlobalKnownHostsFile=/dev/null -o BatchMode=yes -o ConnectTimeout=15 -o LogLevel=ERROR -o ServerAliveInterval=10 -o ServerAliveCountMax=3"
SSH_OPTS=($=SSH_BASE -i "$SSH_KEY")
JUMP_OPTS=($=SSH_BASE -i "$SSH_KEY" -o "ProxyCommand=ssh $SSH_BASE -i $SSH_KEY -W %h:%p $SSH_USER@$SIGNAL_IP")

N_VMS=${#NODE_IPS[@]}
AGENT_PORT=8800
COORD_PORT=9100
BOOTSTRAP_ADDR="${BOOTSTRAP_INTERNAL}:7100"
PAIR_COUNTS=(2 5 10 25 50 100 200 500)

ssh_start() {
  local retries=4 delay=3 i=0
  until ssh "$@"; do
    i=$(( i + 1 ))
    (( i >= retries )) && { echo "ssh_start failed" >&2; return 1; }
    sleep $delay
  done
}
ssh_once() { ssh "$@" 2>/dev/null; }
scp_retry() {
  local retries=5 delay=3 i=0
  until scp "$@"; do
    i=$(( i + 1 ))
    (( i >= retries )) && { echo "scp failed" >&2; return 1; }
    sleep $delay
  done
}

kill_agents() {
  for ip in $NODE_IPS; do
    timeout 10 ssh "${JUMP_OPTS[@]}" $SSH_USER@$ip \
      "pkill -f bench_signaling 2>/dev/null; true" 2>/dev/null || true &
  done
  timeout 10 ssh "${SSH_OPTS[@]}" $SSH_USER@$SIGNAL_IP \
    "pkill -f bench_signaling 2>/dev/null; pkill -f signal_server 2>/dev/null; true" 2>/dev/null || true &
  timeout 10 ssh "${SSH_OPTS[@]}" $SSH_USER@$BOOTSTRAP_IP \
    "pkill -f bench_signaling 2>/dev/null; true" 2>/dev/null || true &
  wait
}

start_agents() {
  local signaling=$1
  local extra=""
  [[ $signaling == "dht" ]] && extra="-bootstrap-addr=$BOOTSTRAP_ADDR"

  for ip in $NODE_IPS; do
    ssh_start "${JUMP_OPTS[@]}" $SSH_USER@$ip \
      "nohup ~/bench_signaling \
        -role=agent \
        -signaling=$signaling \
        -addr=$ip \
        -dht-port=7200 \
        -agent-port=$AGENT_PORT \
        -sig-server=http://${SIGNAL_INTERNAL}:9000 \
        $extra \
        > /tmp/agent_${ip}.log 2>&1 &" || true &
  done
  wait

  # Wait for agents to be up
  echo "waiting for agents..."
  for ip in $NODE_IPS; do
    for i in $(seq 1 20); do
      if ssh_once "${JUMP_OPTS[@]}" $SSH_USER@$ip \
          "ss -tlnp | grep -q :${AGENT_PORT}"; then
        break
      fi
      sleep 1
    done
  done
  sleep 1
}

run_bench() {
  local signaling=$1 pairs=$2 run=$3
  local tag="${signaling}_n${pairs}_run${run}"
  echo ""
  echo "=== $tag ==="

  kill_agents
  sleep 2

  # Start central signal server if needed
  if [[ $signaling == "central" ]]; then
    ssh_start "${SSH_OPTS[@]}" $SSH_USER@$SIGNAL_IP \
      "nohup ~/signal_server -addr=:9000 > /tmp/signal_server.log 2>&1 &" || true
    sleep 1
  fi

  # Start DHT bootstrap
  ssh_start "${SSH_OPTS[@]}" $SSH_USER@$BOOTSTRAP_IP \
    "nohup ~/bench_signaling \
      -role=agent \
      -signaling=$signaling \
      -addr=$BOOTSTRAP_INTERNAL \
      -dht-port=7100 \
      -agent-port=$AGENT_PORT \
      -sig-server=http://${SIGNAL_INTERNAL}:9000 \
      > /tmp/bootstrap_agent.log 2>&1 &" || true
  sleep 3

  start_agents $signaling

  # Build agent list (internal IPs + port)
  local agent_list=""
  for ip in $NODE_IPS; do
    [[ -n $agent_list ]] && agent_list="${agent_list},"
    agent_list="${agent_list}${ip}:${AGENT_PORT}"
  done

  local out_remote="/tmp/sig_${tag}.json"
  local stagger_ms=0
  [[ $signaling == "dht" && $pairs -gt 10 ]] && stagger_ms=20
  local dispatch_s=$(( (pairs * stagger_ms + 999) / 1000 ))
  local coord_timeout=$(( dispatch_s + 90 ))

  ssh_start "${SSH_OPTS[@]}" $SSH_USER@$SIGNAL_IP \
    "~/bench_signaling \
      -role=coordinator \
      -signaling=$signaling \
      -pairs=$pairs \
      -agents='$agent_list' \
      -coord-port=$COORD_PORT \
      -timeout=${coord_timeout}s \
      -out=$out_remote" || { echo "coordinator failed" >&2; return 1; }

  scp_retry "${SSH_OPTS[@]}" $SSH_USER@$SIGNAL_IP:$out_remote results/signaling/${tag}.json || \
    echo "warning: could not fetch result"
  echo "done: $tag"

  kill_agents
  sleep 2
}

for run in $(seq 1 $RUNS); do
  for pairs in $PAIR_COUNTS; do
    for signaling in central dht; do
      run_bench $signaling $pairs $run
    done
  done
done

echo ""
echo "done. results in results/signaling/"
ls results/signaling/
