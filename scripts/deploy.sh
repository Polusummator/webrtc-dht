#!/usr/bin/env zsh
set -euo pipefail

cd "$(dirname "$0")/.."

SSH_USER="${SSH_USER:-ubuntu}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/ssh-key-1770215555284}"
INFRA_DIR="infra"

echo "applying infrastructure..."
(cd $INFRA_DIR && tofu apply -auto-approve)

echo "reading terraform outputs..."
SIGNAL_IP=$(cd $INFRA_DIR && tofu output -raw signal_server_public_ip)
BOOTSTRAP_IP=$(cd $INFRA_DIR && tofu output -raw bootstrap_public_ip)
NODE_IPS_JSON=$(cd $INFRA_DIR && tofu output -json node_internal_ips)

SSH_BASE="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o GlobalKnownHostsFile=/dev/null -o BatchMode=yes -o ConnectTimeout=30 -o LogLevel=ERROR"
SSH_OPTS=($=SSH_BASE -i "$SSH_KEY")
JUMP_OPTS=($=SSH_BASE -i "$SSH_KEY" -o "ProxyCommand=ssh $SSH_BASE -i $SSH_KEY -W %h:%p $SSH_USER@$SIGNAL_IP")

echo "signal server: $SIGNAL_IP"
echo "bootstrap:     $BOOTSTRAP_IP"
echo "nodes (internal): $NODE_IPS_JSON"

deploy_direct() {
  local host=$1 binary=$2
  echo "deploying $binary -> $host"
  scp "${SSH_OPTS[@]}" dist/$binary $SSH_USER@$host:~/$binary
  ssh "${SSH_OPTS[@]}" $SSH_USER@$host "chmod +x ~/$binary"
}

deploy_via_jump() {
  local host=$1 binary=$2
  echo "deploying $binary -> $host (via jump)"
  scp "${JUMP_OPTS[@]}" dist/$binary $SSH_USER@$host:~/$binary
  ssh "${JUMP_OPTS[@]}" $SSH_USER@$host "chmod +x ~/$binary"
}

wait_for_ssh() {
  local host=$1 jump=${2:-}
  local opts
  if [[ -n "$jump" ]]; then
    opts=("${JUMP_OPTS[@]}")
  else
    opts=("${SSH_OPTS[@]}")
  fi
  echo "waiting for ssh on $host..."
  for i in $(seq 1 30); do
    if ssh "${opts[@]}" -o BatchMode=yes $SSH_USER@$host true 2>/dev/null; then
      return 0
    fi
    sleep 5
  done
  echo "timeout waiting for ssh on $host" >&2
  return 1
}

for host in $SIGNAL_IP $BOOTSTRAP_IP; do
  wait_for_ssh $host
  deploy_direct $host bench_transport
  deploy_direct $host bench_signaling2
  deploy_direct $host bench_dht
  deploy_direct $host signal_server
done

NODE_IPS=($(echo $NODE_IPS_JSON | python3 -c "import json,sys; print('\n'.join(json.load(sys.stdin)))"))
for host in $NODE_IPS; do
  wait_for_ssh $host jump
  deploy_via_jump $host bench_transport
  deploy_via_jump $host bench_signaling2
  deploy_via_jump $host bench_dht
done

echo "deploy done"
