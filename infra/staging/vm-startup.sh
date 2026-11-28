#!/usr/bin/env bash
# Compute Engine startup script for the staging VM (LM-048).
#
# provision.sh installs this as the instance's `startup-script` metadata, so
# it runs as root on every boot. Every step is idempotent:
#   1. install Docker Engine and the Compose plugin from Docker's apt
#      repository, and python3 and the Google Cloud CLI only if they are
#      missing;
#   2. format the data disk only if it has no filesystem, then mount it at
#      /var/lib/ledgermesh through an fstab UUID entry;
#   3. make Docker wait for that mount, so containers can never start on an
#      empty directory of the boot disk;
#   4. create the data directories and configure json-file log rotation;
#   5. stop containers from reaching the GCE metadata server (defence in
#      depth: only Caddy is on a network with egress, see compose.yaml).
#
# Output goes to the serial console and the journal:
#   sudo journalctl -u google-startup-scripts.service
set -euo pipefail

DATA_DEVICE="/dev/disk/by-id/google-ledgermesh-data"
DATA_DIR="/var/lib/ledgermesh"
# The apache/kafka image runs as this UID (checked with
# `docker run --rm --entrypoint id apache/kafka:4.4.0`); stack.sh uses the same value.
KAFKA_UID=1000

log() {
  echo "ledgermesh-startup: $*"
}

export DEBIAN_FRONTEND=noninteractive

# ---- 1. packages ------------------------------------------------------------------

if ! command -v docker >/dev/null || ! docker compose version >/dev/null 2>&1; then
  log "installing Docker Engine and the Compose plugin"
  apt-get update
  apt-get install -y ca-certificates curl gnupg
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  codename="$(. /etc/os-release && echo "$VERSION_CODENAME")"
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian $codename stable" \
    >/etc/apt/sources.list.d/docker.list
  apt-get update
  apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
fi

# deploy.sh (LM-049) keeps its release records as JSON with python3.
if ! command -v python3 >/dev/null; then
  log "installing python3"
  apt-get update
  apt-get install -y python3
fi

if ! command -v gcloud >/dev/null; then
  log "installing the Google Cloud CLI"
  apt-get update
  apt-get install -y apt-transport-https ca-certificates curl gnupg
  curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | gpg --dearmor --yes -o /usr/share/keyrings/cloud.google.gpg
  echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
    >/etc/apt/sources.list.d/google-cloud-sdk.list
  apt-get update
  apt-get install -y google-cloud-cli
fi

# ---- 2. data disk -----------------------------------------------------------------

if [[ ! -e "$DATA_DEVICE" ]]; then
  log "data disk $DATA_DEVICE is not attached; refusing to continue"
  exit 1
fi

# Low-level probe (-p, no cache): exit 0 = a filesystem or partition table
# was found, 2 = nothing recognisable (a new, empty disk). Every other status
# (4 = usage or I/O error, 8 = ambivalent probe) stops here: this disk holds
# all Kafka and PostgreSQL data, so it is formatted only on a definite 2.
probe_status=0
probe_err="$(blkid -p "$DATA_DEVICE" 2>&1 >/dev/null)" || probe_status=$?
case "$probe_status" in
  0) ;;
  2)
    log "no filesystem on $DATA_DEVICE: formatting a new data disk"
    mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard "$DATA_DEVICE"
    ;;
  *)
    log "blkid -p $DATA_DEVICE exited $probe_status (${probe_err:-no message}); refusing to format or mount it"
    exit 1
    ;;
esac

uuid="$(blkid -s UUID -o value "$DATA_DEVICE")"
[[ -n "$uuid" ]] || { log "could not read the data disk UUID"; exit 1; }
mkdir -p "$DATA_DIR"
if ! grep -q "^UUID=$uuid " /etc/fstab; then
  log "adding $DATA_DIR to /etc/fstab"
  echo "UUID=$uuid $DATA_DIR ext4 discard,defaults,nofail 0 2" >>/etc/fstab
fi
if ! mountpoint -q "$DATA_DIR"; then
  mount "$DATA_DIR"
fi
mountpoint -q "$DATA_DIR" || { log "$DATA_DIR is not mounted"; exit 1; }

# ---- 3. Docker waits for the data disk ----------------------------------------------

dropin_dir="/etc/systemd/system/docker.service.d"
dropin="$dropin_dir/ledgermesh-data.conf"
# Forwarded (container) traffic to the metadata server is rejected, so no
# container can obtain the VM service account's token, which can read every
# database password. The host's own traffic (render-env.sh) is not forwarded
# and is unaffected. ExecStartPost re-adds the rule whenever Docker starts.
METADATA_RULE=(DOCKER-USER -d 169.254.169.254/32 -j REJECT)
dropin_content="[Unit]
RequiresMountsFor=$DATA_DIR

[Service]
ExecStartPost=/bin/sh -c 'iptables -C ${METADATA_RULE[*]} 2>/dev/null || iptables -I ${METADATA_RULE[*]}'"
mkdir -p "$dropin_dir"
if [[ ! -f "$dropin" || "$(cat "$dropin")" != "$dropin_content" ]]; then
  echo "$dropin_content" >"$dropin"
  systemctl daemon-reload
fi

# ---- 4. directories and Docker logging ----------------------------------------------

for dir in kafka postgres/coordinator postgres/wallet postgres/catalog postgres/audit-ledger \
  caddy/data caddy/config releases deploy-logs; do
  mkdir -p "$DATA_DIR/$dir"
done
chown "$KAFKA_UID:$KAFKA_UID" "$DATA_DIR/kafka"
chmod 0750 "$DATA_DIR"
install -d -m 0700 /etc/ledgermesh

daemon_json='{
  "log-driver": "json-file",
  "log-opts": { "max-size": "10m", "max-file": "5" },
  "live-restore": true
}'
if [[ ! -f /etc/docker/daemon.json || "$(cat /etc/docker/daemon.json)" != "$daemon_json" ]]; then
  log "configuring Docker log rotation"
  mkdir -p /etc/docker
  echo "$daemon_json" >/etc/docker/daemon.json
  systemctl restart docker
fi
systemctl enable --now docker

# ---- 5. no container reaches the metadata server ------------------------------------

if ! iptables -C "${METADATA_RULE[@]}" 2>/dev/null; then
  log "blocking container traffic to the metadata server"
  iptables -I "${METADATA_RULE[@]}"
fi
iptables -C "${METADATA_RULE[@]}" || { log "the metadata block rule is missing"; exit 1; }

log "ready: docker $(docker version --format '{{.Server.Version}}'), $(docker compose version --short), data on $DATA_DIR"
