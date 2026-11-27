#!/usr/bin/env bash
# Provision the hosted staging environment on Google Compute Engine (LM-048).
#
#   provision.sh up       --billing-account <id> --acme-email <email>
#                         [--admin-member user:<email>]... [options]
#   provision.sh stop     [options]   stop the VM (only disks, snapshots and the IP are billed)
#   provision.sh start    [options]   start it again
#   provision.sh status   [options]   VM state, IP and URL
#   provision.sh teardown --confirm <project> [--billing-account <id>]
#                         [--admin-member ...]... [options]
#
# Every command accepts --dry-run, which prints each gcloud command,
# shell-quoted, and runs nothing. A dry run assumes resources are absent for
# `up` (so every create is shown) and present for `teardown`.
#
# `up` is idempotent: each resource is looked up first and created only when
# it is missing, so it is safe to re-run (for example to add an admin or to
# update the VM's startup script and metadata).
#
# Options (defaults in brackets):
#   --project <id>          [deven-cloud-lab]
#   --region <region>       [us-west1]
#   --zone <zone>           [us-west1-b]
#   --vm <name>             [ledgermesh-staging]
#   --machine-type <type>   [e2-standard-4]
#   --data-disk-size <GB>   [30]
#   --billing-account <id>  billing account for the budget (required for up)
#   --budget-amount <USD>   [50] monthly budget; alerts at 50%, 90% and 100%
#   --acme-email <email>    contact for Let's Encrypt (required for up)
#   --admin-member <member> user:<email> or group:<email>; repeatable. Gets
#                           IAP SSH (port 22 only) and OS Login admin.
#   --confirm <project>     required by teardown; must equal --project
#
# Then run bootstrap-wif.sh once for GitHub Actions (LM-049).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PROJECT="${LEDGERMESH_GCP_PROJECT:-deven-cloud-lab}"
REGION="us-west1"
ZONE="us-west1-b"
VM="ledgermesh-staging"
MACHINE_TYPE="e2-standard-4"
DATA_DISK_SIZE=30
BOOT_DISK_SIZE=20
BILLING_ACCOUNT=""
BUDGET_AMOUNT=50
ACME_EMAIL=""
ADMIN_MEMBERS=()
CONFIRM=""
DRY_RUN=0

# Resource names. All are derived, so teardown finds exactly what up created.
NETWORK="ledgermesh-staging"
SUBNET="ledgermesh-staging-us-west1"
SUBNET_RANGE="10.20.0.0/24"
NETWORK_TAG="ledgermesh-staging"
FW_WEB="ledgermesh-staging-allow-web"
FW_IAP="ledgermesh-staging-allow-iap-ssh"
IAP_RANGE="35.235.240.0/20"
ADDRESS="ledgermesh-staging-ip"
DATA_DISK="ledgermesh-data"
SNAPSHOT_SCHEDULE="ledgermesh-data-daily"
VM_SA_NAME="ledgermesh-staging-vm"
BUDGET_NAME="ledgermesh-staging"
SECRET_PREFIX="ledgermesh-staging"
SECRET_SUFFIXES=(coordinator-db-password wallet-db-password catalog-db-password audit-ledger-db-password)
APIS=(compute.googleapis.com iap.googleapis.com secretmanager.googleapis.com
  billingbudgets.googleapis.com cloudresourcemanager.googleapis.com oslogin.googleapis.com
  iam.googleapis.com)
# Documentation range (RFC 5737), shown only by dry runs instead of the real IP.
DRY_RUN_IP="203.0.113.10"
SSH_CONDITION="expression=destination.port == 22,title=ledgermesh-staging-ssh-only,description=IAP TCP forwarding to SSH only"

die() {
  echo "provision: $*" >&2
  exit 1
}

log() {
  echo "provision: $*" >&2
}

usage() {
  sed -n '2,37p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

[[ $# -ge 1 ]] || { usage >&2; exit 2; }
COMMAND="$1"
shift
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --project) [[ $# -ge 2 ]] || die "$1 needs a value"; PROJECT="$2"; shift 2 ;;
    --region) [[ $# -ge 2 ]] || die "$1 needs a value"; REGION="$2"; shift 2 ;;
    --zone) [[ $# -ge 2 ]] || die "$1 needs a value"; ZONE="$2"; shift 2 ;;
    --vm) [[ $# -ge 2 ]] || die "$1 needs a value"; VM="$2"; shift 2 ;;
    --machine-type) [[ $# -ge 2 ]] || die "$1 needs a value"; MACHINE_TYPE="$2"; shift 2 ;;
    --data-disk-size) [[ $# -ge 2 ]] || die "$1 needs a value"; DATA_DISK_SIZE="$2"; shift 2 ;;
    --billing-account) [[ $# -ge 2 ]] || die "$1 needs a value"; BILLING_ACCOUNT="$2"; shift 2 ;;
    --budget-amount) [[ $# -ge 2 ]] || die "$1 needs a value"; BUDGET_AMOUNT="$2"; shift 2 ;;
    --acme-email) [[ $# -ge 2 ]] || die "$1 needs a value"; ACME_EMAIL="$2"; shift 2 ;;
    --admin-member) [[ $# -ge 2 ]] || die "$1 needs a value"; ADMIN_MEMBERS+=("$2"); shift 2 ;;
    --confirm) [[ $# -ge 2 ]] || die "$1 needs a value"; CONFIRM="$2"; shift 2 ;;
    -h | --help) usage; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done

[[ "$PROJECT" =~ ^[a-z][a-z0-9-]{4,28}[a-z0-9]$ ]] || die "invalid --project: $PROJECT"
[[ "$ZONE" == "$REGION"-* ]] || die "--zone $ZONE is not in --region $REGION"
[[ "$DATA_DISK_SIZE" =~ ^[0-9]+$ ]] || die "--data-disk-size must be a whole number of GB"
[[ "$BUDGET_AMOUNT" =~ ^[0-9]+(\.[0-9]{1,2})?$ ]] || die "--budget-amount must be a number of USD"
for member in ${ADMIN_MEMBERS[@]+"${ADMIN_MEMBERS[@]}"}; do
  [[ "$member" =~ ^(user|group):[^@[:space:]]+@[^@[:space:]]+$ ]] \
    || die "--admin-member must look like user:<email> or group:<email>, got: $member"
done

VM_SA="$VM_SA_NAME@$PROJECT.iam.gserviceaccount.com"

# ---- command runners -----------------------------------------------------------------

quote() {
  local out="" arg
  for arg in "$@"; do
    out+="$(printf '%q' "$arg") "
  done
  echo "${out% }"
}

# Run (or, with --dry-run, print) a command that changes something.
run() {
  echo "+ $(quote "$@")"
  if [[ "$DRY_RUN" == 0 ]]; then
    "$@"
  fi
}

# IAM can lag behind a just-created service account; retry a few times.
run_iam() {
  local attempt
  echo "+ $(quote "$@")"
  [[ "$DRY_RUN" == 1 ]] && return 0
  for attempt in 1 2 3 4 5; do
    if "$@" >/dev/null; then
      return 0
    fi
    [[ "$attempt" == 5 ]] && die "IAM binding failed after 5 attempts: $(quote "$@")"
    sleep $((attempt * 5))
  done
}

# Is a resource present? A dry run prints the lookup and assumes absent for
# up and present for teardown. A lookup error reads as absent; the following
# create then fails loudly, so nothing is skipped silently.
DRY_RUN_ASSUME_PRESENT=0
exists() {
  if [[ "$DRY_RUN" == 1 ]]; then
    echo "# check: $(quote "$@")"
    [[ "$DRY_RUN_ASSUME_PRESENT" == 1 ]]
    return
  fi
  "$@" >/dev/null 2>&1
}

# A read-only query whose output is needed (never changes anything).
query() {
  local placeholder="$1"
  shift
  if [[ "$DRY_RUN" == 1 ]]; then
    echo "# query: $(quote "$@")" >&2
    echo "$placeholder"
    return 0
  fi
  "$@"
}

G=(gcloud --project "$PROJECT" --quiet)

# ---- up --------------------------------------------------------------------------------

static_ip() {
  query "$DRY_RUN_IP" "${G[@]}" compute addresses describe "$ADDRESS" --region "$REGION" --format "value(address)"
}

site_address() {
  echo "$1" | tr '.' '-' | sed 's/$/.sslip.io/'
}

up() {
  [[ -n "$BILLING_ACCOUNT" ]] || die "up needs --billing-account (the budget is part of provisioning)"
  [[ "$ACME_EMAIL" =~ ^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$ ]] \
    || die "up needs --acme-email <email> for the Let's Encrypt account"
  [[ -f "$SCRIPT_DIR/vm-startup.sh" ]] || die "vm-startup.sh not found next to provision.sh"
  if [[ ${#ADMIN_MEMBERS[@]} -eq 0 ]]; then
    log "warning: no --admin-member given; only project owners can reach the VM"
  fi

  log "1/10 enabling APIs"
  run "${G[@]}" services enable "${APIS[@]}"

  log "2/10 network (a dedicated VPC; the default network allows SSH from anywhere)"
  if ! exists "${G[@]}" compute networks describe "$NETWORK"; then
    run "${G[@]}" compute networks create "$NETWORK" --subnet-mode custom
  fi
  if ! exists "${G[@]}" compute networks subnets describe "$SUBNET" --region "$REGION"; then
    run "${G[@]}" compute networks subnets create "$SUBNET" --network "$NETWORK" \
      --region "$REGION" --range "$SUBNET_RANGE" --enable-private-ip-google-access
  fi

  log "3/10 firewall: TCP 80/443 public, TCP 22 from IAP only"
  if ! exists "${G[@]}" compute firewall-rules describe "$FW_WEB"; then
    run "${G[@]}" compute firewall-rules create "$FW_WEB" --network "$NETWORK" \
      --direction INGRESS --action ALLOW --rules tcp:80,tcp:443 \
      --source-ranges 0.0.0.0/0 --target-tags "$NETWORK_TAG"
  fi
  if ! exists "${G[@]}" compute firewall-rules describe "$FW_IAP"; then
    run "${G[@]}" compute firewall-rules create "$FW_IAP" --network "$NETWORK" \
      --direction INGRESS --action ALLOW --rules tcp:22 \
      --source-ranges "$IAP_RANGE" --target-tags "$NETWORK_TAG"
  fi

  log "4/10 static external IPv4"
  if ! exists "${G[@]}" compute addresses describe "$ADDRESS" --region "$REGION"; then
    run "${G[@]}" compute addresses create "$ADDRESS" --region "$REGION" --network-tier PREMIUM
  fi
  local ip site
  ip="$(static_ip)"
  [[ "$ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "could not read the static IP of $ADDRESS"
  site="$(site_address "$ip")"

  log "5/10 VM service account (no project roles except writing logs)"
  if ! exists "${G[@]}" iam service-accounts describe "$VM_SA"; then
    run "${G[@]}" iam service-accounts create "$VM_SA_NAME" \
      --display-name "LedgerMesh staging VM"
  fi
  run_iam "${G[@]}" projects add-iam-policy-binding "$PROJECT" \
    --member "serviceAccount:$VM_SA" --role roles/logging.logWriter --condition None

  log "6/10 database password secrets (generated here, never printed)"
  local suffix name
  for suffix in "${SECRET_SUFFIXES[@]}"; do
    name="$SECRET_PREFIX-$suffix"
    if ! exists "${G[@]}" secrets describe "$name"; then
      run "${G[@]}" secrets create "$name" --replication-policy automatic \
        --labels app=ledgermesh,env=staging
    fi
    if [[ -z "$(query "" "${G[@]}" secrets versions list "$name" --filter "state=ENABLED" --format "value(name)" --limit 1)" ]]; then
      # Hex, so the value is safe inside DATABASE_URL; no trailing newline.
      echo "+ openssl rand -hex 32 | tr -d '\\n' | $(quote "${G[@]}" secrets versions add "$name" --data-file -)"
      if [[ "$DRY_RUN" == 0 ]]; then
        openssl rand -hex 32 | tr -d '\n' | "${G[@]}" secrets versions add "$name" --data-file - >/dev/null
      fi
    fi
    run_iam "${G[@]}" secrets add-iam-policy-binding "$name" \
      --member "serviceAccount:$VM_SA" --role roles/secretmanager.secretAccessor
  done

  log "7/10 persistent data disk with a daily snapshot schedule (7 days)"
  if ! exists "${G[@]}" compute resource-policies describe "$SNAPSHOT_SCHEDULE" --region "$REGION"; then
    run "${G[@]}" compute resource-policies create snapshot-schedule "$SNAPSHOT_SCHEDULE" \
      --region "$REGION" --max-retention-days 7 --on-source-disk-delete keep-auto-snapshots \
      --daily-schedule --start-time 10:00
  fi
  if ! exists "${G[@]}" compute disks describe "$DATA_DISK" --zone "$ZONE"; then
    run "${G[@]}" compute disks create "$DATA_DISK" --zone "$ZONE" --type pd-balanced \
      --size "${DATA_DISK_SIZE}GB" --resource-policies "$SNAPSHOT_SCHEDULE" \
      --labels app=ledgermesh,env=staging
  elif ! query "" "${G[@]}" compute disks describe "$DATA_DISK" --zone "$ZONE" \
    --format "value(resourcePolicies)" | grep -q "/$SNAPSHOT_SCHEDULE"; then
    run "${G[@]}" compute disks add-resource-policies "$DATA_DISK" --zone "$ZONE" \
      --resource-policies "$SNAPSHOT_SCHEDULE"
  fi

  log "8/10 VM"
  local metadata="enable-oslogin=TRUE,block-project-ssh-keys=TRUE,ledgermesh-site-address=$site,ledgermesh-acme-email=$ACME_EMAIL"
  if ! exists "${G[@]}" compute instances describe "$VM" --zone "$ZONE"; then
    run "${G[@]}" compute instances create "$VM" --zone "$ZONE" \
      --machine-type "$MACHINE_TYPE" \
      --image-family debian-12 --image-project debian-cloud \
      --boot-disk-size "${BOOT_DISK_SIZE}GB" --boot-disk-type pd-balanced \
      --shielded-secure-boot --shielded-vtpm --shielded-integrity-monitoring \
      --subnet "$SUBNET" --address "$ip" --network-tier PREMIUM --tags "$NETWORK_TAG" \
      --service-account "$VM_SA" --scopes cloud-platform \
      --disk "name=$DATA_DISK,device-name=$DATA_DISK,mode=rw,auto-delete=no" \
      --metadata "$metadata" \
      --metadata-from-file "startup-script=$SCRIPT_DIR/vm-startup.sh" \
      --labels app=ledgermesh,env=staging
  else
    # Keep the startup script and settings current on re-runs; it runs at next boot.
    run "${G[@]}" compute instances add-metadata "$VM" --zone "$ZONE" \
      --metadata "$metadata" \
      --metadata-from-file "startup-script=$SCRIPT_DIR/vm-startup.sh"
  fi

  log "9/10 admin access for named members (IAP SSH to port 22, OS Login admin)"
  local member
  for member in ${ADMIN_MEMBERS[@]+"${ADMIN_MEMBERS[@]}"}; do
    run_iam "${G[@]}" projects add-iam-policy-binding "$PROJECT" \
      --member "$member" --role roles/iap.tunnelResourceAccessor --condition "$SSH_CONDITION"
    run_iam "${G[@]}" projects add-iam-policy-binding "$PROJECT" \
      --member "$member" --role roles/compute.viewer --condition None
    run_iam "${G[@]}" compute instances add-iam-policy-binding "$VM" --zone "$ZONE" \
      --member "$member" --role roles/compute.osAdminLogin
    # OS Login on a VM that runs as a service account needs this on that account.
    run_iam "${G[@]}" iam service-accounts add-iam-policy-binding "$VM_SA" \
      --member "$member" --role roles/iam.serviceAccountUser
  done

  log "10/10 billing budget (alerts only; it does not cap spending)"
  if [[ -z "$(query "" "${G[@]}" billing budgets list --billing-account "$BILLING_ACCOUNT" \
    --filter "displayName=$BUDGET_NAME" --format "value(name)")" ]]; then
    run "${G[@]}" billing budgets create --billing-account "$BILLING_ACCOUNT" \
      --display-name "$BUDGET_NAME" --budget-amount "${BUDGET_AMOUNT}USD" \
      --threshold-rule percent=0.5 --threshold-rule percent=0.9 --threshold-rule percent=1.0 \
      --filter-projects "projects/$PROJECT"
  fi

  echo
  if [[ "$DRY_RUN" == 1 ]]; then
    echo "Dry run: nothing was changed. $DRY_RUN_IP is a documentation placeholder for the reserved IP."
  fi
  echo "Staging URL: https://$site"
  echo "Next: wait for the startup script, then deploy (docs/deployment/staging.md), and run bootstrap-wif.sh once for LM-049."
}

# ---- stop / start / status --------------------------------------------------------------

stop_vm() {
  run "${G[@]}" compute instances stop "$VM" --zone "$ZONE"
  echo "Stopped. Still billed: the boot and data disks, snapshots and the reserved IP."
}

start_vm() {
  run "${G[@]}" compute instances start "$VM" --zone "$ZONE"
  echo "Started. Docker restarts the containers (restart: unless-stopped) once the data disk is mounted."
}

status() {
  local state ip
  state="$(query "DRY_RUN" "${G[@]}" compute instances describe "$VM" --zone "$ZONE" --format "value(status)")"
  ip="$(static_ip)"
  echo "VM $VM ($ZONE): $state"
  echo "IP: $ip"
  echo "URL: https://$(site_address "$ip")"
}

# ---- teardown ------------------------------------------------------------------------------

teardown() {
  [[ "$CONFIRM" == "$PROJECT" ]] || die "teardown deletes all staging data; pass --confirm $PROJECT"
  DRY_RUN_ASSUME_PRESENT=1

  if exists "${G[@]}" compute instances describe "$VM" --zone "$ZONE"; then
    run "${G[@]}" compute instances delete "$VM" --zone "$ZONE"
  fi
  if exists "${G[@]}" compute disks describe "$DATA_DISK" --zone "$ZONE"; then
    run "${G[@]}" compute disks delete "$DATA_DISK" --zone "$ZONE"
  fi
  local snapshot
  for snapshot in $(query "ledgermesh-data-snapshot-example" "${G[@]}" compute snapshots list \
    --filter "sourceDisk~/disks/$DATA_DISK\$" --format "value(name)"); do
    run "${G[@]}" compute snapshots delete "$snapshot"
  done
  if exists "${G[@]}" compute resource-policies describe "$SNAPSHOT_SCHEDULE" --region "$REGION"; then
    run "${G[@]}" compute resource-policies delete "$SNAPSHOT_SCHEDULE" --region "$REGION"
  fi
  if exists "${G[@]}" compute addresses describe "$ADDRESS" --region "$REGION"; then
    run "${G[@]}" compute addresses delete "$ADDRESS" --region "$REGION"
  fi
  local rule
  for rule in "$FW_WEB" "$FW_IAP"; do
    if exists "${G[@]}" compute firewall-rules describe "$rule"; then
      run "${G[@]}" compute firewall-rules delete "$rule"
    fi
  done
  if exists "${G[@]}" compute networks subnets describe "$SUBNET" --region "$REGION"; then
    run "${G[@]}" compute networks subnets delete "$SUBNET" --region "$REGION"
  fi
  if exists "${G[@]}" compute networks describe "$NETWORK"; then
    run "${G[@]}" compute networks delete "$NETWORK"
  fi
  local suffix
  for suffix in "${SECRET_SUFFIXES[@]}"; do
    if exists "${G[@]}" secrets describe "$SECRET_PREFIX-$suffix"; then
      run "${G[@]}" secrets delete "$SECRET_PREFIX-$suffix"
    fi
  done
  local member
  for member in ${ADMIN_MEMBERS[@]+"${ADMIN_MEMBERS[@]}"}; do
    run "${G[@]}" projects remove-iam-policy-binding "$PROJECT" \
      --member "$member" --role roles/iap.tunnelResourceAccessor --condition "$SSH_CONDITION"
    run "${G[@]}" projects remove-iam-policy-binding "$PROJECT" \
      --member "$member" --role roles/compute.viewer --condition None
  done
  if exists "${G[@]}" iam service-accounts describe "$VM_SA"; then
    run "${G[@]}" projects remove-iam-policy-binding "$PROJECT" \
      --member "serviceAccount:$VM_SA" --role roles/logging.logWriter --condition None
    run "${G[@]}" iam service-accounts delete "$VM_SA"
  fi
  if [[ -n "$BILLING_ACCOUNT" ]]; then
    local budget
    for budget in $(query "billingAccounts/000000-000000-000000/budgets/example" "${G[@]}" billing budgets list \
      --billing-account "$BILLING_ACCOUNT" --filter "displayName=$BUDGET_NAME" --format "value(name)"); do
      run "${G[@]}" billing budgets delete "$budget"
    done
  else
    log "no --billing-account given: delete the '$BUDGET_NAME' budget in the console if you no longer want it"
  fi
  echo
  echo "Teardown finished. Also run: infra/staging/bootstrap-wif.sh teardown --project $PROJECT"
  if [[ ${#ADMIN_MEMBERS[@]} -eq 0 ]]; then
    echo "Admin IAM bindings were not removed (no --admin-member given); remove them in IAM if needed."
  fi
}

case "$COMMAND" in
  up) up ;;
  stop) stop_vm ;;
  start) start_vm ;;
  status) status ;;
  teardown) teardown ;;
  -h | --help) usage ;;
  *) usage >&2; die "unknown command: $COMMAND" ;;
esac
