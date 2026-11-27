#!/usr/bin/env bash
# One-time Workload Identity Federation setup for GitHub Actions (LM-048, used by LM-049).
#
#   bootstrap-wif.sh [up] [--dry-run] [--project <id>] [--zone <zone>] [--vm <name>]
#   bootstrap-wif.sh teardown --confirm <project> [--dry-run] [...]
#
# Lets a GitHub Actions job in sonalilonkar1/LedgerMesh, running for
# refs/heads/main, impersonate a deploy service account without any stored
# key. The provider's attribute condition rejects tokens from any other
# repository (by name and immutable id) or ref, so pull requests and forks
# cannot obtain credentials.
#
# The deploy service account can only reach the staging VM over IAP SSH
# (port 22) with OS Login admin, read Compute metadata, and act as the VM's
# service account (required by OS Login). It has no Secret Manager access:
# the VM reads its own secrets.
#
# Run provision.sh up first; the VM and its service account must exist.
# Prints the values for the GitHub `staging` environment variables.
set -euo pipefail

PROJECT="${LEDGERMESH_GCP_PROJECT:-deven-cloud-lab}"
ZONE="us-west1-b"
VM="ledgermesh-staging"
DRY_RUN=0
CONFIRM=""

REPOSITORY="sonalilonkar1/LedgerMesh"
# Immutable id of the repository (gh api repos/sonalilonkar1/LedgerMesh --jq .id).
# Pinning it means a deleted and re-created repository with the same name
# cannot inherit access.
REPOSITORY_ID="1370369282"
BRANCH_REF="refs/heads/main"
POOL="github"
PROVIDER="ledgermesh-github"
ISSUER="https://token.actions.githubusercontent.com"
DEPLOY_SA_NAME="ledgermesh-deployer"
VM_SA_NAME="ledgermesh-staging-vm"
ATTRIBUTE_MAPPING="google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.repository_id=assertion.repository_id,attribute.ref=assertion.ref"
ATTRIBUTE_CONDITION="assertion.repository=='$REPOSITORY' && assertion.repository_id=='$REPOSITORY_ID' && assertion.ref=='$BRANCH_REF'"
SSH_CONDITION="expression=destination.port == 22,title=ledgermesh-staging-ssh-only,description=IAP TCP forwarding to SSH only"
APIS=(iam.googleapis.com iamcredentials.googleapis.com sts.googleapis.com
  iap.googleapis.com compute.googleapis.com oslogin.googleapis.com)
DRY_RUN_PROJECT_NUMBER="123456789012"

die() {
  echo "bootstrap-wif: $*" >&2
  exit 1
}

usage() {
  sed -n '2,21p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

COMMAND="up"
if [[ $# -gt 0 && "$1" != --* ]]; then
  COMMAND="$1"
  shift
fi
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --project) [[ $# -ge 2 ]] || die "$1 needs a value"; PROJECT="$2"; shift 2 ;;
    --zone) [[ $# -ge 2 ]] || die "$1 needs a value"; ZONE="$2"; shift 2 ;;
    --vm) [[ $# -ge 2 ]] || die "$1 needs a value"; VM="$2"; shift 2 ;;
    --confirm) [[ $# -ge 2 ]] || die "$1 needs a value"; CONFIRM="$2"; shift 2 ;;
    -h | --help) usage; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done
[[ "$PROJECT" =~ ^[a-z][a-z0-9-]{4,28}[a-z0-9]$ ]] || die "invalid --project: $PROJECT"

DEPLOY_SA="$DEPLOY_SA_NAME@$PROJECT.iam.gserviceaccount.com"
VM_SA="$VM_SA_NAME@$PROJECT.iam.gserviceaccount.com"
G=(gcloud --project "$PROJECT" --quiet)

quote() {
  local out="" arg
  for arg in "$@"; do
    out+="$(printf '%q' "$arg") "
  done
  echo "${out% }"
}

run() {
  echo "+ $(quote "$@")"
  if [[ "$DRY_RUN" == 0 ]]; then
    "$@"
  fi
}

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

DRY_RUN_ASSUME_PRESENT=0
exists() {
  if [[ "$DRY_RUN" == 1 ]]; then
    echo "# check: $(quote "$@")"
    [[ "$DRY_RUN_ASSUME_PRESENT" == 1 ]]
    return
  fi
  "$@" >/dev/null 2>&1
}

project_number() {
  if [[ "$DRY_RUN" == 1 ]]; then
    echo "# query: $(quote gcloud projects describe "$PROJECT" --format 'value(projectNumber)')" >&2
    echo "$DRY_RUN_PROJECT_NUMBER"
    return 0
  fi
  gcloud projects describe "$PROJECT" --format 'value(projectNumber)'
}

up() {
  local number principal provider_name
  number="$(project_number)"
  [[ "$number" =~ ^[0-9]+$ ]] || die "could not read the project number"
  principal="principalSet://iam.googleapis.com/projects/$number/locations/global/workloadIdentityPools/$POOL/attribute.repository/$REPOSITORY"
  provider_name="projects/$number/locations/global/workloadIdentityPools/$POOL/providers/$PROVIDER"

  if [[ "$DRY_RUN" == 0 ]]; then
    exists "${G[@]}" compute instances describe "$VM" --zone "$ZONE" \
      || die "VM $VM not found in $ZONE; run provision.sh up first"
    exists "${G[@]}" iam service-accounts describe "$VM_SA" \
      || die "VM service account $VM_SA not found; run provision.sh up first"
  fi

  run "${G[@]}" services enable "${APIS[@]}"

  if ! exists "${G[@]}" iam workload-identity-pools describe "$POOL" --location global; then
    run "${G[@]}" iam workload-identity-pools create "$POOL" --location global \
      --display-name "GitHub Actions"
  fi
  if ! exists "${G[@]}" iam workload-identity-pools providers describe "$PROVIDER" \
    --location global --workload-identity-pool "$POOL"; then
    run "${G[@]}" iam workload-identity-pools providers create-oidc "$PROVIDER" \
      --location global --workload-identity-pool "$POOL" \
      --display-name "LedgerMesh GitHub main" \
      --issuer-uri "$ISSUER" \
      --attribute-mapping "$ATTRIBUTE_MAPPING" \
      --attribute-condition "$ATTRIBUTE_CONDITION"
  else
    # Keep the condition exactly as committed on re-runs.
    run "${G[@]}" iam workload-identity-pools providers update-oidc "$PROVIDER" \
      --location global --workload-identity-pool "$POOL" \
      --attribute-mapping "$ATTRIBUTE_MAPPING" \
      --attribute-condition "$ATTRIBUTE_CONDITION"
  fi

  if ! exists "${G[@]}" iam service-accounts describe "$DEPLOY_SA"; then
    run "${G[@]}" iam service-accounts create "$DEPLOY_SA_NAME" \
      --display-name "LedgerMesh staging deployer (GitHub Actions)"
  fi
  run_iam "${G[@]}" iam service-accounts add-iam-policy-binding "$DEPLOY_SA" \
    --member "$principal" --role roles/iam.workloadIdentityUser
  run_iam "${G[@]}" compute instances add-iam-policy-binding "$VM" --zone "$ZONE" \
    --member "serviceAccount:$DEPLOY_SA" --role roles/compute.osAdminLogin
  run_iam "${G[@]}" projects add-iam-policy-binding "$PROJECT" \
    --member "serviceAccount:$DEPLOY_SA" --role roles/iap.tunnelResourceAccessor \
    --condition "$SSH_CONDITION"
  run_iam "${G[@]}" projects add-iam-policy-binding "$PROJECT" \
    --member "serviceAccount:$DEPLOY_SA" --role roles/compute.viewer --condition None
  run_iam "${G[@]}" iam service-accounts add-iam-policy-binding "$VM_SA" \
    --member "serviceAccount:$DEPLOY_SA" --role roles/iam.serviceAccountUser

  echo
  [[ "$DRY_RUN" == 1 ]] && echo "Dry run: nothing was changed. $DRY_RUN_PROJECT_NUMBER is a placeholder project number."
  echo "GitHub 'staging' environment variables (docs/deployment/staging.md):"
  echo "  GCP_PROJECT_ID=$PROJECT"
  echo "  GCP_ZONE=$ZONE"
  echo "  STAGING_VM_NAME=$VM"
  echo "  GCP_WORKLOAD_IDENTITY_PROVIDER=$provider_name"
  echo "  GCP_DEPLOY_SERVICE_ACCOUNT=$DEPLOY_SA"
}

teardown() {
  [[ "$CONFIRM" == "$PROJECT" ]] || die "teardown removes deploy access; pass --confirm $PROJECT"
  DRY_RUN_ASSUME_PRESENT=1
  if exists "${G[@]}" iam service-accounts describe "$DEPLOY_SA"; then
    run "${G[@]}" projects remove-iam-policy-binding "$PROJECT" \
      --member "serviceAccount:$DEPLOY_SA" --role roles/iap.tunnelResourceAccessor \
      --condition "$SSH_CONDITION"
    run "${G[@]}" projects remove-iam-policy-binding "$PROJECT" \
      --member "serviceAccount:$DEPLOY_SA" --role roles/compute.viewer --condition None
    run "${G[@]}" iam service-accounts delete "$DEPLOY_SA"
  fi
  if exists "${G[@]}" iam workload-identity-pools providers describe "$PROVIDER" \
    --location global --workload-identity-pool "$POOL"; then
    run "${G[@]}" iam workload-identity-pools providers delete "$PROVIDER" \
      --location global --workload-identity-pool "$POOL"
  fi
  if exists "${G[@]}" iam workload-identity-pools describe "$POOL" --location global; then
    run "${G[@]}" iam workload-identity-pools delete "$POOL" --location global
  fi
  echo "Deleted pools and providers stay soft-deleted for 30 days, during which their ids cannot be reused."
}

case "$COMMAND" in
  up) up ;;
  teardown) teardown ;;
  *) usage >&2; die "unknown command: $COMMAND" ;;
esac
