#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
PROJECT=''
REGION='us-central1'
FIRESTORE_LOCATION='us-central1'
SERVICE='bugler'
REPOSITORY='bugler'
DATABASE='(default)'
NAMESPACE='bugler'
DRY_RUN=0
YES=0

usage() {
  printf 'Usage: %s --project PROJECT [--region REGION] [--firestore-location LOCATION] [--service NAME] [--repository NAME] [--database NAME] [--namespace NAME] [--yes] [--dry-run]\n' "$0"
}

while (($# > 0)); do
  case "$1" in
    --project) PROJECT=${2:?--project requires a value}; shift ;;
    --region) REGION=${2:?--region requires a value}; shift ;;
    --firestore-location) FIRESTORE_LOCATION=${2:?--firestore-location requires a value}; shift ;;
    --service) SERVICE=${2:?--service requires a value}; shift ;;
    --repository) REPOSITORY=${2:?--repository requires a value}; shift ;;
    --database) DATABASE=${2:?--database requires a value}; shift ;;
    --namespace) NAMESPACE=${2:?--namespace requires a value}; shift ;;
    --yes) YES=1 ;;
    --dry-run) DRY_RUN=1 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'Unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

if [[ -z "$PROJECT" ]]; then
  printf '%s\n' '--project is required' >&2
  exit 2
fi
if ((${#SERVICE} < 2)) || [[ ! "$SERVICE" =~ ^[a-z][a-z0-9-]{0,47}[a-z0-9]$ ]]; then
  printf '%s\n' '--service must be a lowercase Cloud Run service name of 2 to 49 characters' >&2
  exit 2
fi
if [[ ! "$REPOSITORY" =~ ^[a-z][a-z0-9_-]{0,61}[a-z0-9]$ && ! "$REPOSITORY" =~ ^[a-z]$ ]]; then
  printf '%s\n' '--repository must be a lowercase Artifact Registry repository name' >&2
  exit 2
fi
if [[ -z "$DATABASE" || "$DATABASE" =~ [,=[:space:]] ]]; then
  printf '%s\n' '--database must not be empty or contain commas, equals signs, or whitespace' >&2
  exit 2
fi
if [[ -z "$NAMESPACE" || "$NAMESPACE" =~ [,=[:space:]] ]]; then
  printf '%s\n' '--namespace must not be empty or contain commas, equals signs, or whitespace' >&2
  exit 2
fi

SERVICE_ACCOUNT="${SERVICE:0:24}-run"
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT}@${PROJECT}.iam.gserviceaccount.com"
TRUNCATION_WARNING=''
if ((${#SERVICE} > 24)); then
  TRUNCATION_WARNING="WARNING: the service account id truncates --service to ${SERVICE:0:24}; any other service sharing this 24-character prefix would silently reuse $SERVICE_ACCOUNT_EMAIL. Passing --yes acknowledges this."
fi
SECRET='bugler-bootstrap-admin-token'
IMAGE="${REGION}-docker.pkg.dev/${PROJECT}/${REPOSITORY}/${SERVICE}:latest"
COLD_START_NOTE="min instances 0 can cold start beyond GitHub's 10 second response deadline; GitHub does not automatically retry a failed delivery"

if ((DRY_RUN)); then
  if [[ -n "$TRUNCATION_WARNING" ]]; then
    printf '%s\n' "$TRUNCATION_WARNING"
  fi
  printf '%s\n' \
    'DRY RUN: gcloud auth list --filter=status:ACTIVE' \
    "DRY RUN: verify billing is linked for $PROJECT without changing billing settings" \
    "DRY RUN: enable run.googleapis.com firestore.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com secretmanager.googleapis.com iam.googleapis.com for $PROJECT" \
    "DRY RUN: create or reuse Firestore Native database $DATABASE in $FIRESTORE_LOCATION" \
    'DRY RUN: create or reuse the source/subject/seq and source/type/seq composite indexes from firestore.indexes.json' \
    "DRY RUN: create or reuse service account $SERVICE_ACCOUNT_EMAIL with roles/datastore.user" \
    "DRY RUN: securely prompt for BUGLER_BOOTSTRAP_ADMIN_TOKEN only when secret $SECRET has no enabled version" \
    'DRY RUN: stream the bootstrap token to Secret Manager without argv or persistent files and grant the service identity access' \
    "DRY RUN: create or reuse Docker Artifact Registry repository $REPOSITORY in $REGION" \
    "DRY RUN: gcloud builds submit $PROJECT_DIR --tag=$IMAGE" \
    "DRY RUN: deploy Cloud Run service $SERVICE publicly with BUGLER_STORAGE=firestore, the modern BUGLER_FIRESTORE_* environment, min instances 0, and max instances 1" \
    "Cold-start note: $COLD_START_NOTE." \
    'Scheduler note: while scaled to zero, Bugler cannot originate poll or retention work; an inbound request must first start an instance.'
  exit 0
fi

if [[ -n "$TRUNCATION_WARNING" ]]; then
  printf '%s\n' "$TRUNCATION_WARNING" >&2
fi
if ((YES == 0)); then
  printf '%s\n' '--yes is required because this command changes Google Cloud resources' >&2
  exit 2
fi

command -v gcloud >/dev/null 2>&1 || { printf '%s\n' 'gcloud is required' >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || { printf '%s\n' 'python3 is required to read firestore.indexes.json' >&2; exit 1; }
ACTIVE_ACCOUNT=$(gcloud auth list --filter=status:ACTIVE --format='value(account)' | head -n 1)
if [[ -z "$ACTIVE_ACCOUNT" ]]; then
  printf '%s\n' 'No active gcloud account. Run gcloud auth login first.' >&2
  exit 1
fi
BILLING_ENABLED=$(gcloud billing projects describe "$PROJECT" --format='value(billingEnabled)' 2>/dev/null || true)
if [[ "$BILLING_ENABLED" != 'True' && "$BILLING_ENABLED" != 'true' ]]; then
  printf 'Project %s has no linked billing account. Link one before continuing. This script never changes billing settings.\n' "$PROJECT" >&2
  exit 1
fi

gcloud services enable \
  run.googleapis.com \
  firestore.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  secretmanager.googleapis.com \
  iam.googleapis.com \
  --project="$PROJECT"

if ! gcloud firestore databases describe --database="$DATABASE" --project="$PROJECT" >/dev/null 2>&1; then
  gcloud firestore databases create \
    --database="$DATABASE" \
    --location="$FIRESTORE_LOCATION" \
    --type=firestore-native \
    --project="$PROJECT" \
    --quiet
fi

# firestore.indexes.json is the single source of truth for composite indexes.
# An index counts as existing only when its query scope, collection group, and
# ordered fieldPath sequence match exactly (ignoring the automatic __name__ field).
EXISTING_INDEXES=$(gcloud firestore indexes composite list \
  --database="$DATABASE" \
  --project="$PROJECT" \
  --format=json)
while IFS='|' read -r INDEX_GROUP INDEX_SCOPE INDEX_CONFIGS; do
  [[ -z "$INDEX_GROUP" ]] && continue
  FIELD_ARGS=()
  IFS=',' read -ra INDEX_PARTS <<<"$INDEX_CONFIGS"
  for INDEX_PART in "${INDEX_PARTS[@]}"; do
    FIELD_ARGS+=("--field-config=field-path=${INDEX_PART%%:*},order=${INDEX_PART#*:}")
  done
  gcloud firestore indexes composite create \
    --database="$DATABASE" \
    --collection-group="$INDEX_GROUP" \
    --query-scope="$INDEX_SCOPE" \
    "${FIELD_ARGS[@]}" \
    --project="$PROJECT" \
    --quiet
done < <(EXISTING_INDEXES="$EXISTING_INDEXES" python3 - "$PROJECT_DIR/firestore.indexes.json" <<'PYEOF'
import json
import os
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    wanted = json.load(handle)["indexes"]
existing = json.loads(os.environ.get("EXISTING_INDEXES") or "[]")


def shape(scope, group, fields):
    ordered = tuple(
        (field["fieldPath"], field.get("order", "").upper())
        for field in fields
        if field["fieldPath"] != "__name__"
    )
    return (scope.upper(), group, ordered)


have = set()
for index in existing:
    group = index.get("name", "").split("/collectionGroups/")[-1].split("/")[0]
    have.add(shape(index.get("queryScope", "COLLECTION"), group, index.get("fields", [])))

for index in wanted:
    key = shape(index.get("queryScope", "COLLECTION"), index["collectionGroup"], index["fields"])
    if key in have:
        continue
    scope_flag = "collection-group" if key[0] == "COLLECTION_GROUP" else "collection"
    configs = ",".join(f"{path}:{order.lower()}" for path, order in key[2])
    print(f"{key[1]}|{scope_flag}|{configs}")
PYEOF
)

if ! gcloud iam service-accounts describe "$SERVICE_ACCOUNT_EMAIL" --project="$PROJECT" >/dev/null 2>&1; then
  gcloud iam service-accounts create "$SERVICE_ACCOUNT" --display-name='Bugler Cloud Run' --project="$PROJECT"
fi
gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
  --role=roles/datastore.user \
  --condition=None \
  --quiet >/dev/null

if ! gcloud secrets describe "$SECRET" --project="$PROJECT" >/dev/null 2>&1; then
  gcloud secrets create "$SECRET" --replication-policy=automatic --project="$PROJECT"
fi
SECRET_VERSION=$(gcloud secrets versions list "$SECRET" --project="$PROJECT" --filter='state=ENABLED' --limit=1 --format='value(name)')
if [[ -z "$SECRET_VERSION" ]]; then
  printf '%s' 'Enter a valid bgl_adm_ bootstrap token. Input is hidden: ' >&2
  IFS= read -r -s BOOTSTRAP_TOKEN
  printf '\n' >&2
  if [[ ! "$BOOTSTRAP_TOKEN" =~ ^bgl_adm_ ]]; then
    printf '%s\n' 'Bootstrap token must start with bgl_adm_' >&2
    exit 1
  fi
  printf '%s' "$BOOTSTRAP_TOKEN" | gcloud secrets versions add "$SECRET" --data-file=- --project="$PROJECT" >/dev/null
  unset BOOTSTRAP_TOKEN
fi
gcloud secrets add-iam-policy-binding "$SECRET" \
  --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
  --role=roles/secretmanager.secretAccessor \
  --project="$PROJECT" \
  --quiet >/dev/null

if ! gcloud artifacts repositories describe "$REPOSITORY" --location="$REGION" --project="$PROJECT" >/dev/null 2>&1; then
  gcloud artifacts repositories create "$REPOSITORY" \
    --repository-format=docker \
    --location="$REGION" \
    --description='Bugler Cloud Run images' \
    --project="$PROJECT"
fi

gcloud builds submit "$PROJECT_DIR" --tag="$IMAGE" --project="$PROJECT"
gcloud run deploy "$SERVICE" \
  --image="$IMAGE" \
  --region="$REGION" \
  --project="$PROJECT" \
  --platform=managed \
  --service-account="$SERVICE_ACCOUNT_EMAIL" \
  --port=8787 \
  --min-instances=0 \
  --max-instances=1 \
  --set-env-vars="BUGLER_STORAGE=firestore,BUGLER_FIRESTORE_PROJECT=$PROJECT,BUGLER_FIRESTORE_DATABASE=$DATABASE,BUGLER_FIRESTORE_NAMESPACE=$NAMESPACE" \
  --set-secrets="BUGLER_BOOTSTRAP_ADMIN_TOKEN=$SECRET:latest" \
  --allow-unauthenticated \
  --quiet

URL=$(gcloud run services describe "$SERVICE" --region="$REGION" --project="$PROJECT" --format='value(status.url)')
printf 'Bugler URL: %s\n' "$URL"
printf 'Cold-start note: %s.\n' "$COLD_START_NOTE"
printf '%s\n' 'While the service is scaled to zero, scheduled polls and retention cannot originate work. An inbound webhook, pull, or health request must start an instance first.'
