#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
PROJECT=""
ZONE="us-west1-b"
INSTANCE="bugler"
DRY_RUN=0

usage() {
  printf 'Usage: %s --project PROJECT [--zone ZONE] [--instance NAME] [--dry-run]\n' "$0"
}

while (($# > 0)); do
  case "$1" in
    --project) PROJECT=${2:-}; shift ;;
    --zone) ZONE=${2:-}; shift ;;
    --instance) INSTANCE=${2:-}; shift ;;
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
if [[ ! "$ZONE" =~ ^(us-west1|us-central1|us-east1)-[a-z]$ ]]; then
  printf 'Zone %s is outside the Compute Engine free-tier regions\n' "$ZONE" >&2
  exit 2
fi

if ((DRY_RUN)); then
  printf '%s\n' \
    'DRY RUN: gcloud auth list --filter=status:ACTIVE' \
    "DRY RUN: verify billing is linked for $PROJECT without changing billing settings" \
    "DRY RUN: gcloud services enable compute.googleapis.com --project=$PROJECT" \
    'DRY RUN: bun build --compile --target=bun-linux-x64 src/bugler.ts --outfile dist/bugler-linux-x64' \
    "DRY RUN: create $INSTANCE in $ZONE if absent with --machine-type=e2-micro --boot-disk-type=pd-standard --boot-disk-size=30GB" \
    "DRY RUN: gcloud compute scp the compiled binary to $INSTANCE" \
    'DRY RUN: stop and disable reveille.service when it exists' \
    'DRY RUN: create the bugler user and data directory, initialize the database once, install the systemd unit, and restart it' \
    'DRY RUN: systemd unit runs bugler serve on 0.0.0.0:8787 with /var/lib/bugler/bugler.db' \
    'Firewall guidance: expose TCP 8787 only to a trusted ingress path.' \
    'Cloudflare Tunnel guidance: publish localhost:8787 without a public VM port.' \
    'Tailscale Funnel guidance: publish localhost:8787 through the tailnet when appropriate.'
  exit 0
fi

command -v gcloud >/dev/null 2>&1 || { printf 'gcloud is required\n' >&2; exit 1; }
command -v bun >/dev/null 2>&1 || { printf 'bun is required\n' >&2; exit 1; }
ACTIVE_ACCOUNT=$(gcloud auth list --filter=status:ACTIVE --format='value(account)' | head -n 1)
if [[ -z "$ACTIVE_ACCOUNT" ]]; then
  printf 'No active gcloud account. Run gcloud auth login first.\n' >&2
  exit 1
fi
BILLING_ENABLED=$(gcloud billing projects describe "$PROJECT" --format='value(billingEnabled)' 2>/dev/null || true)
if [[ "$BILLING_ENABLED" != "True" && "$BILLING_ENABLED" != "true" ]]; then
  printf 'Project %s has no linked billing account. Link one before continuing. This script never changes billing settings.\n' "$PROJECT" >&2
  exit 1
fi

gcloud services enable compute.googleapis.com --project="$PROJECT"
mkdir -p "$PROJECT_DIR/dist"
cd "$PROJECT_DIR"
bun build --compile --target=bun-linux-x64 src/bugler.ts --outfile dist/bugler-linux-x64

if ! gcloud compute instances describe "$INSTANCE" --project="$PROJECT" --zone="$ZONE" >/dev/null 2>&1; then
  gcloud compute instances create "$INSTANCE" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type=e2-micro \
    --boot-disk-type=pd-standard \
    --boot-disk-size=30GB \
    --image-family=debian-12 \
    --image-project=debian-cloud \
    --tags=bugler
fi

gcloud compute scp dist/bugler-linux-x64 "$INSTANCE:/tmp/bugler" --project="$PROJECT" --zone="$ZONE"

UNIT=$(cat <<'EOF'
[Unit]
Description=Bugler event relay
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=bugler
Group=bugler
ExecStart=/usr/local/bin/bugler serve --db /var/lib/bugler/bugler.db --host 0.0.0.0 --port 8787
Restart=always
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF
)
UNIT_BASE64=$(printf '%s' "$UNIT" | base64 | tr -d '\n')
REMOTE_COMMAND="sudo install -m 0755 /tmp/bugler /usr/local/bin/bugler; if sudo systemctl list-unit-files reveille.service --no-legend 2>/dev/null | grep -q '^reveille.service'; then sudo systemctl disable --now reveille.service; fi; id -u bugler >/dev/null 2>&1 || sudo useradd --system --home /var/lib/bugler --shell /usr/sbin/nologin bugler; sudo install -d -o bugler -g bugler -m 0750 /var/lib/bugler; if [ ! -f /var/lib/bugler/bugler.db ]; then sudo -u bugler /usr/local/bin/bugler init --db /var/lib/bugler/bugler.db; fi; printf '%s' '$UNIT_BASE64' | base64 -d | sudo tee /etc/systemd/system/bugler.service >/dev/null; sudo systemctl daemon-reload; sudo systemctl enable bugler.service; sudo systemctl restart bugler.service"
gcloud compute ssh "$INSTANCE" --project="$PROJECT" --zone="$ZONE" --command="$REMOTE_COMMAND"

printf '\nBugler is running on %s:8787 inside the VM.\n' "$INSTANCE"
printf '%s\n' \
  'Firewall guidance: expose TCP 8787 only to a trusted ingress path. Do not open it globally by default.' \
  'Cloudflare Tunnel: point a tunnel at http://localhost:8787 for public webhook ingress.' \
  'Tailscale Funnel: publish http://localhost:8787 if that trust model fits your deployment.'
