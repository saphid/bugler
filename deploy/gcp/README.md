# GCP deployment profiles

## Cloud Run scale-to-zero with Firestore

`setup-cloudrun.sh` creates or reuses a Firestore Native database, its source-subject and source-type composite indexes, a dedicated service identity, a Secret Manager bootstrap token, a Docker Artifact Registry repository, and a Cloud Run service. Cloud Build builds the repository Dockerfile and pushes the image. The service receives current `BUGLER_*` configuration, including `BUGLER_STORAGE=firestore`; deprecated `REVEILLE_*` aliases remain an application-level upgrade fallback but are not written by the setup script.

Preview without changing GCP:

```bash
bash deploy/gcp/setup-cloudrun.sh --project YOUR_PROJECT --dry-run
```

Run only after reviewing the output. Generate a valid `bgl_adm_` token, store it immediately, then paste it at the script's hidden prompt:

```bash
bun -e 'import { mintToken } from "./src/auth"; console.log(mintToken("adm").token)'
bash deploy/gcp/setup-cloudrun.sh --project YOUR_PROJECT --yes
```

The setup script prompts securely if Secret Manager has no enabled bootstrap version. The token is streamed to Secret Manager without appearing in process arguments or a persistent file.

The service has zero minimum instances and one maximum instance. Zero minimum can remove idle compute cost, but it has two operational consequences:

- A cold start can exceed GitHub's 10 second response deadline, and GitHub does not automatically retry a failed delivery.
- No process exists to originate poll or retention schedules while the service is at zero. An inbound webhook, pull, health check, or other request must first start an instance.

Keep a listener request active or choose the VM profile when autonomous schedules and prompt webhook admission matter. Firestore, Artifact Registry storage, Cloud Build, Secret Manager, networking, and Cloud Run usage can still incur charges outside their current free grants. The script requires an already linked billing account and never changes billing settings.

Firestore document values have a 1 MiB service limit. The driver advertises 1,014,804 payload bytes after subtracting a measured field envelope, a document-name reserve, and 32 KiB of safety headroom. `measureSizeBytes` and retention use the transactionally maintained UTF-8 byte length of semantic event JSON. This is deliberately an approximation of logical event size, not billed Firestore document, index, or backup storage.

## Always-on VM with SQLite

`setup-vm.sh` creates one `e2-micro` VM in `us-west1`, `us-central1`, or `us-east1` with a 30 GB standard persistent disk, then runs the compiled Bugler binary under systemd.

Within Google's documented always-free allowance, the VM and disk cost $0 at low volume. A public external IPv4 address may be billed depending on current Google Cloud terms. Avoid that caveat by keeping the VM private and using Cloudflare Tunnel or Tailscale Funnel for webhook ingress.

Preview without changing GCP:

```bash
bash deploy/gcp/setup-vm.sh --project YOUR_PROJECT --dry-run
```

Run only after reviewing the output:

```bash
bash deploy/gcp/setup-vm.sh --project YOUR_PROJECT --zone us-west1-b
```

The VM script checks active authentication and requires an already linked billing account. It never enables, changes, or links billing.
