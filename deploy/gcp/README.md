# GCP deployment profiles

The blessed current GCP path is `setup-vm.sh`. It creates one `e2-micro` VM in `us-west1`, `us-central1`, or `us-east1` with a 30 GB standard persistent disk, then runs the compiled Bugler binary under systemd.

Within Google's documented always-free allowance, the VM and disk cost $0 at low volume. A public external IPv4 address may be billed depending on current Google Cloud terms. Avoid that caveat by keeping the VM private and using Cloudflare Tunnel or Tailscale Funnel for webhook ingress.

Cloud Run scale-to-zero is intentionally pending. That profile needs the Firestore storage driver planned for a later P5 increment. Running the SQLite binary on ephemeral Cloud Run storage would violate Bugler's durability contract.

Preview without changing GCP:

```bash
bash deploy/gcp/setup-vm.sh --project YOUR_PROJECT --dry-run
```

Run only after reviewing the output:

```bash
bash deploy/gcp/setup-vm.sh --project YOUR_PROJECT --zone us-west1-b
```

The script checks active authentication and requires an already linked billing account. It never enables, changes, or links billing.
