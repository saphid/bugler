import { describe, expect, it } from "bun:test";
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

const root = join(import.meta.dir, "..");

async function run(script: string, args: string[] = [], env: Record<string, string | undefined> = {}): Promise<{ exitCode: number; stdout: string; stderr: string }> {
  const process = Bun.spawn(["bash", script, ...args], { cwd: root, stdout: "pipe", stderr: "pipe", env: { ...Bun.env, ...env, NO_COLOR: "1" } });
  const [exitCode, stdout, stderr] = await Promise.all([
    process.exited,
    new Response(process.stdout).text(),
    new Response(process.stderr).text(),
  ]);
  return { exitCode, stdout, stderr };
}

describe("deployment setup dry runs", () => {
  it("prints every Cloudflare action without executing it", async () => {
    const result = await run("deploy/cloudflare/setup.sh", ["--dry-run"]);
    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain("DRY RUN: bunx wrangler whoami");
    expect(result.stdout).toContain("wrangler types validates the BUGLER Durable Object binding");
    expect(result.stdout).toContain("DRY RUN: prompt securely for BUGLER_BOOTSTRAP_ADMIN_TOKEN");
    expect(result.stdout).toContain("DRY RUN: bunx wrangler secret put BUGLER_GITHUB_SECRET");
    const initialDeploy = result.stdout.indexOf("deploy creates or updates the Worker before secrets");
    const firstSecret = result.stdout.indexOf("prompt securely for BUGLER_BOOTSTRAP_ADMIN_TOKEN");
    const finalDeploy = result.stdout.indexOf("deploy activates the supplied secrets");
    expect(initialDeploy).toBeGreaterThan(-1);
    expect(firstSecret).toBeGreaterThan(initialDeploy);
    expect(finalDeploy).toBeGreaterThan(firstSecret);
    expect(result.stdout).toContain("POST /sources");
    expect(result.stdout).not.toMatch(/bgl_adm_[A-Za-z0-9_-]+/);
  });

  it("prints the idempotent GCP VM plan without touching a project", async () => {
    const result = await run("deploy/gcp/setup-vm.sh", ["--project", "dry-run-project", "--dry-run"]);
    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain("DRY RUN: gcloud auth list");
    expect(result.stdout).toContain("DRY RUN: verify billing is linked for dry-run-project");
    expect(result.stdout).toContain("--machine-type=e2-micro");
    expect(result.stdout).toContain("--boot-disk-size=30GB");
    expect(result.stdout).toContain("bun build --compile --target=bun-linux-x64");
    expect(result.stdout).toContain("systemd unit");
    expect(result.stdout).toContain("stop and disable reveille.service when it exists");
    expect(result.stdout).toContain("restart it");
    expect(result.stdout).toContain("Cloudflare Tunnel");
    expect(result.stdout).toContain("Tailscale Funnel");
  });

  it("preserves the pre-rename Docker database before choosing the Bugler default", async () => {
    const entrypoint = await Bun.file(join(root, "deploy/docker/entrypoint.sh")).text();
    const legacyCheck = entrypoint.indexOf('[ -f /data/reveille.db ]');
    const newDefault = entrypoint.indexOf('DATABASE=/data/bugler.db');
    expect(legacyCheck).toBeGreaterThan(-1);
    expect(newDefault).toBeGreaterThan(legacyCheck);
    expect(entrypoint).toContain("Using legacy database /data/reveille.db");
    expect(entrypoint).toContain("printf 'Using database %s.\\n' \"$DATABASE\"");
  });

  it("prints the DynamoDB plus operator-supplied compute plan without touching AWS", async () => {
    const result = await run("deploy/aws/setup.sh", ["--region", "us-east-1", "--table", "bugler-test", "--dry-run"]);
    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain("DRY RUN: aws sts get-caller-identity");
    expect(result.stdout).toContain("create or reuse DynamoDB table bugler-test");
    expect(result.stdout).toContain("received-index");
    expect(result.stdout).toContain("least-privilege IAM policy");
    expect(result.stdout).toContain("BUGLER_STORAGE=dynamodb");
    expect(result.stdout).toContain("AWS compute is not provisioned");
    expect(result.stdout).toContain("App Runner has no free tier");
  });

  it("prints both honest Azure Container Apps modes without touching Azure", async () => {
    const zero = await run("deploy/azure/setup.sh", [
      "--resource-group", "bugler-test",
      "--image", "registry.example/bugler:latest",
      "--mode", "scale-to-zero",
      "--key-vault", "bugler-review-kv",
      "--dry-run",
    ]);
    expect(zero.exitCode).toBe(0);
    expect(zero.stdout).toContain("DRY RUN: az account show");
    expect(zero.stdout).toContain("Cosmos free-tier account");
    expect(zero.stdout).toContain("min replicas 0");
    expect(zero.stdout).toContain("cold starts can miss GitHub's 10 second deadline");
    expect(zero.stdout).toContain("az containerapp secret set using a Key Vault reference");
    expect(zero.stdout).toContain("check global Key Vault name availability for bugler-review-kv");
    expect(zero.stdout).toContain("check live and soft-deleted Key Vault name bugler-review-kv");
    expect(zero.stdout).toContain("recover only a matching soft-deleted vault; never purge a protected vault");
    expect(zero.stdout).not.toContain("C2y6yDjf5");

    const warm = await run("deploy/azure/setup.sh", [
      "--resource-group", "bugler-test",
      "--image", "registry.example/bugler:latest",
      "--mode", "always-on",
      "--dry-run",
    ]);
    expect(warm.exitCode).toBe(0);
    expect(warm.stdout).toContain("min replicas 1");
    expect(warm.stdout).toContain("about $3 to $5 per month");
  });

  it("runs the Azure re-entry path under the macOS Bash 3.2 system shell", async () => {
    const directory = await mkdtemp(join(tmpdir(), "bugler-azure-bash32-"));
    try {
      const az = join(directory, "az");
      await writeFile(az, `#!/bin/bash
case "$*" in
  *"keyvault show"*) printf '%s\\n' bugler-test ;;
  *"keyvault check-name"*) printf '%s\\n' true ;;
  *"containerapp identity show"*) printf '%s\\n' principal-id ;;
  *"cosmosdb keys list"*) printf '%s\\n' emulator-key ;;
  *"keyvault secret show"*) printf '%s\\n' https://vault.example/secrets/bugler ;;
  *"cosmosdb show"*"documentEndpoint"*) printf '%s\\n' https://cosmos.example ;;
  *"containerapp show"*"fqdn"*) printf '%s\\n' relay.example ;;
esac
exit 0
`);
      await chmod(az, 0o755);
      const child = Bun.spawn([
        "/bin/bash", "deploy/azure/setup.sh",
        "--resource-group", "bugler-test",
        "--image", "registry.example/bugler:latest",
        "--mode", "scale-to-zero",
        "--key-vault", "bugler-review-kv",
        "--yes",
      ], {
        cwd: root,
        stdout: "pipe",
        stderr: "pipe",
        env: { ...Bun.env, PATH: `${directory}:${Bun.env.PATH}` },
      });
      const [exitCode, stdout, stderr] = await Promise.all([
        child.exited,
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
      ]);
      expect(exitCode, stderr).toBe(0);
      expect(stdout).toContain("Bugler URL: https://relay.example");
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("prints the GitHub webhook create-or-update plan and requires consent for mutation", async () => {
    const dry = await run("deploy/github/wire-repo.sh", ["acme/widgets", "https://relay.example/ingest/github-widgets", "--dry-run"]);
    expect(dry.exitCode).toBe(0);
    expect(dry.stdout).toContain("DRY RUN: gh api GET repos/acme/widgets/hooks");
    expect(dry.stdout).toContain("create or update webhook with events: pull_request,pull_request_review,check_run,issue_comment");
    expect(dry.stdout).toContain("env:BUGLER_GITHUB_SECRET");
    expect(dry.stdout).toContain("source id github-widgets maps to github/acme/widgets");
    expect(dry.stdout).not.toMatch(/[a-f0-9]{64}/);
    const refused = await run("deploy/github/wire-repo.sh", ["acme/widgets", "https://relay.example/ingest/github-widgets"]);
    expect(refused.exitCode).not.toBe(0);
    expect(refused.stderr).toContain("--yes is required");
    expect(await Bun.file(join(root, "deploy/github/wire-repo.sh")).text()).not.toContain("|| true)");
  });

  it("fails closed when GitHub hook listing fails", async () => {
    const directory = await mkdtemp(join(tmpdir(), "bugler-deploy-test-"));
    try {
      const gh = join(directory, "gh");
      const bun = join(directory, "bun");
      const openssl = join(directory, "openssl");
      await writeFile(gh, "#!/usr/bin/env bash\nif [[ ${1:-} == auth ]]; then exit 0; fi\nexit 19\n");
      await writeFile(bun, "#!/usr/bin/env bash\nwhile IFS= read -r _; do :; done\n");
      await writeFile(openssl, "#!/usr/bin/env bash\nexit 99\n");
      await Promise.all([gh, bun, openssl].map((path) => chmod(path, 0o755)));
      const result = await run(
        "deploy/github/wire-repo.sh",
        ["acme/widgets", "https://relay.example/ingest/github-widgets", "--yes"],
        { PATH: `${directory}:${Bun.env.PATH ?? ""}` },
      );
      expect(result.exitCode).toBe(19);
      expect(result.stdout).not.toContain("Created webhook");
      expect(result.stdout).not.toContain("Webhook secret");
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
});
