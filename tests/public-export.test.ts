import { copyFile, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "bun:test";

describe("public repository export", () => {
  it("uses the package version throughout the public API metadata", async () => {
    const root = join(import.meta.dir, "..");
    const packageJson = JSON.parse(await readFile(join(root, "package.json"), "utf8")) as { version: string };
    const openapi = await readFile(join(root, "src/openapi.ts"), "utf8");
    expect(packageJson.version).toBe("0.1.0");
    expect(openapi).toContain(`version: "${packageJson.version}"`);
  });

  it("documents only authentication endpoints implemented by the Hono app", async () => {
    const root = join(import.meta.dir, "..");
    const auth = await readFile(join(root, "AUTH.md"), "utf8");
    const app = await readFile(join(root, "src/app.ts"), "utf8");
    const endpoints = [...auth.matchAll(/`(GET|POST|PATCH|DELETE) (\/[^`]+)`/g)];
    expect(endpoints.length).toBeGreaterThan(0);
    for (const endpoint of endpoints) {
      const method = endpoint[1]!.toLowerCase();
      const route = endpoint[2]!.replaceAll("{id}", ":id");
      expect(app, `${endpoint[1]} ${endpoint[2]}`).toContain(`app.${method}("${route}"`);
    }
  });

  it("creates one clean Bugler commit without internal project records", async () => {
    const parent = await mkdtemp(join(tmpdir(), "bugler-public-test-"));
    const destination = join(parent, "public");
    try {
      const child = Bun.spawn(["bash", "scripts/export-public.sh"], {
        cwd: join(import.meta.dir, ".."),
        env: { ...process.env, BUGLER_EXPORT_DIR: destination },
        stdout: "pipe",
        stderr: "pipe",
      });
      const [exitCode, stdout, stderr] = await Promise.all([
        child.exited,
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
      ]);
      expect(exitCode, stderr).toBe(0);
      expect(stdout).toContain(destination);
      expect(await Bun.file(join(destination, "AGENTS.md")).exists()).toBe(true);
      expect(await Bun.file(join(destination, "scripts/export-public.sh")).exists()).toBe(true);
      expect(await Bun.file(join(destination, "site/index.html")).exists()).toBe(true);
      expect(await Bun.file(join(destination, "CHARTER.md")).exists()).toBe(false);
      expect(await Bun.file(join(destination, "design/_body.html")).exists()).toBe(false);
      const log = Bun.spawnSync(["git", "log", "--format=%s"], { cwd: destination });
      expect(log.stdout.toString().trim()).toBe("Bugler v0.1.0");
      expect((await readFile(join(destination, "package.json"), "utf8"))).toContain('"name": "bugler"');
    } finally {
      await rm(parent, { recursive: true, force: true });
    }
  });

  it("rejects planted GitHub token patterns and local hostnames", async () => {
    const root = join(import.meta.dir, "..");
    const parent = await mkdtemp(join(tmpdir(), "bugler-public-secret-test-"));
    const clone = join(parent, "source");
    const destination = join(parent, "bugler-public-output");
    try {
      const cloned = Bun.spawnSync(["git", "clone", "--quiet", "--local", "--no-hardlinks", root, clone]);
      expect(cloned.exitCode, cloned.stderr.toString()).toBe(0);
      await copyFile(join(root, "scripts/export-public.sh"), join(clone, "scripts/export-public.sh"));
      const probes = [
        `gh${"o"}_FAKE_EXPORT_PROBE`,
        `gh${"p"}_FAKE_EXPORT_PROBE`,
        `gh${"s"}_FAKE_EXPORT_PROBE`,
        `github${"_pat"}_FAKE_EXPORT_PROBE`,
        `runner${".local"} is a private hostname`,
      ];
      for (const [index, probe] of probes.entries()) {
        await writeFile(join(clone, "planted.txt"), `credential ${probe} mid-sentence\n`);
        const staged = Bun.spawnSync(["git", "add", "scripts/export-public.sh", "planted.txt"], { cwd: clone });
        expect(staged.exitCode, staged.stderr.toString()).toBe(0);
        const commitArgs = index === 0
          ? ["git", "commit", "-q", "-m", "plant export probe"]
          : ["git", "commit", "-q", "--amend", "--no-edit"];
        const commit = Bun.spawnSync(commitArgs, {
          cwd: clone,
          env: {
            ...process.env,
            GIT_AUTHOR_NAME: "Probe",
            GIT_AUTHOR_EMAIL: "probe@example.invalid",
            GIT_COMMITTER_NAME: "Probe",
            GIT_COMMITTER_EMAIL: "probe@example.invalid",
          },
        });
        expect(commit.exitCode, commit.stderr.toString()).toBe(0);

        const child = Bun.spawn(["bash", "scripts/export-public.sh"], {
          cwd: clone,
          env: { ...process.env, BUGLER_EXPORT_DIR: destination },
          stdout: "pipe",
          stderr: "pipe",
        });
        const [exitCode, stdout, stderr] = await Promise.all([
          child.exited,
          new Response(child.stdout).text(),
          new Response(child.stderr).text(),
        ]);
        expect(exitCode, probe).not.toBe(0);
        expect(`${stdout}\n${stderr}`, probe).toContain("planted.txt");
        expect(`${stdout}\n${stderr}`, probe).toContain("secret-like value");
      }
    } finally {
      await rm(parent, { recursive: true, force: true });
    }
  });
});
