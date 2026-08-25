import { createHash } from "node:crypto";
import { chmod, lstat, mkdir, readFile, realpath, rename, rm, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, join } from "node:path";
import type { StoredEvent } from "../domain";
import { isRecord, newUlid } from "../util";
import type { LocalStore } from "./store";
import type { LocalActionAttempt, LocalActionRecord } from "./types";

type ActionAttribute = "id" | "source" | "type" | "subject" | "time" | `data.${string}`;
type ActionRule =
  | { and: ActionRule[] }
  | { or: ActionRule[] }
  | { not: ActionRule }
  | { match: { attribute: ActionAttribute; value: unknown } }
  | { prefix: { attribute: ActionAttribute; value: string } }
  | { suffix: { attribute: ActionAttribute; value: string } }
  | { regex: { attribute: ActionAttribute; value: string } };

export interface ActionDefinition {
  version: 1;
  id: string;
  enabled: boolean;
  mirrors: string[];
  "trigger-rule": ActionRule;
  run: {
    command: string[];
    "working-directory": string;
    input?: "stdin-json";
    "large-payload-threshold"?: number;
    "timeout-seconds"?: number;
    "max-concurrency"?: number;
    environment?: Record<string, string>;
    sandbox?: "baseline" | "required";
  };
  retry?: { "max-attempts"?: number; "initial-seconds"?: number; "max-seconds"?: number };
}

export interface ActionManagerOptions {
  actionsDirectory: string;
  stateDirectory: string;
  store: LocalStore;
  now?: () => Date;
}

function countRule(rule: ActionRule): number {
  if ("and" in rule) return 1 + rule.and.reduce((sum, child) => sum + countRule(child), 0);
  if ("or" in rule) return 1 + rule.or.reduce((sum, child) => sum + countRule(child), 0);
  if ("not" in rule) return 1 + countRule(rule.not);
  return 1;
}

function safeRegex(pattern: string): RegExp {
  if (Buffer.byteLength(pattern) > 512) throw new Error("action regex exceeds 512 bytes");
  if (/\\[1-9]|\(\?[:=!<]|(?:\*|\+|\{\d+(?:,\d*)?\})(?:\s*\??)(?:\*|\+|\{)|\([^)]*(?:\*|\+|\{\d+(?:,\d*)?\})[^)]*\)(?:\*|\+|\{)/.test(pattern)) {
    throw new Error("action regex uses an unsafe construct");
  }
  return new RegExp(pattern, "u");
}

function validateRule(value: unknown): asserts value is ActionRule {
  if (!isRecord(value)) throw new Error("trigger-rule nodes must be objects");
  const keys = ["and", "or", "not", "match", "prefix", "suffix", "regex"].filter((key) => key in value);
  if (keys.length !== 1) throw new Error("trigger-rule nodes require exactly one operator");
  if ("and" in value || "or" in value) {
    const children = ("and" in value ? value.and : value.or);
    if (!Array.isArray(children) || children.length === 0) throw new Error("action rule groups cannot be empty");
    for (const child of children) validateRule(child);
    return;
  }
  if ("not" in value) return validateRule(value.not);
  const leaf = (value.match ?? value.prefix ?? value.suffix ?? value.regex);
  if (!isRecord(leaf) || typeof leaf.attribute !== "string" || !("value" in leaf)) throw new Error("invalid action matcher");
  if (!/^(id|source|type|subject|time|data\..+)$/.test(leaf.attribute)) throw new Error("invalid action attribute");
  if (("prefix" in value || "suffix" in value || "regex" in value) && typeof leaf.value !== "string") throw new Error("action string matcher requires a string");
  if ("regex" in value) safeRegex(String(leaf.value));
}

export function validateActionDefinition(value: unknown): ActionDefinition {
  if (!isRecord(value) || value.version !== 1 || typeof value.id !== "string" || !/^[a-z][a-z0-9-]{0,63}$/.test(value.id)) {
    throw new Error("invalid action version or id");
  }
  if (typeof value.enabled !== "boolean" || !Array.isArray(value.mirrors) || value.mirrors.length === 0 || !value.mirrors.every((item) => typeof item === "string")) {
    throw new Error("action enabled and mirrors are required");
  }
  validateRule(value["trigger-rule"]);
  if (countRule(value["trigger-rule"]) > 64) throw new Error("action trigger-rule exceeds 64 nodes");
  if (!isRecord(value.run) || !Array.isArray(value.run.command) || value.run.command.length === 0 || !value.run.command.every((item) => typeof item === "string")) {
    throw new Error("action command must be an argv array");
  }
  if (!isAbsolute(value.run.command[0]!) || typeof value.run["working-directory"] !== "string" || !isAbsolute(value.run["working-directory"])) {
    throw new Error("action executable and working-directory must be absolute");
  }
  return value as unknown as ActionDefinition;
}

function attribute(event: StoredEvent, name: ActionAttribute): unknown {
  if (!name.startsWith("data.")) return event[name as keyof StoredEvent];
  let value: unknown = event.data;
  for (const part of name.slice(5).split(".")) {
    if (!isRecord(value) || !(part in value)) return undefined;
    value = value[part];
  }
  return value;
}

export function matchesActionRule(event: StoredEvent, rule: ActionRule): boolean {
  if ("and" in rule) return rule.and.every((child) => matchesActionRule(event, child));
  if ("or" in rule) return rule.or.some((child) => matchesActionRule(event, child));
  if ("not" in rule) return !matchesActionRule(event, rule.not);
  const leaf = "match" in rule ? rule.match : "prefix" in rule ? rule.prefix : "suffix" in rule ? rule.suffix : rule.regex;
  const candidate = attribute(event, leaf.attribute);
  if (candidate === undefined) return false;
  if ("match" in rule) return JSON.stringify(candidate) === JSON.stringify(leaf.value);
  if (typeof candidate !== "string") return false;
  if ("prefix" in rule) return candidate.startsWith(String(leaf.value));
  if ("suffix" in rule) return candidate.endsWith(String(leaf.value));
  const started = performance.now();
  const matched = safeRegex(String(leaf.value)).test(candidate.slice(0, 64 * 1024));
  if (performance.now() - started > 10) throw new Error("action regex evaluation exceeded 10 ms");
  return matched;
}

export class LocalActionManager {
  private readonly now: () => Date;

  constructor(private readonly options: ActionManagerOptions) {
    this.now = options.now ?? (() => new Date());
  }

  async install(value: unknown): Promise<LocalActionRecord> {
    const definition = validateActionDefinition(value);
    await mkdir(this.options.actionsDirectory, { recursive: true, mode: 0o700 });
    await chmod(this.options.actionsDirectory, 0o700);
    const destination = join(this.options.actionsDirectory, `${definition.id}.json`);
    const temporary = `${destination}.tmp`;
    const bytes = `${JSON.stringify(definition, null, 2)}\n`;
    await writeFile(temporary, bytes, { mode: 0o600 });
    await chmod(temporary, 0o600);
    await rename(temporary, destination);
    const record: LocalActionRecord = {
      id: definition.id, definition, enabled: definition.enabled,
      digest: createHash("sha256").update(bytes).digest("hex"), filePath: destination, loadedAt: this.now().toISOString(),
    };
    const existingConsumer = await this.options.store.getConsumer(`action:${definition.id}`);
    if (!existingConsumer) await this.options.store.createConsumer({ id: `action:${definition.id}`, mirrors: definition.mirrors });
    else if (JSON.stringify(existingConsumer.mirrors) !== JSON.stringify([...new Set(definition.mirrors)])) throw new Error("action mirror changes require action recreation");
    await this.options.store.putAction(record);
    return record;
  }

  async reconcileFiles(): Promise<{ loaded: number; failed: number }> {
    let loaded = 0;
    let failed = 0;
    const root = await realpath(this.options.actionsDirectory).catch(() => undefined);
    if (!root) return { loaded, failed };
    for (const record of await this.options.store.listActions()) {
      if (!record.filePath) continue;
      try {
        const path = await realpath(record.filePath);
        if (dirname(path) !== root) throw new Error("registered action escaped the account action directory");
        const details = await lstat(record.filePath);
        if (!details.isFile() || details.isSymbolicLink() || (details.mode & 0o077) !== 0) throw new Error("action file must be a private regular file");
        if (typeof process.getuid === "function" && details.uid !== process.getuid()) throw new Error("action file owner mismatch");
        const bytes = await readFile(record.filePath, "utf8");
        const definition = validateActionDefinition(JSON.parse(bytes));
        if (definition.id !== record.id) throw new Error("action file id changed");
        await this.options.store.putAction({ ...record, definition, enabled: definition.enabled, digest: createHash("sha256").update(bytes).digest("hex"), loadedAt: this.now().toISOString() });
        loaded += 1;
      } catch (error) {
        await this.options.store.putAction({ ...record, enabled: false, loadError: error instanceof Error ? error.message : String(error), loadedAt: this.now().toISOString() });
        failed += 1;
      }
    }
    return { loaded, failed };
  }

  async remove(id: string): Promise<boolean> {
    const record = await this.options.store.getAction(id);
    if (!record) return false;
    if (record.filePath) await rm(record.filePath, { force: true });
    return this.options.store.deleteAction(id);
  }
}

export interface ActionRunnerOptions {
  store: LocalStore;
  stateDirectory: string;
  now?: () => Date;
  random?: () => number;
  sandboxAvailable?: () => boolean;
  faultAfterExit?: () => void;
}

interface ExecutionResult {
  exitStatus?: number;
  signal?: string;
  stdoutTail: string;
  stderrTail: string;
}

export class LocalActionRunner {
  private readonly now: () => Date;
  private readonly random: () => number;
  private active = false;

  constructor(private readonly options: ActionRunnerOptions) {
    this.now = options.now ?? (() => new Date());
    this.random = options.random ?? Math.random;
  }

  async replayDeadLetter(id: string, signal: AbortSignal): Promise<"succeeded" | "failed" | "not_found" | "unavailable"> {
    const deadLetter = await this.options.store.getDeadLetter(id);
    if (!deadLetter) return "not_found";
    if (!deadLetter.envelopeSnapshot) return "unavailable";
    const record = await this.options.store.getAction(deadLetter.actionId);
    if (!record?.enabled || record.loadError) return "unavailable";
    const definition = validateActionDefinition(record.definition);
    const generation = (deadLetter.replayGeneration ?? 0) + 1;
    const now = this.now().toISOString();
    const attempt: LocalActionAttempt = {
      actionId: record.id, localSeq: deadLetter.localSeq, replayGeneration: generation,
      attempt: 1, state: "running", dueAt: now, startedAt: now,
    };
    await this.options.store.putActionAttempt(attempt);
    const consumer = await this.options.store.getConsumer(`action:${record.id}`);
    const recorded = isRecord(deadLetter.summary) && typeof deadLetter.summary.delivery_id === "string"
      ? deadLetter.summary.delivery_id
      : `action:${record.id}:${consumer?.cursorEpoch ?? 0}:${deadLetter.localSeq}`;
    const result = await this.execute(definition, `action:${record.id}`, consumer?.cursorEpoch ?? 0, deadLetter.envelopeSnapshot, 1, signal, recorded);
    const finishedAt = this.now().toISOString();
    const succeeded = result.exitStatus === 0;
    await this.options.store.putActionAttempt({ ...attempt, ...result, state: succeeded ? "succeeded" : "failed", finishedAt });
    if (succeeded) await this.options.store.markDeadLetterReplayed(id, finishedAt);
    return succeeded ? "succeeded" : "failed";
  }

  async runDue(limit: number, signal: AbortSignal): Promise<{ attempted: number; succeeded: number; deadLettered: number }> {
    if (this.active) return { attempted: 0, succeeded: 0, deadLettered: 0 };
    this.active = true;
    try {
      return await this.runDueExclusive(limit, signal);
    } finally {
      this.active = false;
    }
  }

  private async runDueExclusive(limit: number, signal: AbortSignal): Promise<{ attempted: number; succeeded: number; deadLettered: number }> {
    let attempted = 0;
    let succeeded = 0;
    let deadLettered = 0;
    for (const record of await this.options.store.listActions()) {
      if (attempted >= limit || signal.aborted || !record.enabled || record.loadError) continue;
      const definition = validateActionDefinition(record.definition);
      const nowMs = this.now().getTime();
      const recentHour = await this.options.store.listRecentActionAttempts(record.id, new Date(nowMs - 60 * 60_000).toISOString());
      if (recentHour.filter((attempt) => attempt.state === "failed").length >= 60) continue;
      const recentStarts = recentHour.filter((attempt) => Date.parse(attempt.startedAt ?? attempt.dueAt) >= nowMs - 5 * 60_000);
      let consecutiveStartFailures = 0;
      for (const attempt of [...recentStarts].reverse()) {
        if (attempt.signal !== "spawn_error") break;
        consecutiveStartFailures += 1;
      }
      const lastStartFailureAt = recentStarts.at(-1)?.startedAt ?? recentStarts.at(-1)?.dueAt;
      if (consecutiveStartFailures >= 20 && lastStartFailureAt && nowMs < Date.parse(lastStartFailureAt) + 15 * 60_000) continue;
      const consumerId = `action:${record.id}`;
      const payload = await this.options.store.readConsumer(consumerId, 1);
      if ("gap" in payload) {
        await this.options.store.acknowledgeConsumer(consumerId, payload.gap.to, payload.cursor_epoch);
        continue;
      }
      const event = payload.events[0];
      if (!event) continue;
      if (!matchesActionRule(event, definition["trigger-rule"])) {
        await this.options.store.acknowledgeConsumer(consumerId, event.seq, payload.cursor_epoch);
        continue;
      }
      const attempts = await this.options.store.listActionAttempts(record.id, event.seq);
      const latest = attempts.at(-1);
      if (latest?.state === "succeeded" || latest?.state === "dead_lettered") continue;
      if (latest && Date.parse(latest.dueAt) > this.now().getTime()) continue;
      const attemptNumber = (latest?.attempt ?? 0) + 1;
      const replayGeneration = latest?.replayGeneration ?? 0;
      const startedAt = this.now().toISOString();
      const running: LocalActionAttempt = { actionId: record.id, localSeq: event.seq, replayGeneration, attempt: attemptNumber, state: "running", dueAt: startedAt, startedAt };
      await this.options.store.putActionAttempt(running);
      attempted += 1;
      const deliveryId = `${consumerId}:${payload.cursor_epoch}:${event.seq}`;
      const result = await this.execute(definition, consumerId, payload.cursor_epoch, event, attemptNumber, signal, deliveryId);
      this.options.faultAfterExit?.();
      const finishedAt = this.now().toISOString();
      if (result.exitStatus === 0) {
        await this.options.store.putActionAttempt({ ...running, ...result, state: "succeeded", finishedAt });
        await this.options.store.acknowledgeConsumer(consumerId, event.seq, payload.cursor_epoch);
        succeeded += 1;
        continue;
      }
      const maximumAttempts = Math.max(1, Math.min(definition.retry?.["max-attempts"] ?? 5, 60));
      if (attemptNumber >= maximumAttempts) {
        await this.options.store.putActionAttempt({ ...running, ...result, state: "dead_lettered", finishedAt });
        await this.options.store.putDeadLetter({
          id: newUlid(this.now().getTime()), actionId: record.id, localSeq: event.seq, replayGeneration,
          envelopeSnapshot: event, summary: { attempts: attemptNumber, delivery_id: deliveryId, exit_status: result.exitStatus, signal: result.signal, stdout_tail: result.stdoutTail, stderr_tail: result.stderrTail },
          createdAt: finishedAt,
        });
        await this.options.store.acknowledgeConsumer(consumerId, event.seq, payload.cursor_epoch);
        deadLettered += 1;
      } else {
        const initial = Math.max(1, definition.retry?.["initial-seconds"] ?? 5);
        const cap = Math.max(initial, definition.retry?.["max-seconds"] ?? 300);
        const delay = this.random() * Math.min(cap, initial * (2 ** (attemptNumber - 1)));
        await this.options.store.putActionAttempt({ ...running, ...result, state: "failed", finishedAt, dueAt: new Date(this.now().getTime() + delay * 1_000).toISOString() });
      }
    }
    return { attempted, succeeded, deadLettered };
  }

  private async execute(definition: ActionDefinition, consumerId: string, epoch: number, event: StoredEvent, attempt: number, signal: AbortSignal, deliveryId = `${consumerId}:${epoch}:${event.seq}`): Promise<ExecutionResult> {
    if (definition.run.sandbox === "required" && !(this.options.sandboxAvailable?.() ?? false)) {
      return { signal: "sandbox_unavailable", stdoutTail: "", stderrTail: "sandbox required but unavailable" };
    }
    const serialized = JSON.stringify(event);
    const threshold = definition.run["large-payload-threshold"] ?? 256 * 1024;
    const environment: Record<string, string> = {
      BUGLER_ACTION_ID: definition.id,
      BUGLER_CONSUMER_ID: consumerId,
      BUGLER_LOCAL_SEQ: String(event.seq),
      BUGLER_CURSOR_EPOCH: String(epoch),
      BUGLER_EVENT_ID: event.id,
      BUGLER_SOURCE: event.source,
      BUGLER_TYPE: event.type,
      BUGLER_ATTEMPT: String(attempt),
      BUGLER_DELIVERY_ID: deliveryId,
      ...(event.subject !== undefined ? { BUGLER_SUBJECT: event.subject } : {}),
    };
    for (const [name, value] of Object.entries(definition.run.environment ?? {})) {
      if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name) || /^BUGLER_|^REVEILLE_/.test(name)) throw new Error("action environment name is reserved or invalid");
      if (value.startsWith("env:")) {
        const resolved = process.env[value.slice(4)];
        if (resolved === undefined) throw new Error(`action environment reference ${value} is unresolved`);
        environment[name] = resolved;
      } else environment[name] = value;
    }
    let stdin = serialized;
    let temporary: string | undefined;
    if (Buffer.byteLength(serialized) > threshold) {
      const directory = join(this.options.stateDirectory, "action-payloads");
      await mkdir(directory, { recursive: true, mode: 0o700 });
      temporary = join(directory, `${definition.id}-${event.seq}-${newUlid()}.json`);
      await writeFile(temporary, serialized, { mode: 0o600 });
      await chmod(temporary, 0o600);
      environment.BUGLER_EVENT_FILE = temporary;
      stdin = JSON.stringify({ event_file: temporary, bytes: Buffer.byteLength(serialized) });
    }
    try {
      const child = Bun.spawn(definition.run.command, {
        cwd: definition.run["working-directory"], env: environment, stdin: "pipe", stdout: "pipe", stderr: "pipe", detached: true,
      });
      child.stdin.write(stdin);
      child.stdin.end();
      let timeout: ReturnType<typeof setTimeout> | undefined;
      let killTimer: ReturnType<typeof setTimeout> | undefined;
      const timeoutSeconds = Math.max(1, Math.min(definition.run["timeout-seconds"] ?? 600, 86_400));
      const timedOut = new Promise<number>((resolveExit) => {
        timeout = setTimeout(() => {
          this.killProcessGroup(child.pid, "SIGTERM");
          killTimer = setTimeout(() => this.killProcessGroup(child.pid, "SIGKILL"), 5_000);
          resolveExit(-1);
        }, timeoutSeconds * 1_000);
      });
      let abortRun = () => {};
      const aborted = new Promise<number>((resolveExit) => {
        abortRun = () => { this.killProcessGroup(child.pid, "SIGTERM"); resolveExit(-2); };
        signal.addEventListener("abort", abortRun, { once: true });
      });
      const exitStatus = await Promise.race([child.exited, timedOut, aborted]);
      if (timeout) clearTimeout(timeout);
      const [stdout, stderr] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]);
      if (killTimer) clearTimeout(killTimer);
      signal.removeEventListener("abort", abortRun);
      return {
        ...(exitStatus >= 0 ? { exitStatus } : { signal: exitStatus === -1 ? "timeout" : "aborted" }),
        stdoutTail: stdout.slice(-16 * 1024), stderrTail: stderr.slice(-16 * 1024),
      };
    } catch (error) {
      return { signal: "spawn_error", stdoutTail: "", stderrTail: error instanceof Error ? error.message.slice(-16 * 1024) : String(error).slice(-16 * 1024) };
    } finally {
      if (temporary) await rm(temporary, { force: true });
    }
  }

  private killProcessGroup(pid: number, signal: NodeJS.Signals): void {
    try {
      if (process.platform === "win32") process.kill(pid, signal);
      else process.kill(-pid, signal);
    } catch {
      // The process may have exited between the timer and the signal.
    }
  }
}
