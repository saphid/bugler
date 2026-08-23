import type { AppendEventInput, MappingConfig, MappingValue, VerificationMethod } from "./domain";
import { isoTimestamp, newUlid } from "./util";

export interface MappingInput {
  sourceId: string;
  mapping: MappingConfig;
  headers: Headers;
  rawBody: Uint8Array;
  contentType?: string;
  verificationMethod: VerificationMethod;
  verificationKeyId?: string;
  now?: Date;
  id?: string;
}

export function jsonPointer(value: unknown, pointer: string): unknown {
  if (pointer === "") return value;
  if (!pointer.startsWith("/")) return undefined;
  let current = value;
  for (const encoded of pointer.slice(1).split("/")) {
    const part = encoded.replace(/~1/g, "/").replace(/~0/g, "~");
    if (Array.isArray(current)) {
      const index = Number(part);
      if (!Number.isInteger(index)) return undefined;
      current = current[index];
    } else if (typeof current === "object" && current !== null) {
      current = (current as Record<string, unknown>)[part];
    } else return undefined;
    if (current === undefined) return undefined;
  }
  return current;
}

function asMappedString(value: unknown): string | undefined {
  if (value === undefined || value === null) return undefined;
  return typeof value === "string" ? value : String(value);
}

function resolveMapping(mapping: MappingValue | undefined, headers: Headers, json: unknown): string | undefined {
  if (!mapping) return undefined;
  if ("header" in mapping) return headers.get(mapping.header) ?? undefined;
  if ("json_pointer" in mapping) return asMappedString(jsonPointer(json, mapping.json_pointer));
  if ("static" in mapping) return mapping.static;
  return mapping.template.replace(/\$\{(header|json_pointer):([^}]+)\}|\{\{(header|json_pointer):([^}]+)\}\}/g, (_match, kindA, valueA, kindB, valueB) => {
    const kind = kindA ?? kindB;
    const value = valueA ?? valueB;
    return kind === "header" ? headers.get(value) ?? "" : asMappedString(jsonPointer(json, value)) ?? "";
  });
}

function githubSubject(eventName: string, payload: unknown): string | undefined {
  const root = typeof payload === "object" && payload !== null ? payload as Record<string, unknown> : {};
  const pullRequest = root.pull_request as Record<string, unknown> | undefined;
  const number = pullRequest?.number ?? root.number;
  if (eventName.startsWith("pull_request") && number !== undefined) return `pr/${String(number)}`;
  if (eventName === "check_run" || eventName === "check_suite") {
    const object = root[eventName] as Record<string, unknown> | undefined;
    const prs = object?.pull_requests;
    const pr = Array.isArray(prs) ? prs[0] as Record<string, unknown> | undefined : undefined;
    if (pr?.number !== undefined) return `pr/${String(pr.number)}`;
  }
  if ((eventName === "issues" || eventName === "issue_comment") && root.issue && typeof root.issue === "object") {
    const issueNumber = (root.issue as Record<string, unknown>).number;
    if (issueNumber !== undefined) return `issue/${String(issueNumber)}`;
  }
  if (eventName === "workflow_run" && root.workflow_run && typeof root.workflow_run === "object") {
    const workflowId = (root.workflow_run as Record<string, unknown>).id;
    if (workflowId !== undefined) return `workflow-run/${String(workflowId)}`;
  }
  return undefined;
}

function githubTime(payload: unknown): string | undefined {
  if (typeof payload !== "object" || payload === null) return undefined;
  const root = payload as Record<string, unknown>;
  for (const key of ["review", "check_run", "check_suite", "pull_request", "workflow_run", "issue"]) {
    const object = root[key];
    if (typeof object !== "object" || object === null) continue;
    const record = object as Record<string, unknown>;
    for (const field of ["submitted_at", "completed_at", "updated_at", "created_at"]) {
      if (typeof record[field] === "string") return record[field];
    }
  }
  return undefined;
}

export function mapEnvelope(input: MappingInput): AppendEventInput {
  const now = input.now ?? new Date();
  const contentType = input.contentType?.split(";", 1)[0]?.trim().toLowerCase();
  const text = new TextDecoder().decode(input.rawBody);
  let parsed: unknown;
  const isJson = contentType === "application/json" || Boolean(contentType?.endsWith("+json"));
  if (isJson) {
    try { parsed = JSON.parse(text); } catch { parsed = undefined; }
  }
  const github = input.mapping.preset === "github";
  const githubEvent = input.headers.get("x-github-event") ?? "unknown";
  const action = typeof parsed === "object" && parsed !== null && typeof (parsed as Record<string, unknown>).action === "string"
    ? String((parsed as Record<string, unknown>).action)
    : undefined;
  const type = github
    ? `com.github.${githubEvent}${action ? `.${action}` : ""}`
    : resolveMapping(input.mapping.type, input.headers, parsed);
  if (!type) throw new Error("mapping did not produce a type");
  const event: AppendEventInput = {
    specversion: "1.0",
    id: input.id ?? newUlid(now.getTime()),
    sourceId: input.sourceId,
    source: input.mapping.source,
    type,
    receivedat: isoTimestamp(now),
    verificationmethod: input.verificationMethod,
    payloadverified: input.verificationMethod === "hmac-sha256",
    rawSize: input.rawBody.byteLength,
  };
  const subject = github ? githubSubject(githubEvent, parsed) : resolveMapping(input.mapping.subject, input.headers, parsed);
  const dedupekey = github
    ? input.headers.get("x-github-delivery") ? `gh/${input.headers.get("x-github-delivery")}` : undefined
    : resolveMapping(input.mapping.dedupekey, input.headers, parsed);
  const time = github ? githubTime(parsed) : resolveMapping(input.mapping.time, input.headers, parsed);
  if (subject) event.subject = subject;
  if (dedupekey) event.dedupekey = dedupekey;
  if (time) event.time = time;
  if (input.verificationKeyId) event.verificationkeyid = input.verificationKeyId;
  if (input.contentType) event.datacontenttype = input.contentType;
  if (isJson && parsed !== undefined) event.data = parsed;
  else event.data_base64 = Buffer.from(input.rawBody).toString("base64");
  return event;
}
