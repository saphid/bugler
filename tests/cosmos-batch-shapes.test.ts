import { describe, expect, test } from "bun:test";
import {
  classifyCosmosBatchResponse,
  cosmosBatchHttpStatus,
} from "../src/storage/cosmos";

describe("Cosmos transactional batch response shapes", () => {
  test("classifies the emulator-only returned 207 shape from per-operation statuses", () => {
    expect(classifyCosmosBatchResponse({
      code: 207,
      result: [{ statusCode: 412 }, { statusCode: 424 }],
    }, 2)).toEqual({ kind: "operations", codes: [412, 424], topLevelCode: 207 });
  });

  test("preserves the documented real-service first-failure body for per-operation classification", () => {
    const body = JSON.stringify([{ statusCode: 409 }, { statusCode: 424 }]);
    expect(cosmosBatchHttpStatus(409, body)).toBe(207);
    expect(classifyCosmosBatchResponse({ code: 409, body: JSON.parse(body) }, 2)).toEqual({
      kind: "operations",
      codes: [409, 424],
      topLevelCode: 409,
    });
  });

  test("prefers per-operation results and fails retryably on incomplete or unknown shapes", () => {
    expect(classifyCosmosBatchResponse({
      code: 409,
      result: [{ statusCode: 412 }, { statusCode: 424 }],
    }, 2)).toEqual({ kind: "operations", codes: [412, 424], topLevelCode: 409 });
    expect(classifyCosmosBatchResponse({ code: 429 }, 2)).toEqual({ kind: "top_level_failure", code: 429 });
    expect(classifyCosmosBatchResponse({ code: 200 }, 2)).toEqual({ kind: "retryable" });
    expect(classifyCosmosBatchResponse({ code: 409, result: [{ statusCode: 409 }] }, 2)).toEqual({ kind: "retryable" });
    expect(classifyCosmosBatchResponse({ code: 409, result: [{ statusCode: "409" }, { statusCode: 424 }] }, 2)).toEqual({ kind: "retryable" });
    expect(classifyCosmosBatchResponse(new Error("opaque SDK failure"), 2)).toEqual({ kind: "retryable" });
  });
});
