import type { EventEnvelope, Filter, FilterAttribute, StringPredicate } from "./domain";
import { isRecord } from "./util";

const ATTRIBUTES = new Set<FilterAttribute>(["source", "type", "subject", "id", "time"]);

export class FilterValidationError extends Error {}

export function validateFilter(value: unknown): asserts value is Filter {
  let nodes = 0;
  const visit = (node: unknown): void => {
    nodes += 1;
    if (nodes > 32) throw new FilterValidationError("filter exceeds the 32-node limit");
    if (!isRecord(node)) throw new FilterValidationError("filter node must be an object");
    const keys = Object.keys(node);
    if (keys.length !== 1) throw new FilterValidationError("filter node must contain exactly one operator");
    const operator = keys[0];
    if (operator === "all" || operator === "any") {
      const children = node[operator];
      if (!Array.isArray(children) || children.length === 0) throw new FilterValidationError(`${operator} requires a non-empty array`);
      children.forEach(visit);
      return;
    }
    if (operator === "not") {
      visit(node.not);
      return;
    }
    if (operator !== "exact" && operator !== "prefix" && operator !== "suffix") throw new FilterValidationError("unknown filter operator");
    const predicate = node[operator];
    if (!isRecord(predicate) || Object.keys(predicate).length === 0) throw new FilterValidationError(`${operator} requires attributes`);
    for (const [attribute, expected] of Object.entries(predicate)) {
      if (!ATTRIBUTES.has(attribute as FilterAttribute) || typeof expected !== "string") throw new FilterValidationError("invalid filter attribute or value");
    }
  };
  visit(value);
}

function predicateMatches(event: EventEnvelope, predicate: StringPredicate, operator: "exact" | "prefix" | "suffix"): boolean {
  return Object.entries(predicate).every(([attribute, expected]) => {
    const actual = event[attribute as FilterAttribute];
    if (typeof actual !== "string" || expected === undefined) return false;
    if (operator === "exact") return actual === expected;
    if (operator === "prefix") return actual.startsWith(expected);
    return actual.endsWith(expected);
  });
}

export function matchesFilter(event: EventEnvelope, filter: Filter): boolean {
  if ("exact" in filter) return predicateMatches(event, filter.exact, "exact");
  if ("prefix" in filter) return predicateMatches(event, filter.prefix, "prefix");
  if ("suffix" in filter) return predicateMatches(event, filter.suffix, "suffix");
  if ("all" in filter) return filter.all.every((child) => matchesFilter(event, child));
  if ("any" in filter) return filter.any.some((child) => matchesFilter(event, child));
  return !matchesFilter(event, filter.not);
}
