import type { EventQuery, Filter, StoredEvent } from "./domain";
import { matchesFilter } from "./filter";
import type { StorageDriver } from "./storage/driver";
import type { EventEntitlement } from "./delivery";

export interface HistoryResult {
  events: StoredEvent[];
  next_since_seq?: number;
  oldest_available_seq: number;
}

export async function queryHistory(driver: StorageDriver, query: EventQuery, subscriberFilter?: Filter, entitlement: EventEntitlement = "deny"): Promise<HistoryResult> {
  if (entitlement === "deny") return { events: [], oldest_available_seq: await driver.oldestAvailableSeq() };
  let filtered: StoredEvent[];
  let scannedThrough: number | undefined;
  let scanBoundReached = false;
  if (!subscriberFilter && entitlement === "allow") {
    filtered = await driver.queryEvents({ ...query, limit: query.limit + 1 });
  } else {
    filtered = [];
    let cursor = query.sinceSeq ?? 0;
    for (let page = 0; page < 100 && filtered.length <= query.limit; page += 1) {
      const rows = await driver.queryEvents({ ...query, sinceSeq: cursor, limit: 1_000 });
      filtered.push(...rows.filter((event) => (!subscriberFilter || matchesFilter(event, subscriberFilter))
        && (entitlement === "allow" || matchesFilter(event, entitlement))));
      if (rows.length < 1_000) break;
      cursor = rows[rows.length - 1]!.seq;
      scannedThrough = cursor;
      if (page === 99) scanBoundReached = true;
    }
  }
  const page = filtered.slice(0, query.limit);
  const result: HistoryResult = {
    events: page,
    oldest_available_seq: await driver.oldestAvailableSeq(),
  };
  if (filtered.length > query.limit && page.length) result.next_since_seq = page[page.length - 1]!.seq;
  else if (scanBoundReached && scannedThrough !== undefined) result.next_since_seq = scannedThrough;
  return result;
}
