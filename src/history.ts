import type { EventQuery, Filter, StoredEvent } from "./domain";
import { matchesFilter } from "./filter";
import type { StorageDriver } from "./storage/driver";

export interface HistoryResult {
  events: StoredEvent[];
  next_since_seq?: number;
  oldest_available_seq: number;
}

export async function queryHistory(driver: StorageDriver, query: EventQuery, subscriberFilter?: Filter): Promise<HistoryResult> {
  let filtered: StoredEvent[];
  if (!subscriberFilter) {
    filtered = await driver.queryEvents({ ...query, limit: query.limit + 1 });
  } else {
    filtered = [];
    let cursor = query.sinceSeq ?? 0;
    for (let page = 0; page < 100 && filtered.length <= query.limit; page += 1) {
      const rows = await driver.queryEvents({ ...query, sinceSeq: cursor, limit: 1_000 });
      filtered.push(...rows.filter((event) => matchesFilter(event, subscriberFilter)));
      if (rows.length < 1_000) break;
      cursor = rows[rows.length - 1]!.seq;
    }
  }
  const page = filtered.slice(0, query.limit);
  const result: HistoryResult = {
    events: page,
    oldest_available_seq: await driver.oldestAvailableSeq(),
  };
  if (filtered.length > query.limit && page.length) result.next_since_seq = page[page.length - 1]!.seq;
  return result;
}
