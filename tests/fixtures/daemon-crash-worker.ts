import { SqliteLocalStore } from "../../src/daemon/sqlite-store";
import type { LocalCommitPhase } from "../../src/daemon/types";
import type { StoredEvent } from "../../src/domain";

const [databasePath, phase] = process.argv.slice(2) as [string, LocalCommitPhase];
const setup = new SqliteLocalStore(databasePath);
if (!(await setup.getConsumer("agent"))) {
  await setup.createMirror("hosted");
  await setup.createConsumer({ id: "agent", mirrors: ["hosted"] });
}
setup.close();

const store = new SqliteLocalStore(databasePath, {
  fault(at) {
    if (at === phase) process.kill(process.pid, "SIGKILL");
  },
});
const event: StoredEvent = {
  specversion: "1.0",
  id: "crash-event",
  source: "source/a",
  sourceId: "source/a",
  type: "dev.bugler.test",
  seq: 1,
  receivedat: "2026-08-25T00:00:00.001Z",
  verificationmethod: "none",
  payloadverified: false,
  rawSize: 1,
  data: { value: "x" },
};
await store.commitHostedFrame({ kind: "events", subscriptionId: "hosted", cursorEpoch: 0, events: [event] });
store.close();
