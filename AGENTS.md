# Bugler contributor instructions

## Required checks

Run from the repository root:

```bash
bun install
bun run typecheck
bun run test:core
bun run test:workers
bash scripts/smoke.sh
```

Run `bun run test` for the complete suite. The DynamoDB and Cosmos suites use Docker-backed emulators through `scripts/test-dynamodb.sh` and `scripts/test-cosmos.sh`.

## Storage neutrality

Keep SQL, engine APIs, key layouts, and engine-specific limits inside `src/storage/`. Core code depends only on `StorageDriver`. Any semantic storage change must update the shared conformance suite and pass unchanged against every driver.

## Review standards

- Preserve atomic commit, monotonic sequence, cursor epoch, lease fencing, retention, and commit-before-ACK guarantees.
- Add tests for success, rejection, concurrency, and restart behavior at the public boundary.
- Keep tests offline except for loopback listeners and local emulators.
- Never commit credentials, generated tokens, local databases, or machine-specific paths.
- Run typecheck, relevant focused tests, the complete suite, and smoke before declaring a change complete.

See `README.md`, `AUTH.md`, and `IMPLEMENTATION-NOTES.md` for the public operating and architecture maps.
