# Contributing to Bugler

## Local checks

Bugler requires Bun 1.4.0 or newer.

```bash
bun install
bun run typecheck
bun run test:core
bun run test:workers
bash scripts/smoke.sh
```

`bun run test` also runs the DynamoDB Local and Cosmos emulator suites. Docker must be available. Run an emulator suite directly with:

```bash
bun run test:dynamodb
bun run test:cosmos
```

Deployment shell changes must also pass `shellcheck`:

```bash
shellcheck scripts/*.sh deploy/*/*.sh
```

## Change standards

- Keep all storage behavior behind `StorageDriver`. No SQL or engine assumptions may escape `src/storage/`.
- Extend the shared storage conformance suite for contract changes. Do not weaken a case for one driver.
- Test failure, fencing, concurrency, restart, and commit-order behavior when relevant.
- Use loopback listeners or local emulators. Tests must not call public network targets.
- Keep credentials, token values, databases, logs, and machine-specific paths out of commits.
- Update `README.md` and `IMPLEMENTATION-NOTES.md` when commands, structure, or operating behavior changes.

## Review checklist

A review should assess both the documented contract and the implementation. Report evidence with file and line references, classify user-impacting correctness separately from cleanup, and reproduce concurrency or durability findings where practical.
