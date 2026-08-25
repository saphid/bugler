# Daemon MCP compatibility matrix

Protocol validation on 2026-08-25 used the MCP SDK client against the real `bugler mcp --account` child-process transport. It covers initialization, discovery, immediate drain, bounded wait, epoch ACK, cancellation, concurrent calls, restart, invalid arguments, and protocol-only stdout. The test is `tests/daemon-mcp.test.ts` and runs in CI.

| Harness | Version | Discovery | Check | Wait | ACK | Cancel | Restart | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Claude Code | 2.1.228 | pending | pending | pending | pending | pending | pending | pending |
| Codex CLI | 0.149.1 | pending | pending | pending | pending | pending | pending | pending |
| Cursor CLI | 3.15.6 | pending | pending | pending | pending | pending | pending | pending |
| VS Code | 1.133.0 | pending | pending | pending | pending | pending | pending | pending |
| Claude Desktop | untested | pending | pending | pending | pending | pending | pending | pending |

The named product-harness matrix remains a manual release gate because exercising model-driven tool calls in those products requires their own authenticated runtime and would violate the repository's offline-test rule. No pending cell is inferred from stdio protocol compatibility. The daemon MCP adapter is implemented, but a release must not claim those harnesses as validated until the matrix is run and its transcripts replace the pending cells.
