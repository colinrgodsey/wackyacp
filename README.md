# wackyacp

Producer-side ACP bridge binaries: small executables that let [wackypub](https://github.com/colinrgodsey/wackypub) dispatch turns to non-native agent backends. wackypub speaks D112 protobuf to the bridge over stdin/stdout; the bridge speaks [Agent Client Protocol](https://agentclientprotocol.com/) (JSON-RPC 2.0 over stdio) to a harness subprocess. No daemon, no persistent process — spawned per dispatch, dies on stdin EOF.

This is the local-flavor instance of wackypub's bridge abstraction (D116 REMOTE_MANIFEST dispatch): wackypub treats every backend identically — a folder plus a routing entry — and talking to a remote agent is the bridge's job, not wackypub's.

## Binaries

- **`wackyacp`** — generic bridge. Takes any ACP-speaking harness command plus an agent folder, holds `acp-session.json` / `acp-session.lock` there, serves D112 on stdio. (D117.)
- **`wackyagy`** — purpose-built bridge exposing Google's Antigravity CLI (`agy`) as an ACP agent over newline-delimited JSON-RPC 2.0 on stdin/stdout. Polls agy's conversation store for live deltas; persists bridge state under `~/.wackyacp/agy-acp`.

## Architecture

```
wackypub dispatch ──D112 proto / stdio──▶ wackyacp ──ACP / stdio──▶ harness subprocess
                                              │                         (agy, npx …)
                                              ▼
                                    <agent-folder>/
                                      acp-session.json   (sessionId + ownership)
                                      acp-session.lock   (per-agent turn lock)
```

- `internal/acp` — ACP client: `initialize` handshake, capability-driven session establishment (`resume` → `load` → `new`), prompt streaming, permission auto-deny-with-warning (a real deny response back over ACP plus a warning chunk over D112 — pass-through would hang the turn).
- `internal/session` — file-driven session persistence. `acp-session.json` holds `{sessionId, agent_folder, createdAt}` (atomic tmp+rename writes, `0600`); `acp-session.lock` is an exclusive flock taken *before* reading state or spawning the harness and held through the whole turn. Saved sessions carry an ownership assertion: a session recorded for another folder is refused, never reused (cross-agent leak prevention).
- `internal/harness` — harness subprocess lifecycle: `LookPath` resolution at startup (fail fast on misconfiguration), per-turn spawn with process-group isolation (`Setsid`), negative-PID group kill on cancel/EOF so multi-process trees (`npx → node → server`) can't orphan grandchildren.
- `internal/d112` — D112 gRPC service served on stdio. Read-only + turn-generation surface for v1; write methods (`AddMedia`, `StripSignatures`, `CompactSession`) return `codes.Unimplemented` until a real remote-write requirement lands. ACP `usage_update` notifications accumulate into end-of-turn `TurnUsage`.
- `internal/agy` — the wackyagy bridge core (conversation polling, delta streaming, quota/rate-limit sniffing).
- `testdata/acpshimbin/` — scripted ACP-over-stdio fixture server (`resume-ok`, `fail-resume`, `emit-usage`, `hang-on-permission`) so tests never need a real harness.

Explicit non-goals (D117): no CWD-based agent-folder fallback (`--agent-folder` must be absolute), no persistent harness subprocess (no daemon, no TTL — cold resume per turn), no ACP imports in wackypub's module graph, no hand-copied D112 proto (single source via `replace` directive; splitting `agentv1` out has named triggers). Auth for local bridging is a documented non-issue (same user, same machine, pipes aren't network-reachable).

## Build & install

```bash
go build -o bin/wackyacp ./cmd/wackyacp
go build -o bin/wackyagy ./cmd/wackyagy
```

Requires Go 1.25.7+. The `replace` directive in `go.mod` points at a local wackypub checkout for the D112 generated types.

## Usage

`wackyacp` is driven by wackypub dispatch (REMOTE_MANIFEST line), not by hand — but the flags are the contract:

```bash
# REMOTE_MANIFEST lines:
agy:           wackyacp --harness-cmd=agy --harness-args='--prompt' --agent-folder=/abs/path/to/agy
claude-review: wackyacp --harness-cmd=npx --harness-args='-y @agentclientprotocol/claude-agent-acp' --agent-folder=/abs/path/to/claude-review
```

- `--harness-cmd` (required): harness binary, resolved via `LookPath` at startup — fail fast, no retry.
- `--harness-args` (optional): shell-quoted extra args (shlex-split).
- `--agent-folder` (required): absolute path to the agent folder holding `acp-session.json` / `acp-session.lock`. No CWD fallback, by decision.

Per-turn flow: acquire `acp-session.lock` → read/verify session file → spawn harness (process-group isolated) → ACP `initialize` → resume/load/new → serve D112 on stdio until stdin EOF → release lock, reap child. Delete `acp-session.json` to reset an agent, exactly like deleting `session.jsonl` resets a native one.

`wackyagy` flags: `--agy-bin` (default `/usr/local/bin/agy`, then `agy` on PATH), `--workdir`, `--conversations-dir` (default `~/.gemini/antigravity-cli/conversations`), `--state-dir` (default `~/.wackyacp/agy-acp`), `--log-dir` (default `~/.gemini/antigravity-cli/log`), `--extra-args` (also `AGY_EXTRA_ARGS`), `--print-timeout` (default `20m`), `--poll-interval` (default `100ms`), `--show-narration` (also `AGY_SHOW_NARRATION`), `--version`. Diagnostics go to stderr; protocol messages only on stdout.

## Clients

Any ACP client can drive these bridges: [acpx](https://github.com/openclaw/acpx), [Zed](https://zed.dev/), [OpenClaw](https://openclaw.ai/), `@agentclientprotocol/claude-agent-acp`. From wackypub's side they are just backends behind a routing entry — consumers never know which backend served a turn.

## Testing

```bash
go test -race ./...
```

Unit tests run against the `acpshimbin` fixture (no network, no API keys). End-to-end runs against a real harness: `@agentclientprotocol/claude-agent-acp` for `wackyacp`, authenticated `agy` for `wackyagy` (initialize → session/new → streaming prompt → multi-turn continuity verified live).

## Related

- D116 — REMOTE_MANIFEST dispatch layer (the consumer side).
- D117 (`wackypub/decisions/architecture/d117-wackyacp-producer-side-acp-bridge-binary`) — this binary's design record.
- D112 — D112 proto schema (the wire format).

## License

MIT
