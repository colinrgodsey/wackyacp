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

`wackyagy` flags and defaults are documented by `wackyagy --help`. Diagnostics go to stderr; protocol messages only on stdout.

## CLI surface

The CLI carries its own flag documentation: every surface prints its full flag
catalog on request. The README below only points at those and records the
behavioral contracts the help text cannot convey.

```bash
wackyacp --help                 # bridge dispatch mode (harness flags)
wackyacp serve --help           # serve mode: stdio, TCP, and --remote dial
wackyacp model --help           # model get|set
wackyagy --help                 # wackyagy bridge flags
```

### Bridge / dispatch mode

Driven by wackypub dispatch (REMOTE_MANIFEST line), not by hand - the flags are
the contract:

```bash
agy:           wackyacp --harness-cmd=agy --harness-args='--prompt' --agent-folder=/abs/path/to/agy
claude-review: wackyacp --harness-cmd=npx --harness-args='-y @agentclientprotocol/claude-agent-acp' --agent-folder=/abs/path/to/claude-review
```

`--agent-folder` must be absolute (no CWD fallback, by decision). Per-turn flow:
acquire `acp-session.lock` -> read/verify session file -> spawn harness
(process-group isolated) -> ACP `initialize` -> resume/load/new -> serve D112 on
stdio until stdin EOF -> release lock, reap child. Delete `acp-session.json` to
reset an agent. `--permission-mode deny` (D117 default) auto-denies harness
permission requests with a warning chunk; `approve` grants everything, so it is
only for trusted local harnesses. It is read by dispatch mode and the model CLI;
serve mode does not accept it (no harness to ask).

### `wackyacp model get|set`

Per-agent model inspection/selection (ACP config-option passthrough):

```bash
wackyacp model get --agent-folder=/abs/path/to/agy
wackyacp model set --agent-folder=/abs/path/to/agy deepseek-v4-pro
```

- `--agent-folder` is canonical; `--workspace` / `-w` are deprecated aliases.
  Without it the current directory is used and made absolute.
- Output is one JSON line on stdout (the machine contract). Errors go to stderr
  AND a `{"error": ...}` line on stdout with a non-zero exit; harness
  config-option rejections carry the JSON-RPC code (`{"error": ..., "code": -32602}`).
- Takes the per-agent session lock so it cannot interleave with a live turn.

### Per-session model selection (`/model` slash passthrough)

In dispatch/bridge mode, a user prompt of the form `/model <id>` (or `/model`
alone to query) is intercepted by the bridge d112 server before it reaches the
harness: the prompt becomes a `session/set_config_option` with `configId:
"model"` on the harness session, and the harness confirmation is returned as
the turn text. The model is scoped to the harness session, not the agent folder.
Serve mode does not expose this: it implements only initialize/new/load/prompt/
cancel/close and answers anything else with method-not-found.

### `wackyacp serve` (inverted ACP server)

```bash
# stdio mode: serve an agent folder over ACP on stdin/stdout
wackyacp serve --agent-folder=/abs/path/to/agy
# TCP mode: ACP on a listener (ACP sessions have NO auth - loopback only!)
wackyacp serve --agent-folder=/abs/path/to/agy --host=127.0.0.1 --port=10023
```

- Reads ACP JSON-RPC 2.0 lines from stdin (stdio) or a socket (TCP). `--host`
  defaults to 127.0.0.1; binding a non-loopback host prints a loud warning that
  ACP has no authentication. `--port 0` (or omitted) selects stdio mode.
- **Shutdown**: stdio mode exits on stdin EOF (clean), reading stdin on a
  scanner goroutine so shutdown never hangs; TCP mode closes the listener and
  active connections on SIGTERM so Accept/Read wake immediately and the child is
  reaped rather than orphaned.
- **Cancel lifecycle**: `session/cancel` is a notification (no response, per
  spec); it is idempotent - cancelling with no turn in flight is a no-op,
  cancelling an in-flight turn cancels exactly that turn, and the stream for a
  cancelled turn ends rather than spinning. The cancel is process-wide: any
  connection may cancel the agent in-flight turn.

### `wackyacp serve --remote` (dial a wackypub tcp-serve endpoint)

Serve-mode without a local agent folder - the conversation lives on a remote or
local wackypub tcp-serve host, identified by agent id:

```bash
# wackypub tcp-serve on node2 serves agent "agy" (tcp-serve --listen :8423, WACKYPUB_SERVE_TOKEN set)
wackyacp serve --remote=node2:8423 --agent-id=agy --token=$WACKYPUB_SERVE_TOKEN
# or a wackypub tcp-serve on the same host
wackyacp serve --remote=127.0.0.1:8423 --agent-id=agy
```

- `--remote host:port` dials the wackypub AgentService (D112) over TCP; requires
  `--agent-id` and is mutually exclusive with `--agent-folder` (the folder stays
  on the remote host). `--token` falls back to `WACKYPUB_SERVE_TOKEN` env.
- No local workspace is required and no child wackypub is spawned: the server is
  an ACP-to-D112 translator with the session id derived from the agent id.

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
