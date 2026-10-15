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

`wackyagy` flags: `--agy-bin` (default `/usr/local/bin/agy`, then `agy` on PATH), `--workdir`, `--conversations-dir` (default `~/.gemini/antigravity-cli/conversations`), `--state-dir` (default `~/.wackyacp/agy-acp`), `--log-dir` (default `~/.gemini/antigravity-cli/log`), `--extra-args` (also `AGY_EXTRA_ARGS`), `--print-timeout` (default `20m`), `--poll-interval` (default `100ms`), `--show-narration` (also `AGY_SHOW_NARRATION`), `--permission-mode` (default `deny`; `approve` passes `agy --dangerously-skip-permissions` for trusted harnesses), `--version`. Diagnostics go to stderr; protocol messages only on stdout.

## CLI surface

The binary has no `--help` beyond Go's flag-package usage print at the top level: the
subcommand parsers (`serve`, `model`) reject unknown flags, so `--help` on a subcommand
errors out. Canonical usage is here in the README; a real help verb is a tracked
follow-up, not built here.

### Flags (bridge / dispatch mode)

The flags are the contract between wackypub dispatch and this binary. Remaining flags
not shown in the REMOTE_MANIFEST example above:

- `--permission-mode` (`deny` default, `approve` for trusted local harnesses): how to
  answer `session/request_permission` from the harness. D117 default is `deny`
  (auto-deny with a warning chunk returned over D112) - safe for unattended dispatch
  and the default when the flag is absent from a manifest line. `approve` auto-approves
  every request; that means the harness can run tools and touch files with no prompt,
  so use it only for trusted local harnesses (e.g. a review bot in a sandbox).
  Read by dispatch mode and the model CLI. Serve mode does NOT accept it: the
  serve parser rejects unknown flags, and serve never spawns a harness to ask.

### `wackyacp model get|set`

Per-agent model inspection and selection (ACP config-option passthrough, #22-adjacent).
The model is a session-scoped value the harness resolves; the CLI spawns the harness
to ask, so it needs the same contract flags as dispatch:

```bash
wackyacp model get --agent-folder=/abs/path/to/agy
wackyacp model set --agent-folder=/abs/path/to/agy deepseek-v4-pro
```

- `--agent-folder` is canonical; `--workspace` / `-w` are deprecated aliases for
  scripts. Without it the current directory is used and made absolute.
- `--harness-cmd`, `--harness-args`, `--permission-mode` are accepted and passed
  through, matching dispatch.
- Output is one JSON line on stdout (the machine contract): `get` returns the current
  model plus the harness's config options; `set` returns the confirmed model plus
  options. Errors go to stderr AND a `{"error": ...}` line on stdout with a non-zero
  exit, so scripts can fail on the JSON shape. Harness config-option rejections carry
  the JSON-RPC code (`{"error": ..., "code": -32602}`).
- Takes the per-agent session lock so it cannot interleave with a live turn.

### Per-session model selection (`/model` slash passthrough)

In dispatch/bridge mode, a user prompt of the form `/model <id>` (or `/model` alone to
query) is intercepted by the bridge's d112 server before it reaches the harness: the
prompt becomes a `session/set_config_option` with `configId: "model"` on the harness
session, and the harness's confirmation is returned as the turn text. This is the
per-session model selection surface - the model is scoped to the harness session, not
the agent folder, so another session talking to the same agent keeps its own model
unless it also selects one. Serve mode does NOT expose this: the serve parser implements
only the ACP methods in its capabilities (initialize/new/load/prompt/cancel/close) and
answers anything else with method-not-found.

### `wackyacp serve` (inverted ACP server)

```bash
# stdio mode: serve an agent folder over ACP on stdin/stdout
wackyacp serve --agent-folder=/abs/path/to/agy
# TCP mode: ACP on a listener (ACP sessions have NO auth - loopback only!)
wackyacp serve --agent-folder=/abs/path/to/agy --host=127.0.0.1 --port=10023
```

- `--agent-folder` (required in local mode; absolute path only, no CWD fallback, matching
the dispatch contract). `--wackypub-bin` overrides the wackypub binary used to back the
session (default `wackypub` on PATH).
- `--host` / `--port`: TCP mode. `--host` defaults to `127.0.0.1`; binding a non-loopback
  host prints a loud warning that ACP has no authentication. `--port 0` (or omitted)
  selects stdio mode. `--host` with `--port 0` is rejected as a footgun.
- Reads ACP JSON-RPC 2.0 lines from stdin (stdio mode) or a socket (TCP mode), serves
  `initialize` / `session/new` / `session/load` / `session/prompt` / `session/cancel` /
  `session/close` and shutdown/exit semantics per the ACP spec; the session is backed by
  a wackypub child spawned per server (stdio) or a remote endpoint.
- **Shutdown**: stdio mode exits on stdin EOF (clean), reading stdin on a scanner
goroutine so shutdown never hangs; TCP mode closes the listener and active connections
on SIGTERM so Accept/Read wake immediately and the child is reaped rather than orphaned.
- **Cancel lifecycle** (post-#25): `session/cancel` is a notification (no response, per
  spec); it is idempotent - cancelling with no turn in flight is a no-op, cancelling an
  in-flight turn cancels exactly that turn, and the stream for a cancelled turn ends
  with the cancellation rather than spinning. The cancel is process-wide: any connection
  may cancel the agent's in-flight turn.

### `wackyacp serve --remote` (dial a wackypub tcp-serve endpoint)

Serve-mode without a local agent folder - the conversation lives on a remote or local
wackypub tcp-serve host, identified by agent id. This is the shape for a frontend that
wants ACP locally while wackypub owns the workspace:

```bash
# REMOTE_MANIFEST-style pairing: wackypub tcp-serve on node2 serves agent "agy"
# (wackypub tcp-serve --listen :8423 on node2, WACKYPUB_SERVE_TOKEN set)
wackyacp serve --remote=node2:8423 --agent-id=agy --token=$WACKYPUB_SERVE_TOKEN
#    or a wackypub tcp-serve on the same host without TLS/TCP security:
wackyacp serve --remote=127.0.0.1:8423 --agent-id=agy
```

- `--remote host:port`: dial the wackypub AgentService (D112) over TCP. Requires
  `--agent-id` (the agent the remote tcp-serve hosts); mutually exclusive with
  `--agent-folder` (the folder stays on the remote host).
- `--token` supplies the wackypub tcp-serve bearer token; falls back to
  `WACKYPUB_SERVE_TOKEN` env. The remote endpoint must have the token configured for
  non-loopback binds (tcp-serve auth, wackypub).
- All other serve flags (`--host`, `--port`, `--wackypub-bin`) behave identically;
  `--port 0` is stdio ACP, `--port N` is TCP ACP, and the backend is the remote agent.
- No local workspace is required and no child wackypub is spawned: the server is just
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
