# wackyacp CLI reference

Every command below is documented against the Go source in cmd/ and internal/. The
CLI also carries its own help — run any surface with `--help` (or bare `--help` at
the top level) for the flag catalog; this file is the human-readable companion with
examples and expected output.

```
wackyacp --help                 # dispatch-mode flag catalog (incl. claude-sdk driver flags)
wackyacp serve --help           # serve mode: stdio, TCP, and --remote dial
wackyacp model --help           # model get|set
wackyagy --help                 # wackyagy bridge flags
```

## wackyacp (bridge dispatch mode)

Serves one agent to wackypub over the D112 protocol on stdin/stdout. Driven by a
REMOTE_MANIFEST line, not by hand. Two runtime drivers:

- `--driver=acp` (default): spawns a harness subprocess and speaks ACP to it.
- `--driver=claude-sdk`: drives the Claude Agent SDK directly (no harness, no
  ACP layer), passing the wackypub-rendered system prompt to Claude.

### Common flags (both drivers)

- `--agent-folder` (required): absolute path to the agent folder holding
  `acp-session.json` / `acp-session.lock`. No CWD fallback.
- `--permission-mode` (default `deny`): answer `session/request_permission`
  with `deny` (auto-deny + warning chunk) or `approve` (auto-grant; trusted
  local harnesses only).

### ACP driver flags (`--driver=acp`)

- `--harness-cmd` (required for acp): harness binary (agy, npx, ...), resolved
  via `LookPath` at startup — fail fast, no retry.
- `--harness-args`: extra harness args, shell-quoted (shlex-split).

Example REMOTE_MANIFEST lines:

```bash
agy:           wackyacp --harness-cmd=agy --harness-args=--prompt --agent-folder=/abs/path/to/agy
claude-review: wackyacp --harness-cmd=npx --harness-args=-y @agentclientprotocol/claude-agent-acp --agent-folder=/abs/path/to/claude-review
```

Per-turn flow: acquire `acp-session.lock`, read/verify the session file, spawn the
harness (process-group isolated), ACP `initialize`, resume/load/new, serve D112 on
stdio until stdin EOF, release the lock, reap the child. Delete `acp-session.json`
to reset an agent.

### Claude SDK driver flags (`--driver=claude-sdk`)

Replaces the harness+ACP layer with a direct `@anthropic-ai/claude-agent-sdk`
worker. Same D112 gRPC surface, same lock discipline (`acp-session.lock` taken
before any session read; the SDK worker starts lazily on the first turn so a
Status-only probe costs no Claude process).

- `--system-prompt-mode` (default `append`): `append` keeps Claude Code's own
  preset and adds the wackypub-rendered prompt (AGENTS.md with `@`-macro
  expansion, AUTOLOADED_SKILLS, PERSISTENT_MEMORY, scratchpad addendum);
  `replace` sends only the wackypub prompt as a custom system prompt.
- `--claude-sdk-dir`: directory containing `@anthropic-ai/claude-agent-sdk`
  (default: the pinned install; env `WACKYACP_CLAUDE_SDK_DIR` overrides).
- `--node-bin` (default `node`): node executable that runs the SDK worker.
- `--additional-directories`: comma-separated dirs the SDK may work in outside
  the agent folder, replacing per-edit permission prompts.
- `--claude-dir`: Claude configuration directory probed for a stale OAuth
  refresh lock (default: resolve as the ACP path does).
- `--model`: model the SDK runs for every turn (default: Claude's own
  resolution).

Example REMOTE_MANIFEST line:

```bash
claude: wackyacp --driver=claude-sdk --agent-folder=/abs/path/to/claude \
        --system-prompt-mode=append --node-bin=/usr/local/bin/node
```

The bridge persists the session id into `acp-session.json` when the first turn
creates it, so the next bridge can resume the same Claude session.

## wackyacp serve (inverted ACP server)

Serves an agent as an in-process ACP server, backed by a wackypub child (local)
or a remote wackypub tcp-serve endpoint. ACP JSON-RPC 2.0 on stdin (stdio mode)
or a socket (TCP mode).

```bash
# stdio mode: serve an agent folder over ACP on stdin/stdout
wackyacp serve --agent-folder=/abs/path/to/agy
# TCP mode: ACP on a listener (ACP sessions have NO auth - loopback only!)
wackyacp serve --agent-folder=/abs/path/to/agy --host=127.0.0.1 --port=10023
# remote mode: dial a wackypub tcp-serve endpoint by agent id
wackyacp serve --remote=127.0.0.1:8423 --agent-id=agy
```

Flags:

- `--agent-folder` (required in local mode; absolute; no CWD fallback).
- `--wackypub-bin` (default `wackypub` on PATH): wackypub binary backing a
  local session.
- `--host` (default `127.0.0.1`): TCP bind host. Binding a non-loopback host
  prints a loud warning that ACP has no authentication.
- `--port`: TCP port; `0` or omitted selects stdio mode.
- `--remote host:port`: dial the wackypub AgentService over TCP; requires
  `--agent-id`, mutually exclusive with `--agent-folder` (the folder stays on
  the remote host).
- `--token`: bearer token for the remote endpoint (env `WACKYPUB_SERVE_TOKEN`).

Behavioral contracts:

- **Shutdown**: stdio mode exits on stdin EOF (clean), reading stdin on a
  scanner goroutine so shutdown never hangs; TCP mode closes the listener and
  active connections on SIGTERM so Accept/Read wake immediately and the child
  is reaped rather than orphaned.
- **Cancel lifecycle**: `session/cancel` is a notification (no response per
  spec); idempotent — cancelling with no turn in flight is a no-op, cancelling
  an in-flight turn cancels exactly that turn, and the stream ends with the
  cancellation rather than spinning. The cancel is process-wide: any connection
  may cancel the agent's in-flight turn.

## wackyacp model get|set

Per-agent model inspection/selection (ACP config-option passthrough).

```bash
wackyacp model get --agent-folder=/abs/path/to/agy
wackyacp model set --agent-folder=/abs/path/to/agy deepseek-v4-pro
```

- `--agent-folder` is canonical; `--workspace` / `-w` are deprecated aliases.
  Without it the current directory is used and made absolute.
- `--harness-cmd`, `--harness-args`, `--permission-mode` are accepted and
  passed through, matching dispatch.
- Output is one JSON line on stdout (the machine contract): `get` returns the
  current model plus config options; `set` returns the confirmed model plus
  options. Errors go to stderr AND a `{"error": ...}` line on stdout with a
  non-zero exit; harness config-option rejections carry the JSON-RPC code
  (`{"error": ..., "code": -32602}`).
- Takes the per-agent session lock so it cannot interleave with a live turn.

## wackyagy (Antigravity bridge)

Purpose-built bridge exposing Google's Antigravity CLI (`agy`) as an ACP agent
over newline-delimited JSON-RPC 2.0 on stdin/stdout. Polls agy's conversation
store for live deltas; persists bridge state under `~/.wackyacp/agy-acp`.

```bash
wackyagy --agy-bin=/usr/local/bin/agy --workdir=/abs/path/to/agy
```

Flags and defaults:

- `--agy-bin` (default `/usr/local/bin/agy`, then `agy` on PATH).
- `--workdir`: working directory for agy and the directory shared with it
  (default: process cwd).
- `--conversations-dir` (default `~/.gemini/antigravity-cli/conversations`).
- `--state-dir` (default `~/.wackyacp/agy-acp`).
- `--log-dir` (default `~/.gemini/antigravity-cli/log`).
- `--extra-args`: extra agy arguments, shell-quoted (env `AGY_EXTRA_ARGS`).
- `--print-timeout` (default `20m`): agy `--print-timeout` value.
- `--poll-interval` (default `100ms`): how often to re-read the conversation
  database.
- `--show-narration`: keep agy's leading "I will ..." planning lines in the
  output (env `AGY_SHOW_NARRATION`).
- `--permission-mode` (default `deny`): how tool permission requests are
  handled — `deny` (agy default, headless agy denies) or `approve` (passes
  `agy --dangerously-skip-permissions`).
- `--version`: print version and exit.

Diagnostics go to stderr; protocol messages only on stdout.

## Per-session model selection (`/model` slash passthrough)

In dispatch/bridge mode, a user prompt of the form `/model <id>` (or `/model`
alone to query) is intercepted by the bridge's d112 server before it reaches the
harness: the prompt becomes a `session/set_config_option` with
`configId: "model"` on the harness session, and the harness confirmation is
returned as the turn text. The model is scoped to the harness session, not the
agent folder. Serve mode does not expose this: it implements only
initialize/new/load/prompt/cancel/close and answers anything else with
method-not-found.

## See also

- The README (`README.md`) covers architecture, build/install, and the
  behavioral contracts (session locking, permission mode, BACKUP_POLICY).
- Every surface prints its full flag catalog via `--help` — the authoritative
  in-binary source.
