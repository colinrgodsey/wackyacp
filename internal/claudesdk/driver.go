package claudesdk

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/colinrgodsey/wackyacp/internal/acp"
	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
)

// Defaults for SDK-direct mode. SDKDir is the pinned install named on the card;
// override with --claude-sdk-dir or WACKYACP_CLAUDE_SDK_DIR when the pnpm
// layout moves.
const (
	DefaultNodeBin = "node"
	DefaultSDKDir  = "/home/dranbo/node_modules/.pnpm/node_modules/@anthropic-ai/claude-agent-sdk"
	sdkDirEnvVar   = "WACKYACP_CLAUDE_SDK_DIR"
	workerFileName = "claude-sdk-worker.mjs"
	// workerFileExt must be the last element of a materialized file name:
	// node derives the module format from the suffix.
	workerFileExt = ".mjs"

	// defaultPermissionMode removes the ACP session/request_permission round
	// trip. The bridge is a trusted local process and wackypub already decides
	// which agents get an SDK-direct manifest line.
	defaultPermissionMode = "bypassPermissions"

	readyTimeout = 30 * time.Second
	closeGrace   = 2 * time.Second
)

// Config configures a Driver.
type Config struct {
	// AgentFolder is the absolute wackypub agent directory. It doubles as the
	// SDK cwd, which is what makes .claude/skills discovery work.
	AgentFolder string
	// SystemPromptMode chooses append vs replace.
	SystemPromptMode SystemPromptMode
	// SessionID is a persisted session to resume. Empty means the first turn
	// creates a new one.
	SessionID string
	NodeBin   string
	SDKDir    string
	// AdditionalDirectories grants the SDK access outside the agent folder,
	// replacing the per-edit permission prompts the ACP adapter generated.
	AdditionalDirectories []string
	PermissionMode        string
	// Model is handed to the SDK per query. Empty means let Claude resolve it, which
	// is the behavior every bridge that sets nothing relies on.
	Model string
	// OnSessionEstablished fires with the session id after the first successful
	// turn, so the bridge can persist an id it created itself.
	OnSessionEstablished func(sessionID string)
	// Spawn overrides worker creation. Tests inject a canned worker so the frame
	// protocol runs without node installed.
	// BridgeCtx bounds the worker process for the life of the bridge. It must
	// outlive any single turn, because D112 hands Prompt the gRPC stream context
	// and that context ends when the turn does. Nil means context.Background(),
	// which leaves Close as the only shutdown path.
	BridgeCtx context.Context
	Spawn     func(ctx context.Context) (*exec.Cmd, error)
}

// Driver implements d112.ACPDriver against the Claude Agent SDK. One driver
// owns one worker process for the life of the bridge.
type Driver struct {
	cfg       Config
	persisted bool

	mu         sync.Mutex
	spawnMu    sync.Mutex
	cmd        *exec.Cmd
	workerCtx  context.Context
	workerDone chan struct{}
	generation int64
	stdin      io.WriteCloser
	enc        *json.Encoder
	pending    map[int64]chan *workerEvent
	callbacks  map[int64]*acp.TurnCallbacks
	ready      chan struct{}
	readyErr   error
	gone       chan struct{}

	idCounter atomic.Int64

	sessionID     string
	hasSession    bool
	prevPromptSHA string
	seq           int64
}

// New validates cfg and returns a driver whose worker starts lazily on first use.
func New(cfg Config) (*Driver, error) {
	if cfg.AgentFolder == "" || !filepath.IsAbs(cfg.AgentFolder) {
		return nil, fmt.Errorf("claudesdk: AgentFolder must be an absolute path, got %q", cfg.AgentFolder)
	}
	if cfg.NodeBin == "" {
		cfg.NodeBin = DefaultNodeBin
	}
	if cfg.SDKDir == "" {
		if env := os.Getenv(sdkDirEnvVar); env != "" {
			cfg.SDKDir = env
		} else {
			cfg.SDKDir = DefaultSDKDir
		}
	}
	if cfg.PermissionMode == "" {
		cfg.PermissionMode = defaultPermissionMode
	}
	if cfg.SystemPromptMode == "" {
		cfg.SystemPromptMode = ModeAppend
	}
	if cfg.BridgeCtx == nil {
		cfg.BridgeCtx = context.Background()
	}
	return &Driver{
		cfg:           cfg,
		persisted:     cfg.SessionID != "",
		pending:       make(map[int64]chan *workerEvent),
		callbacks:     make(map[int64]*acp.TurnCallbacks),
		ready:         make(chan struct{}),
		gone:          make(chan struct{}),
		sessionID:     cfg.SessionID,
		prevPromptSHA: readPromptSHA(cfg.AgentFolder),
	}, nil
}

// SessionID reports the session the driver is driving. The first turn assigns
// one when the bridge started without a persisted session.
func (d *Driver) SessionID() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.sessionID
}

// LastConfigOptions satisfies d112.ACPDriver: SDK mode advertises no options.
func (d *Driver) LastConfigOptions() json.RawMessage {
	return json.RawMessage(`{"configOptions":[]}`)
}

// SetConfigOption satisfies d112.ACPDriver. Nothing is settable in v1: system
// prompt mode is a launch flag and tool-surface modes are a separate ticket, so
// a request is refused rather than silently ignored.
func (d *Driver) SetConfigOption(ctx context.Context, sessionID, configID, value string) (json.RawMessage, error) {
	return nil, fmt.Errorf("claudesdk: no config options are settable in SDK-direct mode (requested %q=%q)", configID, value)
}

// workerPath materializes the embedded worker script and returns its path. The
// file is content-addressed so concurrent bridges share one copy and an old
// binary never reuses a stale script.
func (d *Driver) workerPath() (string, error) {
	src, err := workerScript()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(os.TempDir(), "wackyacp-claudesdk")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("creating worker dir %s: %w", dir, err)
	}
	// The extension has to come last. Node derives the module format from the
	// file suffix, so a name like claude-sdk-worker.mjs-<tag> dies with
	// ERR_UNKNOWN_FILE_EXTENSION before it can report anything useful. The tag is
	// the content digest: concurrent bridges share one file, and a rebuilt binary
	// gets a different name instead of rewriting a script another bridge is
	// still executing.
	path := filepath.Join(dir, "claude-sdk-worker-"+contentTag(src)+workerFileExt)
	if sameFile(path, src) {
		return path, nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if errors.Is(err, fs.ErrExist) {
		if sameFile(path, src) {
			return path, nil
		}
		return writePrivateWorker(dir, src)
	}
	if err != nil {
		return "", fmt.Errorf("creating worker script %s: %w", path, err)
	}
	if _, err := f.Write([]byte(src)); err != nil {
		_ = f.Close()
		return "", fmt.Errorf("writing worker script %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("closing worker script %s: %w", path, err)
	}
	return path, nil
}

// ensureWorker starts the worker once and waits for its ready frame.
func (d *Driver) ensureWorker(ctx context.Context) error {
	d.spawnMu.Lock()
	defer d.spawnMu.Unlock()

	d.mu.Lock()
	starting := d.cmd == nil
	ready, gone := d.ready, d.gone
	d.mu.Unlock()

	if starting {
		if err := d.startWorker(); err != nil {
			return err
		}
		// Re-read: the spawn installed this worker's own handshake channels.
		d.mu.Lock()
		ready, gone = d.ready, d.gone
		d.mu.Unlock()
	}

	select {
	case <-ready:
		d.mu.Lock()
		err := d.readyErr
		d.mu.Unlock()
		return err
	case <-gone:
		return errors.New("claudesdk: worker exited before becoming ready")
	case <-time.After(readyTimeout):
		return fmt.Errorf("claudesdk: worker did not report ready within %s", readyTimeout)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// workerPID reports the process id of the current worker, or 0 when there is
// none. Lifecycle tests use it to assert one worker serves many turns.
func (d *Driver) workerPID() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cmd == nil || d.cmd.Process == nil {
		return 0
	}
	return d.cmd.Process.Pid
}

// spawnedWorkerContext returns the context the current worker was created
// with. It must be the bridge lifetime context: a worker bound to a turn
// context is killed the moment that turn's RPC completes.
func (d *Driver) spawnedWorkerContext() context.Context {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.workerCtx
}

// doneSignal returns the current worker completion channel, which readLoop
// closes after reaping the process. It is the only join point Close may use,
// because readLoop is the single owner of cmd.Wait.
func (d *Driver) doneSignal() chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.workerDone
}

func (d *Driver) readySignal() chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ready
}

func (d *Driver) goneSignal() chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.gone
}

func (d *Driver) startWorker() error {
	d.mu.Lock()
	d.generation++
	gen := d.generation
	d.ready = make(chan struct{})
	d.gone = make(chan struct{})
	d.readyErr = nil
	d.workerDone = make(chan struct{})
	d.mu.Unlock()

	life := d.cfg.BridgeCtx
	var cmd *exec.Cmd
	if d.cfg.Spawn != nil {
		spawned, err := d.cfg.Spawn(life)
		if err != nil {
			wrapped := fmt.Errorf("claudesdk: spawn: %w", err)
			d.failStart(gen, wrapped)
			return wrapped
		}
		cmd = spawned
	} else {
		path, err := d.workerPath()
		if err != nil {
			d.failStart(gen, err)
			return err
		}
		cmd = exec.CommandContext(life, d.cfg.NodeBin, path)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		wrapped := fmt.Errorf("claudesdk: worker stdin: %w", err)
		d.failStart(gen, wrapped)
		return wrapped
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		wrapped := fmt.Errorf("claudesdk: worker stdout: %w", err)
		d.failStart(gen, wrapped)
		return wrapped
	}
	cmd.Stderr = os.Stderr
	// Append rather than replace: an injected Spawn may already have set its own
	// environment, and a nil cmd.Env means the child should keep the inherited
	// one rather than boot with nothing.
	base := cmd.Env
	if base == nil {
		base = os.Environ()
	}
	cmd.Env = append(base, sdkDirEnvVar+"="+d.cfg.SDKDir)

	if err := cmd.Start(); err != nil {
		wrapped := fmt.Errorf("claudesdk: starting worker: %w", err)
		d.failStart(gen, wrapped)
		return wrapped
	}

	d.mu.Lock()
	d.cmd = cmd
	d.workerCtx = life
	d.stdin = stdin
	d.enc = json.NewEncoder(stdin)
	d.mu.Unlock()

	go d.readLoop(stdout, cmd, gen, d.doneSignal())
	return nil
}

// failStart retires a worker that never came up, so the next turn retries from
// scratch and anyone waiting on this generation learns why instead of blocking
// until the ready timeout.
func (d *Driver) failStart(gen int64, reason error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if gen != d.generation {
		return
	}
	d.cmd = nil
	d.stdin = nil
	d.enc = nil
	d.workerCtx = nil
	if reason != nil && !isClosed(d.ready) {
		d.readyErr = reason
		close(d.ready)
	}
}

// readLoop demultiplexes worker frames. One goroutine per worker; it returns
// when the worker closes stdout.
func (d *Driver) readLoop(stdout io.Reader, cmd *exec.Cmd, gen int64, done chan struct{}) {
	defer func() {
		if done != nil {
			close(done)
		}
	}()
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		if len(line) == 0 {
			continue
		}
		var ev workerEvent
		if err := json.Unmarshal(line, &ev); err != nil {
			fmt.Fprintf(os.Stderr, "wackyacp claudesdk: dropping malformed worker frame: %v\n", err)
			continue
		}
		d.dispatch(&ev)
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "wackyacp claudesdk: worker stdout read error: %v\n", err)
	}
	_ = cmd.Wait()
	d.shutdownWorker(gen)
}

func (d *Driver) dispatch(ev *workerEvent) {
	switch ev.Type {
	case frameReady:
		d.mu.Lock()
		if !isClosed(d.ready) {
			close(d.ready)
		}
		d.mu.Unlock()
	case frameEvent:
		d.deliverEvent(ev)
	case frameResult, frameError:
		d.deliverResult(ev)
	case frameToolRequest:
		// Tool handlers run concurrently so a slow scratchpad read cannot stall
		// the frame stream the turn's text arrives on.
		go d.handleToolRequest(ev)
	default:
		fmt.Fprintf(os.Stderr, "wackyacp claudesdk: unknown worker frame type %q\n", ev.Type)
	}
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// deliverEvent forwards one streamed SDK event to the turn that asked for it.
// Only the read loop calls this, so seq needs no lock.
func (d *Driver) deliverEvent(ev *workerEvent) {
	d.mu.Lock()
	cbs := d.callbacks[ev.ID]
	d.mu.Unlock()
	if cbs == nil {
		return
	}
	switch ev.Kind {
	case kindText:
		if cbs.OnChunk != nil && ev.Text != "" {
			_ = cbs.OnChunk(ev.Text)
		}
	case kindWarning:
		if cbs.OnWarning != nil && ev.Text != "" {
			_ = cbs.OnWarning(ev.Text)
		}
	case kindToolCall:
		d.seq++
		if cbs.OnToolCall != nil {
			_ = cbs.OnToolCall(&agentv1.ToolCall{
				CallId:      orNewCallID(ev.CallID),
				ToolName:    ev.ToolName,
				ArgsSummary: ev.ArgsSummary,
				Seq:         d.seq,
			})
		}
	case kindToolUpdate:
		if cbs.OnToolCallUpdate != nil {
			_ = cbs.OnToolCallUpdate(&agentv1.ToolCallUpdate{
				CallId:   orNewCallID(ev.CallID),
				ToolName: ev.ToolName,
				Status:   mapToolStatus(ev.Status),
			})
		}
	case kindThinking:
		// Thinking has no place on the D112 turn surface. Dropping it beats
		// smuggling it through as text, which would corrupt rendered replies.
	}
}

func orNewCallID(id string) string {
	if id != "" {
		return id
	}
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// mapToolStatus maps SDK statuses onto the ACP canonical set D112 renders.
func mapToolStatus(status string) string {
	switch status {
	case acp.ToolStatusPending, acp.ToolStatusInProgress, acp.ToolStatusCompleted, acp.ToolStatusFailed, acp.ToolStatusDenied:
		return status
	default:
		return acp.ToolStatusInProgress
	}
}

func (d *Driver) deliverResult(ev *workerEvent) {
	d.mu.Lock()
	ch := d.pending[ev.ID]
	delete(d.pending, ev.ID)
	delete(d.callbacks, ev.ID)
	d.mu.Unlock()
	if ch != nil {
		select {
		case ch <- ev:
		default:
		}
	}
}

func (d *Driver) write(req workerRequest) error {
	d.mu.Lock()
	enc := d.enc
	d.mu.Unlock()
	if enc == nil {
		return errors.New("claudesdk: worker is not running")
	}
	if err := enc.Encode(req); err != nil {
		return fmt.Errorf("claudesdk: writing to worker: %w", err)
	}
	return nil
}

// Prompt drives one SDK turn and maps its stream onto wackypub TurnCallbacks.
func (d *Driver) Prompt(ctx context.Context, sessionID, promptText string, callbacks acp.TurnCallbacks) (*acp.PromptResult, error) {
	if err := d.ensureWorker(ctx); err != nil {
		return nil, err
	}
	composed, err := ComposeSystemPromptForAgentFolder(d.cfg.AgentFolder)
	if err != nil {
		return nil, err
	}
	skills, err := SyncOnDemandSkills(d.cfg.AgentFolder)
	if err != nil {
		return nil, err
	}
	for _, w := range skills.Warnings {
		if callbacks.OnWarning != nil {
			_ = callbacks.OnWarning("skill sync: " + w)
		}
	}

	d.mu.Lock()
	if sessionID != "" {
		d.sessionID = sessionID
	}
	if d.sessionID == "" {
		d.sessionID = newSessionID()
	}
	// Resume whenever a transcript is expected to exist already: either one was
	// restored from acp-session.json, or this bridge already completed a turn.
	resume := d.hasSession || d.persisted
	sid := d.sessionID
	d.mu.Unlock()

	result, err := d.runTurn(ctx, sid, resume, promptText, composed, skills.Names, callbacks)
	if errors.Is(err, errResumeFailed) {
		// Claude's own housekeeping can delete or truncate the transcript behind
		// a persisted id. Starting over is recoverable, so retry once rather than
		// failing the caller's turn.
		warning := "session context lost: cannot resume claude session " + sid + ", continuing this turn in a fresh session"
		fmt.Fprintln(os.Stderr, "wackyacp claudesdk:", warning)
		if callbacks.OnWarning != nil {
			_ = callbacks.OnWarning(warning)
		}
		result, err = d.runTurn(ctx, sid, false, promptText, composed, skills.Names, callbacks)
	}
	if err != nil {
		return nil, err
	}

	d.mu.Lock()
	d.hasSession = true
	d.prevPromptSHA = composed.SHA256
	writePromptSHA(d.cfg.AgentFolder, composed.SHA256)
	notify := d.cfg.OnSessionEstablished
	established := d.sessionID
	d.mu.Unlock()
	if notify != nil {
		notify(established)
	}
	return result, nil
}

// errResumeFailed signals that the worker could not resume the requested
// transcript, which Prompt answers with exactly one fresh-session retry.
var errResumeFailed = errors.New("claudesdk: resume failed")

func (d *Driver) runTurn(ctx context.Context, sid string, resume bool, promptText string, composed *ComposedPrompt, skills []string, callbacks acp.TurnCallbacks) (*acp.PromptResult, error) {
	d.mu.Lock()
	id := d.idCounter.Add(1)
	ch := make(chan *workerEvent, 1)
	d.pending[id] = ch
	cbs := callbacks
	d.callbacks[id] = &cbs
	snapshot := composed.ShouldSnapshot(d.prevPromptSHA)
	mode := string(d.cfg.SystemPromptMode)
	d.mu.Unlock()

	req := workerRequest{
		ID:        id,
		Op:        opPrompt,
		SessionID: sid,
		Prompt:    promptText,
		SystemPrompt: &systemPromptSpec{
			Mode:     mode,
			Content:  composed.Content,
			Snapshot: snapshot,
		},
		Skills:                skills,
		Cwd:                   d.cfg.AgentFolder,
		AdditionalDirectories: d.cfg.AdditionalDirectories,
		SettingSources:        []string{"user", "project", "local"},
		PermissionMode:        d.cfg.PermissionMode,
		Model:                 d.cfg.Model,
	}
	if resume {
		req.Resume = true
	}
	if err := d.write(req); err != nil {
		d.forgetRequest(id)
		return nil, err
	}

	for {
		select {
		case ev := <-ch:
			if ev.Type == frameError {
				if ev.Code == codeResumeFailed {
					return nil, errResumeFailed
				}
				return nil, fmt.Errorf("claudesdk: turn failed: %s", ev.Message)
			}
			result := &acp.PromptResult{StopReason: ev.StopReason}
			if ev.Usage != nil {
				result.Usage = acp.UsageMetrics{
					PromptTokens:     ev.Usage.InputTokens,
					CompletionTokens: ev.Usage.OutputTokens,
					TotalTokens:      ev.Usage.TotalTokens,
					Backend:          "claude-agent-sdk",
				}
			}
			if ev.SessionID != "" {
				d.mu.Lock()
				d.sessionID = ev.SessionID
				d.mu.Unlock()
			}
			return result, nil
		case <-ctx.Done():
			d.interrupt(sid)
			d.forgetRequest(id)
			return nil, ctx.Err()
		case <-d.goneSignal():
			d.forgetRequest(id)
			return nil, errors.New("claudesdk: worker exited mid-turn")
		}
	}
}

func (d *Driver) interrupt(sid string) {
	d.mu.Lock()
	running := d.cmd != nil
	d.mu.Unlock()
	if !running {
		return
	}
	_ = d.write(workerRequest{ID: d.idCounter.Add(1), Op: opCancel, SessionID: sid})
}

func (d *Driver) forgetRequest(id int64) {
	d.mu.Lock()
	delete(d.pending, id)
	delete(d.callbacks, id)
	d.mu.Unlock()
}

// Cancel interrupts the in-flight turn without tearing the worker down, so the
// caller can immediately ask another question.
func (d *Driver) Cancel(sessionID string) error {
	d.interrupt(sessionID)
	return nil
}

// Close terminates the worker. Safe to call more than once.
func (d *Driver) Close() error {
	d.mu.Lock()
	cmd := d.cmd
	done := d.workerDone
	d.cmd = nil
	d.stdin = nil
	d.enc = nil
	d.workerDone = nil
	d.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	_ = cmd.Process.Signal(os.Interrupt)
	// readLoop owns cmd.Wait, so Close joins on its completion channel instead of
	// waiting a second time: two Waits on one Cmd race on its fields, and Go 1.25
	// turns that into a panic inside awaitGoroutines.
	if done != nil {
		select {
		case <-done:
		case <-time.After(closeGrace):
			_ = cmd.Process.Kill()
			select {
			case <-done:
			case <-time.After(closeGrace):
			}
		}
	}
	return nil
}

// shutdownWorker marks the worker dead and fails everything still waiting.
func (d *Driver) shutdownWorker(gen int64) {
	d.mu.Lock()
	if gen != d.generation {
		// A superseded worker's read loop must not tear down the current one.
		d.mu.Unlock()
		return
	}
	if !isClosed(d.gone) {
		close(d.gone)
	}
	pending := d.pending
	d.pending = make(map[int64]chan *workerEvent)
	d.callbacks = make(map[int64]*acp.TurnCallbacks)
	d.cmd = nil
	d.workerDone = nil
	d.stdin = nil
	d.enc = nil
	if !isClosed(d.ready) {
		d.readyErr = errors.New("claudesdk: worker exited before ready")
		close(d.ready)
	}
	d.mu.Unlock()
	for _, ch := range pending {
		select {
		case ch <- &workerEvent{Type: frameError, Message: "worker exited"}:
		default:
		}
	}
}

// newSessionID mints an RFC 4122 v4 UUID, the shape the SDK requires for a
// custom sessionId.
func newSessionID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "wackyacp-" + orNewCallID("")
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// sameFile reports whether path already holds exactly src.
func sameFile(path, src string) bool {
	data, err := os.ReadFile(path)
	return err == nil && string(data) == src
}

// writePrivateWorker handles the case where a shared worker file exists with
// different content: write a private copy under a unique name, extension last,
// rather than clobbering a script that may be running right now.
func writePrivateWorker(dir, src string) (string, error) {
	f, err := os.CreateTemp(dir, "claude-sdk-worker-*.mjs")
	if err != nil {
		return "", fmt.Errorf("creating private worker script: %w", err)
	}
	if _, err := f.Write([]byte(src)); err != nil {
		_ = f.Close()
		return "", fmt.Errorf("writing private worker script: %w", err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("closing private worker script: %w", err)
	}
	return f.Name(), nil
}
