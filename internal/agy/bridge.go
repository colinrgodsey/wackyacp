// Package agy implements an ACP (Agent Client Protocol) agent-side bridge over
// Google Antigravity CLI (agy).
//
// agy has no server mode: each prompt is a fresh subprocess and the transcript
// lives in a SQLite database agy writes on disk. The bridge therefore presents
// newline-delimited JSON-RPC 2.0 on stdin/stdout while it forks agy per prompt
// turn and polls that database for streamed progress.
package agy

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// Config configures a Bridge. Every filesystem location is injectable so the
// bridge can be pointed at a fixture instead of a real agy install.
type Config struct {
	// AgyBin is the agy executable.
	AgyBin string
	// WorkingDir is both the child's working directory and the directory passed
	// to agy with --add-dir. agy takes no per-session cwd, so it is fixed per
	// bridge process.
	WorkingDir string
	// ConversationsDir is where agy writes its per-conversation SQLite files.
	ConversationsDir string
	// StateDir is where the bridge persists session bindings.
	StateDir string
	// LogDir is where agy writes cli-*.log, used for swallowed-error detection.
	LogDir string
	// ExtraArgs are additional agy arguments appended to every prompt.
	ExtraArgs []string
	// PrintTimeout is agy's own per-prompt timeout, in agy's notation (e.g. 20m).
	PrintTimeout string
	// PollInterval is how often the conversation database is re-read.
	PollInterval time.Duration
	// ShowNarration keeps agy's internal "I will ..." planning lines in the
	// output instead of dropping them.
	ShowNarration bool
	// Version is reported in agentInfo.
	Version string
	// Stderr receives diagnostics. Protocol messages never use it.
	Stderr io.Writer
}

func (c Config) withDefaults() Config {
	if c.PrintTimeout == "" {
		c.PrintTimeout = defaultPrintTimeout
	}
	if c.PollInterval <= 0 {
		c.PollInterval = defaultPoll
	}
	if c.Version == "" {
		c.Version = "dev"
	}
	return c
}

const maxInMemorySessions = 64

// Bridge serves ACP requests over stdio, backed by agy.
type Bridge struct {
	cfg        Config
	transcript *Transcript
	store      *Store
	models     *ModelProvider
	starter    agentStarter
	logf       func(format string, args ...any)

	writer *lineWriter

	mu       sync.Mutex
	sessions map[string]StoredSession
	order    []string
	turns    map[string]*activeTurn

	handlers sync.WaitGroup
}

// NewBridge returns a Bridge that spawns the real agy binary.
func NewBridge(cfg Config) *Bridge {
	return newBridge(cfg, nil)
}

// NewBridgeWithStarter returns a Bridge whose agent process launcher is
// substituted, for exercising the turn loop without an agent. The real starter is
// covered by the process-stub integration tests.
func NewBridgeWithStarter(cfg Config, starter agentStarter) *Bridge {
	return newBridge(cfg, starter)
}

func newBridge(cfg Config, starter agentStarter) *Bridge {
	cfg = cfg.withDefaults()

	logTarget := cfg.Stderr
	if logTarget == nil {
		logTarget = io.Discard
	}
	logger := newPrefixLogger(logTarget, "[wackyagy] ")

	if starter == nil {
		starter = newProcessStarter(cfg)
	}
	b := &Bridge{
		cfg:        cfg,
		transcript: NewTranscript(cfg.ConversationsDir),
		store:      NewStore(cfg.StateDir),
		models:     NewModelProvider(cfg.AgyBin, cfg.StateDir, logger),
		starter:    starter,
		logf:       logger,
		sessions:   map[string]StoredSession{},
		turns:      map[string]*activeTurn{},
	}
	return b
}

func (b *Bridge) pollInterval() time.Duration {
	if b.cfg.PollInterval <= 0 {
		return defaultPoll
	}
	return b.cfg.PollInterval
}

// Serve reads JSON-RPC messages from in until EOF, dispatching each one, and
// writes every response and notification to out. In-flight turns are cancelled
// when the client goes away, matching the repo-wide rule that stdin EOF
// terminates work rather than orphaning it.
func (b *Bridge) Serve(ctx context.Context, in io.Reader, out io.Writer) error {
	b.writer = newLineWriter(out)

	serveCtx, cancelServe := context.WithCancel(ctx)
	defer func() {
		b.cancelAllTurns()
		cancelServe()
		b.handlers.Wait()
	}()

	scanner := bufio.NewScanner(in)
	buf := make([]byte, 64*1024)
	scanner.Buffer(buf, 10*1024*1024)

	for scanner.Scan() {
		b.handleLine(serveCtx, scanner.Bytes())
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("reading stdin: %w", err)
	}
	return nil
}

func (b *Bridge) handleLine(ctx context.Context, line []byte) {
	if len(strings.TrimSpace(string(line))) == 0 {
		return
	}

	var req request
	if err := json.Unmarshal(line, &req); err != nil {
		// An unparseable line is reported rather than dropped: silence presents a
		// client framing bug as a hung request. The id is unknowable here, so the
		// reply carries a null id as JSON-RPC requires.
		b.reportParseError(fmt.Sprintf("parse error: %v", err))
		return
	}

	isRequest := len(req.ID) > 0 && string(req.ID) != "null"
	if req.Method == "" {
		if isRequest {
			b.respondError(req.ID, CodeInvalidRequest, "missing method")
		}
		return
	}

	switch req.Method {
	case "initialize":
		if isRequest {
			b.handleInitialize(ctx, req)
		}
	case "session/new":
		if isRequest {
			b.handleSessionNew(ctx, req)
		}
	case "session/load":
		if isRequest {
			b.handleSessionLoad(ctx, req)
		}
	case "session/prompt":
		if isRequest {
			b.handleSessionPrompt(ctx, req)
		}
	case "session/cancel":
		b.handleSessionCancel(req, isRequest)
	case "session/setConfigOption", "session/set_config_option":
		if isRequest {
			b.handleSetConfigOption(ctx, req)
		}
	default:
		if isRequest {
			b.respondError(req.ID, CodeMethodNotFound, "method not found: "+req.Method)
		}
	}
}

func (b *Bridge) handleInitialize(_ context.Context, req request) {
	b.respond(req.ID, map[string]any{
		"protocolVersion": 1,
		"agentInfo": map[string]any{
			"name":    "agy",
			"version": b.cfg.Version,
		},
		"agentCapabilities": map[string]any{
			"streaming":   true,
			"loadSession": true,
		},
	})
}

func (b *Bridge) handleSessionNew(ctx context.Context, req request) {
	var params map[string]json.RawMessage
	if len(req.Params) > 0 {
		if err := json.Unmarshal(req.Params, &params); err != nil {
			b.respondError(req.ID, CodeInvalidParams, "params must be an object: "+err.Error())
			return
		}
		// agy takes a single working directory per process, so a per-session cwd
		// cannot be honoured; say so instead of quietly ignoring it.
		if raw, ok := params["cwd"]; ok {
			b.logf("ignoring session/new cwd %s; agy uses --workdir %s", string(raw), b.cfg.WorkingDir)
		}
	}

	sessionID, err := newSessionID()
	if err != nil {
		b.respondError(req.ID, CodeServerFailure, err.Error())
		return
	}
	b.setSession(sessionID, StoredSession{LastStepIdx: -1})
	b.respond(req.ID, map[string]any{
		"sessionId":     sessionID,
		"configOptions": b.models.ConfigOptions(ctx, ""),
	})
}

func (b *Bridge) handleSessionLoad(ctx context.Context, req request) {
	var params struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil || params.SessionID == "" {
		b.respondError(req.ID, CodeInvalidParams, "session/load requires params.sessionId")
		return
	}
	sess, ok := b.resolveSession(params.SessionID)
	if !ok {
		b.respondError(req.ID, CodeServerFailure, fmt.Sprintf("unknown sessionId: %s", params.SessionID))
		return
	}
	b.respond(req.ID, map[string]any{
		"sessionId":     params.SessionID,
		"configOptions": b.models.ConfigOptions(ctx, sess.ModelID),
	})
}

func (b *Bridge) handleSessionPrompt(ctx context.Context, req request) {
	var params struct {
		SessionID string        `json:"sessionId"`
		Prompt    []promptBlock `json:"prompt"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		b.respondError(req.ID, CodeInvalidParams, "session/prompt params: "+err.Error())
		return
	}
	if params.SessionID == "" {
		b.respondError(req.ID, CodeInvalidParams, "session/prompt requires params.sessionId")
		return
	}
	sess, ok := b.resolveSession(params.SessionID)
	if !ok {
		// Answering with an empty turn would hide a lost or mistyped session id, so
		// this is reported rather than answered quietly.
		b.respondError(req.ID, CodeServerFailure, fmt.Sprintf("unknown sessionId: %s", params.SessionID))
		return
	}
	promptText := joinPromptBlocks(b.logf, params.Prompt)

	turnCtx, cancelTurn := context.WithCancel(ctx)
	turn, ok := b.registerTurn(params.SessionID, cancelTurn)
	if !ok {
		cancelTurn()
		b.respondError(req.ID, CodeServerFailure, fmt.Sprintf("turn already in progress for sessionId: %s", params.SessionID))
		return
	}

	b.handlers.Add(1)
	go func() {
		defer b.handlers.Done()
		defer cancelTurn()
		defer b.unregisterTurn(turn)

		emit := func(update Update) error {
			return b.notify("session/update", map[string]any{
				"sessionId": params.SessionID,
				"update":    update,
			})
		}

		outcome, err := b.runTurn(turnCtx, turn, promptText, sess, emit)
		if outcome.ConversationID != "" {
			updated := StoredSession{
				ConversationID: outcome.ConversationID,
				LastStepIdx:    outcome.LastStepIdx,
				ModelID:        sess.ModelID,
			}
			b.setSession(params.SessionID, updated)
			if err := b.store.Write(params.SessionID, updated); err != nil {
				b.logf("persisting session %s: %v", params.SessionID, err)
			}
		}
		if err != nil {
			b.respondError(req.ID, errorCode(err), err.Error())
			return
		}
		b.respond(req.ID, map[string]any{"stopReason": outcome.StopReason})
	}()
}

func (b *Bridge) handleSessionCancel(req request, isRequest bool) {
	var params struct {
		SessionID string `json:"sessionId"`
	}
	// A cancel carries parameters that may be malformed; it is still answered, so
	// a client tearing down a session cannot deadlock on a bad id.
	if err := json.Unmarshal(req.Params, &params); err == nil && params.SessionID != "" {
		if b.cancelTurn(params.SessionID) {
			b.logf("cancelled turn for session %s", params.SessionID)
		} else {
			b.logf("session/cancel for session %s: no turn in progress", params.SessionID)
		}
	}
	if isRequest {
		b.respond(req.ID, map[string]any{})
	}
}

func (b *Bridge) handleSetConfigOption(ctx context.Context, req request) {
	var params struct {
		SessionID string `json:"sessionId"`
		ConfigID  string `json:"configId"`
		Value     string `json:"value"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		b.respondError(req.ID, CodeInvalidParams, "session/setConfigOption params: "+err.Error())
		return
	}
	if params.SessionID == "" || params.ConfigID == "" || params.Value == "" {
		b.respondError(req.ID, CodeInvalidParams, "session/setConfigOption requires sessionId, configId, and value")
		return
	}
	if params.ConfigID != "model" {
		b.respondError(req.ID, CodeInvalidParams, fmt.Sprintf("unsupported configId: %s (only \"model\" is available)", params.ConfigID))
		return
	}
	sess, ok := b.resolveSession(params.SessionID)
	if !ok {
		b.respondError(req.ID, CodeServerFailure, fmt.Sprintf("unknown sessionId: %s", params.SessionID))
		return
	}

	sess.ModelID = b.models.CanonicalID(params.Value)
	b.setSession(params.SessionID, sess)
	if err := b.store.Write(params.SessionID, sess); err != nil {
		b.respondError(req.ID, CodeServerFailure, fmt.Sprintf("persisting session %s: %v", params.SessionID, err))
		return
	}
	b.respond(req.ID, map[string]any{
		"configOptions": b.models.ConfigOptions(ctx, sess.ModelID),
	})
}

// resolveSession returns a session from memory, restoring it from the durable
// store if the bridge has been restarted.
func (b *Bridge) resolveSession(sessionID string) (StoredSession, bool) {
	b.mu.Lock()
	sess, ok := b.sessions[sessionID]
	b.mu.Unlock()
	if ok {
		return sess, true
	}
	stored, err := b.store.Read()
	if err != nil {
		b.logf("reading session store: %v", err)
		return StoredSession{}, false
	}
	sess, ok = stored[sessionID]
	if !ok {
		return StoredSession{}, false
	}
	b.setSession(sessionID, sess)
	return sess, true
}

func (b *Bridge) setSession(sessionID string, sess StoredSession) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.sessions[sessionID]; !exists {
		b.order = append(b.order, sessionID)
	}
	b.sessions[sessionID] = sess
	b.evictLocked()
}

// evictLocked drops the oldest idle session once the in-memory cache exceeds its
// bound. Only sessions without an in-flight turn are eligible, and dropping one
// loses nothing: the durable store restores it on the next request.
func (b *Bridge) evictLocked() {
	if len(b.sessions) <= maxInMemorySessions {
		return
	}
	for i, id := range b.order {
		if _, active := b.turns[id]; active {
			continue
		}
		delete(b.sessions, id)
		b.order = append(b.order[:i], b.order[i+1:]...)
		return
	}
}

func (b *Bridge) registerTurn(sessionID string, cancel context.CancelFunc) (*activeTurn, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, busy := b.turns[sessionID]; busy {
		return nil, false
	}
	turn := &activeTurn{sessionID: sessionID, cancel: cancel}
	b.turns[sessionID] = turn
	return turn, true
}

func (b *Bridge) unregisterTurn(turn *activeTurn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// Only clear the entry if it is still this turn, so a replacement turn that
	// started while this one was finishing is not removed by mistake.
	if current, ok := b.turns[turn.sessionID]; ok && current == turn {
		delete(b.turns, turn.sessionID)
	}
}

// cancelTurn interrupts the in-flight turn of a session, reporting whether one
// was running.
func (b *Bridge) cancelTurn(sessionID string) bool {
	b.mu.Lock()
	turn, ok := b.turns[sessionID]
	b.mu.Unlock()
	if !ok {
		return false
	}
	turn.cancelled.Store(true)
	turn.cancel()
	return true
}

func (b *Bridge) cancelAllTurns() {
	b.mu.Lock()
	turns := make([]*activeTurn, 0, len(b.turns))
	for _, turn := range b.turns {
		turns = append(turns, turn)
	}
	b.mu.Unlock()
	for _, turn := range turns {
		turn.cancel()
	}
}

// respond writes a successful response, or an error response when id is absent
// because nothing can be answered without one.
func (b *Bridge) respond(id json.RawMessage, result any) {
	if len(id) == 0 || string(id) == "null" {
		return
	}
	if err := b.write(response{JSONRPC: "2.0", ID: id, Result: result}); err != nil {
		b.logf("writing response: %v", err)
	}
}

// reportParseError answers a line that could not be parsed as a JSON-RPC object.
func (b *Bridge) reportParseError(message string) {
	if err := b.write(response{JSONRPC: "2.0", ID: json.RawMessage("null"), Error: &wireError{Code: CodeParseError, Message: message}}); err != nil {
		b.logf("writing parse error: %v", err)
	}
}

func (b *Bridge) respondError(id json.RawMessage, code int, message string) {
	if len(id) == 0 || string(id) == "null" {
		b.logf("cannot reply to malformed message (%d): %s", code, message)
		return
	}
	if err := b.write(response{JSONRPC: "2.0", ID: id, Error: &wireError{Code: code, Message: message}}); err != nil {
		b.logf("writing error response: %v", err)
	}
}

func (b *Bridge) notify(method string, params any) error {
	return b.write(notification{JSONRPC: "2.0", Method: method, Params: params})
}

func (b *Bridge) write(message any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.writer == nil {
		return errors.New("bridge is not serving")
	}
	return b.writer.WriteJSON(message)
}

// errorCode maps an internal error onto the JSON-RPC code the client should see,
// defaulting to a generic internal error.
func errorCode(err error) int {
	var te *turnError
	if errors.As(err, &te) {
		return te.Code
	}
	return CodeInternalError
}

// promptBlock is one content block of a session/prompt request.
type promptBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// joinPromptBlocks flattens prompt content into the single string agy accepts.
// Blocks that carry no text (images, resource links) are dropped because agy's
// print mode has no way to receive them, so that loss is logged rather than
// silent.
func joinPromptBlocks(logf func(format string, args ...any), blocks []promptBlock) string {
	parts := make([]string, 0, len(blocks))
	for _, block := range blocks {
		if block.Text == "" {
			if block.Type != "" && block.Type != "text" && logf != nil {
				logf("dropping unsupported prompt block of type %q: agy print mode accepts text only", block.Type)
			}
			continue
		}
		parts = append(parts, block.Text)
	}
	return strings.TrimSpace(strings.Join(parts, "\n"))
}

// newSessionID mints an RFC 4122 version 4 UUID without pulling in a dependency.
func newSessionID() (string, error) {
	var raw [16]byte
	if _, err := io.ReadFull(rand.Reader, raw[:]); err != nil {
		return "", fmt.Errorf("generating session id: %w", err)
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16]), nil
}
