package agy

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const testTimeout = 5 * time.Second

type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// testEnv is a bridge pointed entirely at throwaway directories.
type testEnv struct {
	workDir          string
	conversationsDir string
	stateDir         string
	logDir           string
	logs             *syncBuffer
}

func newTestEnv(t *testing.T) testEnv {
	t.Helper()
	root := t.TempDir()
	env := testEnv{
		workDir:          filepath.Join(root, "work"),
		conversationsDir: filepath.Join(root, "conversations"),
		stateDir:         filepath.Join(root, "state"),
		logDir:           filepath.Join(root, "log"),
		logs:             &syncBuffer{},
	}
	for _, dir := range []string{env.workDir, env.conversationsDir, env.stateDir, env.logDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatalf("creating %s: %v", dir, err)
		}
	}
	return env
}

func (e testEnv) config() Config {
	return Config{
		AgyBin:           "/nonexistent/agy-under-test",
		WorkingDir:       e.workDir,
		ConversationsDir: e.conversationsDir,
		StateDir:         e.stateDir,
		LogDir:           e.logDir,
		PollInterval:     2 * time.Millisecond,
		PrintTimeout:     "20m",
		Version:          "test",
		Stderr:           e.logs,
	}
}

// scriptFunc is the body of a fake agy run. It receives the argv the bridge built,
// so a script can behave differently for a first turn and a bound conversation.
type scriptFunc func(ctx context.Context, argv []string) error

// scriptRunner substitutes for spawning agy. Each queued script runs inside
// Wait, so it can grow the conversation database while a turn is in flight and
// observe cancellation the way a real process would.
type scriptRunner struct {
	mu       sync.Mutex
	scripts  []scriptFunc
	argvs    [][]string
	started  []chan struct{}
	startErr error
}

func (s *scriptRunner) add(fn scriptFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scripts = append(s.scripts, fn)
}

// failNextSpawn makes the next spawn fail, standing in for a missing or
// unrunnable agent binary.
func (s *scriptRunner) failNextSpawn(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startErr = err
}

func (s *scriptRunner) starter() agentStarter {
	return func(ctx context.Context, argv []string) (agentProcess, error) {
		s.mu.Lock()
		index := len(s.argvs)
		s.argvs = append(s.argvs, append([]string(nil), argv...))
		started := make(chan struct{})
		s.started = append(s.started, started)
		script := s.scriptFor(index)
		startErr := s.startErr
		s.startErr = nil
		s.mu.Unlock()

		if startErr != nil {
			return nil, startErr
		}
		close(started)
		if script == nil {
			script = func(context.Context, []string) error { return nil }
		}
		return &scriptedAgent{ctx: ctx, argv: argv, script: script}, nil
	}
}

func (s *scriptRunner) scriptFor(index int) scriptFunc {
	if index < len(s.scripts) {
		return s.scripts[index]
	}
	return nil
}

// waitStarted blocks until the nth agy invocation has been spawned, which is how
// a test knows a turn is in flight before it cancels or races it.
func (s *scriptRunner) waitStarted(t *testing.T, index int) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for {
		s.mu.Lock()
		var ch chan struct{}
		if index < len(s.started) {
			ch = s.started[index]
		}
		s.mu.Unlock()
		if ch != nil {
			select {
			case <-ch:
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("agy invocation %d never started within %s", index, testTimeout)
		}
	}
}

func (s *scriptRunner) argvAt(t *testing.T, index int) []string {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if index >= len(s.argvs) {
		t.Fatalf("expected at least %d agy invocations, got %d", index+1, len(s.argvs))
	}
	return append([]string(nil), s.argvs[index]...)
}

func (s *scriptRunner) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.argvs)
}

// scriptedAgent is the fake agy child.
type scriptedAgent struct {
	ctx    context.Context
	argv   []string
	script scriptFunc
	stderr string
}

func (a *scriptedAgent) Wait() error { return a.script(a.ctx, a.argv) }

func (a *scriptedAgent) StderrTail() string { return a.stderr }

// message is any JSON-RPC object the bridge writes back.
type message struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *wireError      `json:"error,omitempty"`
}

// harness drives a Bridge over pipes the way a client would.
type harness struct {
	t             *testing.T
	stdinR        *io.PipeReader
	stdinW        *io.PipeWriter
	outR          *io.PipeReader
	outW          *io.PipeWriter
	lines         chan string
	serveErr      chan error
	nextID        int
	notifications []message
	parked        map[string]message
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{t: t, lines: make(chan string, 512), serveErr: make(chan error, 1), parked: map[string]message{}}
	h.stdinR, h.stdinW = io.Pipe()
	h.outR, h.outW = io.Pipe()

	go func() {
		scanner := bufio.NewScanner(h.outR)
		for scanner.Scan() {
			h.lines <- scanner.Text()
		}
		close(h.lines)
	}()
	t.Cleanup(func() {
		_ = h.stdinW.Close()
		_ = h.outW.Close()
		_ = h.outR.Close()
		_ = h.stdinR.Close()
	})
	return h
}

func (h *harness) serve(bridge *Bridge) {
	h.t.Helper()
	go func() {
		h.serveErr <- bridge.Serve(context.Background(), h.stdinR, h.outW)
	}()
}

func (h *harness) sendLine(line string) {
	h.t.Helper()
	if _, err := io.WriteString(h.stdinW, line+"\n"); err != nil {
		h.t.Fatalf("writing %q to the bridge: %v", line, err)
	}
}

func (h *harness) request(method string, params any) int {
	h.t.Helper()
	h.nextID++
	id := h.nextID
	payload, err := json.Marshal(struct {
		JSONRPC string `json:"jsonrpc"`
		ID      int    `json:"id"`
		Method  string `json:"method"`
		Params  any    `json:"params,omitempty"`
	}{JSONRPC: "2.0", ID: id, Method: method, Params: params})
	if err != nil {
		h.t.Fatalf("encoding %s request: %v", method, err)
	}
	h.sendLine(string(payload))
	return id
}

func (h *harness) nextMessage() (message, bool) {
	h.t.Helper()
	select {
	case line, ok := <-h.lines:
		if !ok {
			return message{}, false
		}
		var msg message
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			h.t.Fatalf("bridge wrote a malformed message %q: %v", line, err)
		}
		return msg, true
	case <-time.After(testTimeout):
		h.t.Fatalf("no message from the bridge within %s", testTimeout)
		return message{}, false
	}
}

// responseFor reads until the response to id arrives, retaining notifications and
// parking any other response, because concurrent requests may be answered in any
// order.
func (h *harness) responseFor(id int) message {
	h.t.Helper()
	want := fmt.Sprintf("%d", id)
	if msg, ok := h.parked[want]; ok {
		delete(h.parked, want)
		return msg
	}
	for {
		msg, ok := h.nextMessage()
		if !ok {
			h.t.Fatalf("stream closed before the response to %d", id)
		}
		if msg.Method != "" {
			h.notifications = append(h.notifications, msg)
			continue
		}
		got := strings.TrimSpace(string(msg.ID))
		if got == want {
			return msg
		}
		h.parked[got] = msg
	}
}

func (h *harness) updates() []Update {
	h.t.Helper()
	var updates []Update
	for _, note := range h.notifications {
		if note.Method != "session/update" {
			h.t.Fatalf("unexpected notification method %q", note.Method)
		}
		var params struct {
			SessionID string `json:"sessionId"`
			Update    Update `json:"update"`
		}
		if err := json.Unmarshal(note.Params, &params); err != nil {
			h.t.Fatalf("malformed session/update params %s: %v", note.Params, err)
		}
		updates = append(updates, params.Update)
	}
	return updates
}

func (h *harness) text() string {
	h.t.Helper()
	var b strings.Builder
	for _, u := range h.updates() {
		if u.Content != nil {
			b.WriteString(u.Content.Text)
		}
	}
	return b.String()
}

func (h *harness) closeStdin() {
	h.t.Helper()
	if err := h.stdinW.Close(); err != nil {
		h.t.Errorf("closing stdin: %v", err)
	}
}

func (h *harness) waitServe() error {
	h.t.Helper()
	select {
	case err := <-h.serveErr:
		return err
	case <-time.After(testTimeout):
		h.t.Fatalf("Serve did not return within %s", testTimeout)
		return nil
	}
}

func mustFail(t *testing.T, msg message, code int) {
	t.Helper()
	if msg.Error == nil {
		t.Fatalf("expected error %d, got result %s", code, msg.Result)
	}
	if msg.Error.Code != code {
		t.Fatalf("error = %d (%s), want %d", msg.Error.Code, msg.Error.Message, code)
	}
}

func mustOK(t *testing.T, msg message) {
	t.Helper()
	if msg.Error != nil {
		t.Fatalf("unexpected error %d: %s", msg.Error.Code, msg.Error.Message)
	}
}

func stopReason(t *testing.T, msg message) string {
	t.Helper()
	mustOK(t, msg)
	var result struct {
		StopReason string `json:"stopReason"`
	}
	if err := json.Unmarshal(msg.Result, &result); err != nil {
		t.Fatalf("malformed prompt result %s: %v", msg.Result, err)
	}
	return result.StopReason
}

// isUUID reports the canonical 8-4-4-4-12 lowercase hex form with a version nibble
// of 4, which is what clients persist and replay.
func isUUID(s string) bool {
	if len(s) != 36 || s[14] != '4' {
		return false
	}
	for i, c := range s {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
			if !isHex {
				return false
			}
		}
	}
	return true
}

func TestInitializeAdvertisesCapabilities(t *testing.T) {
	h := newHarness(t)
	h.serve(NewBridgeWithStarter(newTestEnv(t).config(), (&scriptRunner{}).starter()))

	msg := h.responseFor(h.request("initialize", map[string]any{"protocolVersion": 1}))
	mustOK(t, msg)

	var result struct {
		ProtocolVersion int `json:"protocolVersion"`
		AgentInfo       struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"agentInfo"`
		AgentCapabilities struct {
			Streaming   bool `json:"streaming"`
			LoadSession bool `json:"loadSession"`
		} `json:"agentCapabilities"`
	}
	if err := json.Unmarshal(msg.Result, &result); err != nil {
		t.Fatalf("malformed initialize result %s: %v", msg.Result, err)
	}
	if result.ProtocolVersion != 1 {
		t.Fatalf("protocolVersion = %d, want 1", result.ProtocolVersion)
	}
	if result.AgentInfo.Name != "agy" || result.AgentInfo.Version != "test" {
		t.Fatalf("agentInfo = %+v", result.AgentInfo)
	}
	if !result.AgentCapabilities.Streaming || !result.AgentCapabilities.LoadSession {
		t.Fatalf("agentCapabilities = %+v, want streaming and loadSession", result.AgentCapabilities)
	}
}

func TestSessionNewMintsUUIDAndConfig(t *testing.T) {
	env := newTestEnv(t)
	h := newHarness(t)
	h.serve(NewBridgeWithStarter(env.config(), (&scriptRunner{}).starter()))

	msg := h.responseFor(h.request("session/new", map[string]any{}))
	mustOK(t, msg)

	var result struct {
		SessionID     string            `json:"sessionId"`
		ConfigOptions []json.RawMessage `json:"configOptions"`
	}
	if err := json.Unmarshal(msg.Result, &result); err != nil {
		t.Fatalf("malformed session/new result %s: %v", msg.Result, err)
	}
	if !isUUID(result.SessionID) {
		t.Fatalf("sessionId %q is not a UUID", result.SessionID)
	}
	if len(result.ConfigOptions) != 0 {
		t.Fatalf("configOptions = %s, want the empty list without a working agy", msg.Result)
	}
}

func TestUnknownMethodsAndMalformedLines(t *testing.T) {
	h := newHarness(t)
	h.serve(NewBridgeWithStarter(newTestEnv(t).config(), (&scriptRunner{}).starter()))

	mustFail(t, h.responseFor(h.request("session/list", map[string]any{})), CodeMethodNotFound)
	mustFail(t, h.responseFor(h.request("session/prompt", map[string]any{"sessionId": "missing"})), CodeServerFailure)

	h.sendLine(`{not json`)
	msg, ok := h.nextMessage()
	if !ok {
		t.Fatal("a malformed line was dropped without a report")
	}
	if msg.Error == nil || msg.Error.Code != CodeParseError {
		t.Fatalf("malformed line produced %+v, want a -32700 report", msg.Error)
	}

	// A notification for an unknown method must not be answered at all, but the
	// request before it proves the connection is still alive.
	h.sendLine(`{"jsonrpc":"2.0","method":"session/unknown","params":{}}`)
	id := h.request("initialize", nil)
	mustOK(t, h.responseFor(id))
}

// textBySession returns the text streamed for each session id, which is how a
// test detects output that was attributed to the wrong session.
func (h *harness) textBySession() map[string]string {
	h.t.Helper()
	texts := map[string]string{}
	for _, note := range h.notifications {
		var params struct {
			SessionID string `json:"sessionId"`
			Update    struct {
				Content *Content `json:"content"`
			} `json:"update"`
		}
		if err := json.Unmarshal(note.Params, &params); err != nil {
			h.t.Fatalf("malformed session/update params %s: %v", note.Params, err)
		}
		if params.Update.Content != nil {
			texts[params.SessionID] += params.Update.Content.Text
		}
	}
	return texts
}

// streamInterleaved reports whether notifications from two different sessions were
// written back to back, which is what makes an interleaving bug reachable.
func (h *harness) streamInterleaved() bool {
	h.t.Helper()
	var previous string
	for _, note := range h.notifications {
		var params struct {
			SessionID string `json:"sessionId"`
		}
		if err := json.Unmarshal(note.Params, &params); err != nil {
			h.t.Fatalf("malformed session/update params %s: %v", note.Params, err)
		}
		if previous != "" && params.SessionID != previous {
			return true
		}
		previous = params.SessionID
	}
	return false
}

// conversationFlag reads the --conversation value the bridge passed to agy.
func conversationFlag(argv []string) string {
	for i, arg := range argv {
		if arg == "--conversation" && i+1 < len(argv) {
			return argv[i+1]
		}
	}
	return ""
}
