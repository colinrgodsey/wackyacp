package agy

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// agentLogLine renders a message the way agy's glog output looks on disk.
func agentLogLine(message string) string {
	return "E0917 08:34:23.910604    84 log.go:398] " + message + "\n"
}

// cfgAgyBin is the executable the test config points at; argv[0] carries it so a
// starter can exec the slice directly.
const cfgAgyBin = "/nonexistent/agy-under-test"

func startBridge(t *testing.T, env testEnv, runner *scriptRunner) *harness {
	t.Helper()
	h := newHarness(t)
	h.serve(NewBridgeWithStarter(env.config(), runner.starter()))
	return h
}

func newSession(t *testing.T, h *harness) string {
	t.Helper()
	msg := h.responseFor(h.request("session/new", map[string]any{}))
	mustOK(t, msg)
	var result struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(msg.Result, &result); err != nil {
		t.Fatalf("malformed session/new result %s: %v", msg.Result, err)
	}
	if result.SessionID == "" {
		t.Fatal("session/new returned no sessionId")
	}
	return result.SessionID
}

func promptID(h *harness, sessionID, text string) int {
	return h.request("session/prompt", map[string]any{
		"sessionId": sessionID,
		"prompt":    []map[string]string{{"type": "text", "text": text}},
	})
}

// writesConversation returns a script that makes agy appear to create a
// conversation with the given rows, which is how the bridge binds a session.
func writesConversation(t *testing.T, env testEnv, name string, rows []row) scriptFunc {
	t.Helper()
	return func(context.Context, []string) error {
		if _, err := os.Stat(filepath.Join(env.conversationsDir, name+".db")); err == nil {
			return nil
		}
		newConversationDB(t, env.conversationsDir, name, rows)
		return nil
	}
}

func TestPromptStreamsTextAndToolThenEndsTurn(t *testing.T) {
	env := newTestEnv(t)
	runner := &scriptRunner{}
	runner.add(writesConversation(t, env, "conv-1", []row{
		{idx: 1, stepType: stepTypeText, payload: textPayload("Hello")},
		{idx: 2, stepType: 5, payload: toolPayload("read_file", `{"path":"/x"}`)},
		{idx: 3, stepType: stepTypeText, payload: textPayload(" world")},
	}))
	h := startBridge(t, env, runner)
	sessionID := newSession(t, h)

	msg := h.responseFor(h.request("session/prompt", map[string]any{
		"sessionId": sessionID,
		"prompt": []map[string]string{
			{"type": "text", "text": "summarize this"},
			{"type": "image", "data": "AAAA"},
		},
	}))
	if got := stopReason(t, msg); got != StopReasonEndTurn {
		t.Fatalf("stopReason = %q, want end_turn", got)
	}
	if got := h.text(); got != "Hello world" {
		t.Fatalf("streamed text = %q, want %q", got, "Hello world")
	}

	var tools []Update
	for _, u := range h.updates() {
		if u.SessionUpdate == updateToolCall || u.SessionUpdate == updateToolDone {
			tools = append(tools, u)
		}
	}
	if len(tools) != 2 {
		t.Fatalf("tool updates = %d, want the call/update pair", len(tools))
	}
	if tools[0].Title != "read_file: /x" || tools[1].Status != "completed" {
		t.Fatalf("tool pair = %+v / %+v", tools[0], tools[1])
	}
	if !strings.Contains(env.logs.String(), `dropping unsupported prompt block of type "image"`) {
		t.Fatalf("dropped prompt block was not reported; logs:\n%s", env.logs.String())
	}
}

func TestPromptArgvGainsConversationAndModel(t *testing.T) {
	env := newTestEnv(t)
	runner := &scriptRunner{}
	runner.add(writesConversation(t, env, "conv-argv", []row{
		{idx: 1, stepType: stepTypeText, payload: textPayload("one")},
	}))
	runner.add(func(context.Context, []string) error { return nil })
	h := startBridge(t, env, runner)
	sessionID := newSession(t, h)

	if got := stopReason(t, h.responseFor(promptID(h, sessionID, "first question"))); got != StopReasonEndTurn {
		t.Fatalf("turn 1 stopReason = %q", got)
	}
	first := strings.Join(runner.argvAt(t, 0), " ")
	want := strings.Join([]string{cfgAgyBin, "--add-dir", env.workDir, "--print-timeout", "20m", "-p", "first question"}, " ")
	if first != want {
		t.Fatalf("turn 1 argv = %s, want %s", first, want)
	}

	if err := os.WriteFile(filepath.Join(env.stateDir, "models_cache.json"), []byte(`["Gemini 3.1 Pro (Low)"]`), 0o600); err != nil {
		t.Fatal(err)
	}
	mustOK(t, h.responseFor(h.request("session/setConfigOption", map[string]string{
		"sessionId": sessionID, "configId": "model", "value": "Gemini 3.1 Pro (Low)",
	})))

	if got := stopReason(t, h.responseFor(promptID(h, sessionID, "follow up"))); got != StopReasonEndTurn {
		t.Fatalf("turn 2 stopReason = %q", got)
	}
	second := strings.Join(runner.argvAt(t, 1), " ")
	if !strings.Contains(second, "--conversation conv-argv") {
		t.Fatalf("turn 2 argv = %s, want the bound conversation re-passed", second)
	}
	if !strings.Contains(second, "--model Gemini 3.1 Pro (Low)") {
		t.Fatalf("turn 2 argv = %s, want the selected model", second)
	}
	if strings.Contains(second, "conv-argv conv-argv") {
		t.Fatalf("turn 2 argv repeated the conversation: %s", second)
	}
}

func TestExtraArgsAndPrintTimeoutArePassedThrough(t *testing.T) {
	env := newTestEnv(t)
	runner := &scriptRunner{}
	runner.add(func(context.Context, []string) error { return nil })
	h := newHarness(t)

	cfg := env.config()
	cfg.ExtraArgs = []string{"--experimental", "--print-timeout=5m"}
	h.serve(NewBridgeWithStarter(cfg, runner.starter()))

	sessionID := newSession(t, h)
	mustOK(t, h.responseFor(promptID(h, sessionID, "hi")))

	argv := strings.Join(runner.argvAt(t, 0), " ")
	if !strings.Contains(argv, "--experimental") {
		t.Fatalf("argv = %s, want the extra argument", argv)
	}
	if strings.Count(argv, "--print-timeout") != 1 {
		t.Fatalf("argv = %s, want exactly one print timeout", argv)
	}
}

func TestPromptPersistsBinding(t *testing.T) {
	env := newTestEnv(t)
	runner := &scriptRunner{}
	runner.add(writesConversation(t, env, "conv-persist", []row{
		{idx: 4, stepType: stepTypeText, payload: textPayload("answer")},
	}))
	h := startBridge(t, env, runner)
	sessionID := newSession(t, h)
	mustOK(t, h.responseFor(promptID(h, sessionID, "question")))

	sessions, err := NewStore(env.stateDir).Read()
	if err != nil {
		t.Fatalf("reading the store the bridge wrote: %v", err)
	}
	sess, ok := sessions[sessionID]
	if !ok {
		t.Fatalf("session %s was not persisted: %v", sessionID, sessions)
	}
	if sess.ConversationID != "conv-persist" || sess.LastStepIdx != 4 {
		t.Fatalf("persisted session = %+v, want conv-persist at step 4", sess)
	}
}

func TestConcurrentPromptOnSameSessionIsRejected(t *testing.T) {
	env := newTestEnv(t)
	runner := &scriptRunner{}
	done := make(chan struct{})
	runner.add(func(ctx context.Context, _ []string) error {
		<-ctx.Done()
		close(done)
		return ctx.Err()
	})
	h := startBridge(t, env, runner)
	sessionID := newSession(t, h)

	first := promptID(h, sessionID, "long turn")
	runner.waitStarted(t, 0)

	msg := h.responseFor(promptID(h, sessionID, "racing turn"))
	if msg.Error == nil || msg.Error.Code != CodeServerFailure {
		t.Fatalf("racing prompt = %+v, want a -32000 rejection", msg.Error)
	}
	if !strings.Contains(msg.Error.Message, "already in progress") {
		t.Fatalf("rejection message = %q", msg.Error.Message)
	}

	cancel := h.request("session/cancel", map[string]string{"sessionId": sessionID})
	mustOK(t, h.responseFor(cancel))
	if got := stopReason(t, h.responseFor(first)); got != StopReasonCancelled {
		t.Fatalf("stopReason = %q, want cancelled", got)
	}
	<-done
}

func TestCancelStopsTurnAndReportsCancelled(t *testing.T) {
	env := newTestEnv(t)
	runner := &scriptRunner{}
	runner.add(func(ctx context.Context, _ []string) error {
		<-ctx.Done()
		return ctx.Err()
	})
	h := startBridge(t, env, runner)
	sessionID := newSession(t, h)

	id := promptID(h, sessionID, "long turn")
	runner.waitStarted(t, 0)

	// Cancel as a notification, the way ACP clients send it.
	h.sendLine(`{"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":"` + sessionID + `"}}`)

	if got := stopReason(t, h.responseFor(id)); got != StopReasonCancelled {
		t.Fatalf("stopReason = %q, want cancelled", got)
	}
}

func TestCancelWithoutTurnIsAnswered(t *testing.T) {
	env := newTestEnv(t)
	h := startBridge(t, env, &scriptRunner{})

	id := h.request("session/cancel", map[string]string{"sessionId": "nobody-home"})
	mustOK(t, h.responseFor(id))
}

func TestAgentFailureIsReportedWithStderr(t *testing.T) {
	env := newTestEnv(t)
	runner := &scriptRunner{}
	runner.add(func(context.Context, []string) error {
		return errors.New("agy process: exit status 3")
	})
	h := startBridge(t, env, runner)

	sessionID := newSession(t, h)
	msg := h.responseFor(promptID(h, sessionID, "question"))
	if msg.Error == nil || msg.Error.Code != CodeServerFailure {
		t.Fatalf("failed turn = %+v, want a -32000 error", msg.Error)
	}
	if !strings.Contains(msg.Error.Message, "agy failed") {
		t.Fatalf("message = %q, want it to quote the agent failure", msg.Error.Message)
	}
}

func TestSpawnFailureIsReported(t *testing.T) {
	env := newTestEnv(t)
	runner := &scriptRunner{}
	runner.failNextSpawn(errors.New("starting /nonexistent/agy-under-test: no such file or directory"))
	h := startBridge(t, env, runner)

	sessionID := newSession(t, h)
	msg := h.responseFor(promptID(h, sessionID, "question"))
	if msg.Error == nil || msg.Error.Code != CodeServerFailure {
		t.Fatalf("spawn failure = %+v, want a -32000 error", msg.Error)
	}
	if !strings.Contains(msg.Error.Message, "failed to run agy") {
		t.Fatalf("message = %q", msg.Error.Message)
	}
}

func TestSwallowedBackendErrorIsReported(t *testing.T) {
	env := newTestEnv(t)
	runner := &scriptRunner{}
	// agy exits cleanly with no output, having written the real failure only to its
	// own log directory.
	runner.add(func(context.Context, []string) error {
		return os.WriteFile(filepath.Join(env.logDir, "cli-swallowed.log"), []byte(agentLogLine("agent executor error: 429 RESOURCE_EXHAUSTED")), 0o600)
	})
	h := startBridge(t, env, runner)

	sessionID := newSession(t, h)
	msg := h.responseFor(promptID(h, sessionID, "question"))
	if msg.Error == nil {
		t.Fatalf("a swallowed backend failure was reported as success: %s", msg.Result)
	}
	if msg.Error.Code != CodeInternalError {
		t.Fatalf("error code = %d, want -32603", msg.Error.Code)
	}
	if !strings.Contains(msg.Error.Message, "RESOURCE_EXHAUSTED") {
		t.Fatalf("message = %q, want the agent's own error", msg.Error.Message)
	}
}

func TestSetConfigOptionValidation(t *testing.T) {
	env := newTestEnv(t)
	h := startBridge(t, env, &scriptRunner{})
	sessionID := newSession(t, h)

	mustFail(t, h.responseFor(h.request("session/setConfigOption", map[string]string{
		"sessionId": sessionID, "configId": "reasoning", "value": "high",
	})), CodeInvalidParams)

	mustFail(t, h.responseFor(h.request("session/setConfigOption", map[string]string{
		"sessionId": "unknown", "configId": "model", "value": "m",
	})), CodeServerFailure)

	mustFail(t, h.responseFor(h.request("session/setConfigOption", map[string]string{
		"sessionId": sessionID, "configId": "model",
	})), CodeInvalidParams)

	// The snake_case alias the reference bridge also answers must work.
	msg := h.responseFor(h.request("session/set_config_option", map[string]string{
		"sessionId": sessionID, "configId": "model", "value": "Some Model",
	}))
	mustOK(t, msg)

	// The selection must survive into the state the next turn reads.
	sessions, err := NewStore(env.stateDir).Read()
	if err != nil {
		t.Fatal(err)
	}
	if sessions[sessionID].ModelID != "Some Model" {
		t.Fatalf("persisted model = %q, want Some Model", sessions[sessionID].ModelID)
	}
}

func TestSessionLoadAcrossRestart(t *testing.T) {
	env := newTestEnv(t)
	runner := &scriptRunner{}
	runner.add(writesConversation(t, env, "conv-restart", []row{
		{idx: 2, stepType: stepTypeText, payload: textPayload("before restart")},
	}))
	h := startBridge(t, env, runner)
	sessionID := newSession(t, h)
	mustOK(t, h.responseFor(promptID(h, sessionID, "question")))
	h.closeStdin()
	if err := h.waitServe(); err != nil {
		t.Fatalf("Serve: %v", err)
	}

	runner2 := &scriptRunner{}
	h2 := startBridge(t, env, runner2)

	mustFail(t, h2.responseFor(h2.request("session/load", map[string]string{"sessionId": "nope"})), CodeServerFailure)
	mustFail(t, h2.responseFor(h2.request("session/load", map[string]any{})), CodeInvalidParams)

	msg := h2.responseFor(h2.request("session/load", map[string]string{"sessionId": sessionID}))
	mustOK(t, msg)
	var result struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(msg.Result, &result); err != nil || result.SessionID != sessionID {
		t.Fatalf("session/load result = %s", msg.Result)
	}

	// The restored session must continue from its recorded step, not replay history.
	runner2.add(func(context.Context, []string) error { return nil })
	mustOK(t, h2.responseFor(promptID(h2, sessionID, "after restart")))
	argv := strings.Join(runner2.argvAt(t, 0), " ")
	if !strings.Contains(argv, "--conversation conv-restart") {
		t.Fatalf("argv after restart = %s", argv)
	}
	if got := h2.text(); got != "" {
		t.Fatalf("replayed %q, want only new output", got)
	}
}

func TestServeStopsInFlightTurnAtEOF(t *testing.T) {
	env := newTestEnv(t)
	runner := &scriptRunner{}
	observed := make(chan error, 1)
	runner.add(func(ctx context.Context, _ []string) error {
		<-ctx.Done()
		observed <- ctx.Err()
		return ctx.Err()
	})
	h := startBridge(t, env, runner)
	sessionID := newSession(t, h)
	promptID(h, sessionID, "long turn")
	runner.waitStarted(t, 0)

	h.closeStdin()
	if err := h.waitServe(); err != nil {
		t.Fatalf("Serve returned %v, want a clean stop at EOF", err)
	}
	select {
	case err := <-observed:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("agent context ended with %v, want cancellation", err)
		}
	default:
		t.Fatal("Serve returned while the agent child was still running")
	}
}

func TestNarrationFlagControlsPlanningLines(t *testing.T) {
	env := newTestEnv(t)
	runner := &scriptRunner{}
	runner.add(writesConversation(t, env, "conv-narration", []row{
		{idx: 1, stepType: stepTypeText, payload: textPayload("I will open the file")},
	}))

	cfg := env.config()
	cfg.ShowNarration = true
	h := newHarness(t)
	h.serve(NewBridgeWithStarter(cfg, runner.starter()))

	sessionID := newSession(t, h)
	mustOK(t, h.responseFor(promptID(h, sessionID, "question")))
	if got := h.text(); got != "I will open the file" {
		t.Fatalf("streamed %q with narration enabled", got)
	}
}

func TestInMemorySessionBoundDropsOldestIdle(t *testing.T) {
	env := newTestEnv(t)
	bridge := NewBridgeWithStarter(env.config(), (&scriptRunner{}).starter())

	for i := 0; i <= maxInMemorySessions; i++ {
		bridge.setSession("s"+string(rune('a'+i%26))+string(rune('0'+i/26)), StoredSession{LastStepIdx: -1})
	}
	bridge.mu.Lock()
	count := len(bridge.sessions)
	bridge.mu.Unlock()
	if count > maxInMemorySessions {
		t.Fatalf("%d sessions held in memory, want at most %d", count, maxInMemorySessions)
	}

	if _, err := os.Stat(filepath.Join(env.stateDir, "sessions.json")); !os.IsNotExist(err) {
		t.Fatal("an in-memory-only session was written to disk")
	}
}

// TestConcurrentSessionsDoNotInterleaveWrites exercises two sessions streaming at
// the same time: notifications come from each turn's polling goroutine, so
// without serialised writes a client would receive mangled frames and sessions
// could see each other's text.
func TestConcurrentSessionsDoNotInterleaveWrites(t *testing.T) {
	env := newTestEnv(t)
	runner := &scriptRunner{}
	runner.add(writesConversation(t, env, "conv-a", []row{{idx: 1, stepType: stepTypeText, payload: textPayload("a")}}))
	runner.add(writesConversation(t, env, "conv-b", []row{{idx: 1, stepType: stepTypeText, payload: textPayload("b")}}))

	// Both children must be mid-turn at the same moment, otherwise this test would
	// silently measure two sequential turns instead of two concurrent ones.
	var bothRunning sync.WaitGroup
	bothRunning.Add(2)

	grow := func(ctx context.Context, argv []string) error {
		name := conversationFlag(argv)
		if name == "" {
			return errors.New("continuing turn was not given --conversation")
		}
		bothRunning.Done()
		done := make(chan struct{})
		go func() { bothRunning.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(testTimeout):
			return errors.New("the second session never started, so nothing was concurrent")
		case <-ctx.Done():
			return ctx.Err()
		}
		db, err := sql.Open("sqlite", "file:"+filepath.Join(env.conversationsDir, name+".db")+"?_pragma=busy_timeout(10000)")
		if err != nil {
			return fmt.Errorf("opening %s: %w", name, err)
		}
		defer func() { _ = db.Close() }()

		var idx int64
		if err := db.QueryRow("SELECT COALESCE(MAX(idx), 0) FROM steps").Scan(&idx); err != nil {
			return fmt.Errorf("reading steps of %s: %w", name, err)
		}
		for i := 0; i < 12; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			idx++
			if _, err := db.Exec("INSERT INTO steps (idx, step_type, step_payload) VALUES (?, 15, ?)", idx, textPayload(name)); err != nil {
				return fmt.Errorf("inserting into %s: %w", name, err)
			}
			time.Sleep(2 * time.Millisecond)
		}
		return nil
	}
	runner.add(grow)
	runner.add(grow)

	h := startBridge(t, env, runner)
	first := newSession(t, h)
	second := newSession(t, h)

	mustOK(t, h.responseFor(promptID(h, first, "bind a")))
	h.notifications = nil
	mustOK(t, h.responseFor(promptID(h, second, "bind b")))
	h.notifications = nil

	idFirst := promptID(h, first, "stream a")
	idSecond := promptID(h, second, "stream b")

	if got := stopReason(t, h.responseFor(idFirst)); got != StopReasonEndTurn {
		t.Fatalf("first session stopReason = %q", got)
	}
	if got := stopReason(t, h.responseFor(idSecond)); got != StopReasonEndTurn {
		t.Fatalf("second session stopReason = %q", got)
	}

	texts := h.textBySession()
	if len(texts) != 2 {
		t.Fatalf("streamed sessions = %v, want both sessions", texts)
	}
	if !h.streamInterleaved() {
		t.Fatal("the two sessions streamed sequentially, so this test proved nothing about interleaving")
	}
	// The first session bound conv-a and the second conv-b, so each must have
	// streamed its own rows only, in order, with nothing borrowed or lost.
	for session, want := range map[string]string{first: strings.Repeat("conv-a", 12), second: strings.Repeat("conv-b", 12)} {
		if got := texts[session]; got != want {
			t.Fatalf("session %s streamed %q, want exactly its own 12 rows", session, got)
		}
	}
}

// TestRunTurnCancelBeforeSpawnSkipsStarter is the regression guard for the cancel/spawn
// race: if session/cancel lands between turn registration and the starter being reached,
// runTurn must not fork agy just to SIGTERM it. A pre-cancelled child context means the
// starter must never be invoked and the outcome reports cancellation.
func TestRunTurnCancelBeforeSpawnSkipsStarter(t *testing.T) {
	env := newTestEnv(t)

	var mu sync.Mutex
	spawnCount := 0
	starter := func(ctx context.Context, argv []string) (agentProcess, error) {
		mu.Lock()
		spawnCount++
		mu.Unlock()
		fatal := func(context.Context, []string) error { return nil }
		return &scriptedAgent{ctx: ctx, argv: argv, script: fatal}, nil
	}

	b := NewBridgeWithStarter(env.config(), starter)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel lands before the turn goroutine ever reaches the spawn

	turn, ok := b.registerTurn("sess-cancel-before-spawn", cancel)
	if !ok {
		t.Fatal("registerTurn failed")
	}

	sess := StoredSession{ConversationID: "convo-existing", LastStepIdx: 0}
	outcome, err := b.runTurn(ctx, turn, "prompt", sess, func(Update) error { return nil })
	if err != nil {
		t.Fatalf("runTurn returned error for a cancelled-before-spawn turn: %v", err)
	}
	if outcome.StopReason != StopReasonCancelled {
		t.Fatalf("StopReason = %q, want %q", outcome.StopReason, StopReasonCancelled)
	}

	mu.Lock()
	defer mu.Unlock()
	if spawnCount != 0 {
		t.Fatalf("starter invoked %d times for a turn cancelled before spawn, want 0", spawnCount)
	}
}
