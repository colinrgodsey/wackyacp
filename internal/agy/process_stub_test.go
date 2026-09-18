package agy

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// The stub binary is built once per test binary run, mirroring how the existing
// acpshimbin tests provide a real child process for the spawn paths.
var (
	stubOnce sync.Once
	stubPath string
	stubErr  error
)

func agyStub(t *testing.T) string {
	t.Helper()
	stubOnce.Do(func() {
		dir, err := os.MkdirTemp("", "agystubbin")
		if err != nil {
			stubErr = fmt.Errorf("creating stub output dir: %w", err)
			return
		}
		stubPath = filepath.Join(dir, "agystubbin")
		cmd := exec.Command("go", "build", "-o", stubPath, "../../testdata/agystubbin")
		output, err := cmd.CombinedOutput()
		if err != nil {
			stubErr = fmt.Errorf("building the agy stub: %v: %s", err, output)
		}
	})
	if stubErr != nil {
		t.Fatal(stubErr)
	}
	return stubPath
}

func stubEnv(t *testing.T, scenario string) (testEnv, *harness) {
	t.Helper()
	env := newTestEnv(t)
	cfg := env.config()
	cfg.AgyBin = agyStub(t)
	cfg.PollInterval = 5 * time.Millisecond

	t.Setenv("STUB_SCENARIO", scenario)
	t.Setenv("STUB_CONVERSATIONS_DIR", env.conversationsDir)
	t.Setenv("STUB_LOG_DIR", env.logDir)

	h := newHarness(t)
	h.serve(NewBridge(cfg))
	return env, h
}

func waitForFile(t *testing.T, path string) string {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for {
		data, err := os.ReadFile(path)
		if err == nil {
			return string(data)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never appeared within %s: %v", path, testTimeout, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestStubStreamsThroughARealChildProcess(t *testing.T) {
	env, h := stubEnv(t, "stream")
	defer func() {
		if logs := env.logs.String(); strings.Contains(logs, "STUB-STDOUT-NOISE") {
			t.Fatalf("agent stdout leaked into bridge diagnostics:\n%s", logs)
		}
	}()

	sessionID := newSession(t, h)
	msg := h.responseFor(promptID(h, sessionID, "hello agy"))
	if got := stopReason(t, msg); got != StopReasonEndTurn {
		t.Fatalf("stopReason = %q, want end_turn", got)
	}
	if got := h.text(); got != "Hello world done" {
		t.Fatalf("streamed %q, want %q", got, "Hello world done")
	}

	var calls, completions int
	for _, update := range h.updates() {
		switch update.SessionUpdate {
		case updateToolCall:
			calls++
			if update.Title != "read_file: /x" || string(update.RawInput) != `{"path":"/x"}` {
				t.Fatalf("tool call = %+v", update)
			}
		case updateToolDone:
			completions++
			if update.Status != "completed" {
				t.Fatalf("tool update = %+v, want completed", update)
			}
		}
	}
	if calls != 1 || completions != 1 {
		t.Fatalf("tool pair counts = %d/%d, want 1/1", calls, completions)
	}

	sessions, err := NewStore(env.stateDir).Read()
	if err != nil {
		t.Fatal(err)
	}
	if got := sessions[sessionID].ConversationID; got != "stub-conversation" {
		t.Fatalf("bound conversation %q, want stub-conversation", got)
	}
	if got := sessions[sessionID].LastStepIdx; got != 3 {
		t.Fatalf("LastStepIdx = %d, want 3", got)
	}
}

func TestStubSecondTurnContinuesTheConversation(t *testing.T) {
	_, h := stubEnv(t, "stream")

	sessionID := newSession(t, h)
	mustOK(t, h.responseFor(promptID(h, sessionID, "first")))
	h.notifications = nil

	msg := h.responseFor(promptID(h, sessionID, "second"))
	if got := stopReason(t, msg); got != StopReasonEndTurn {
		t.Fatalf("stopReason = %q", got)
	}
	if got := h.text(); got != "Hello world done" {
		t.Fatalf("turn 2 streamed %q, want only the new rows", got)
	}
}

func TestStubCancelReachesTheProcessGroup(t *testing.T) {
	env, h := stubEnv(t, "sleep")
	killed := filepath.Join(env.workDir, "killed")
	ready := filepath.Join(env.workDir, "ready")
	t.Setenv("STUB_KILLED_FILE", killed)
	t.Setenv("STUB_READY_FILE", ready)

	sessionID := newSession(t, h)
	id := promptID(h, sessionID, "long turn")
	waitForFile(t, ready)

	h.sendLine(`{"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":"` + sessionID + `"}}`)

	if got := stopReason(t, h.responseFor(id)); got != StopReasonCancelled {
		t.Fatalf("stopReason = %q, want cancelled", got)
	}
	if got := waitForFile(t, killed); !strings.Contains(got, "terminated") {
		t.Fatalf("stub observed %q, want SIGTERM delivered to the group", got)
	}
}

func TestStubFailureQuotesStderr(t *testing.T) {
	_, h := stubEnv(t, "fail")

	sessionID := newSession(t, h)
	msg := h.responseFor(promptID(h, sessionID, "question"))
	if msg.Error == nil || msg.Error.Code != CodeServerFailure {
		t.Fatalf("failed turn = %+v, want -32000", msg.Error)
	}
	if !strings.Contains(msg.Error.Message, "quota exceeded") {
		t.Fatalf("message = %q, want the child's stderr tail", msg.Error.Message)
	}
}

func TestStubSwallowedErrorIsReported(t *testing.T) {
	_, h := stubEnv(t, "swallow")

	sessionID := newSession(t, h)
	msg := h.responseFor(promptID(h, sessionID, "question"))
	if msg.Error == nil || msg.Error.Code != CodeInternalError {
		t.Fatalf("swallowed failure = %+v, want -32603", msg.Error)
	}
	if !strings.Contains(msg.Error.Message, "RESOURCE_EXHAUSTED") {
		t.Fatalf("message = %q", msg.Error.Message)
	}
}

func TestStubReceivesModelAndPromptInArgv(t *testing.T) {
	env, h := stubEnv(t, "stream")
	argvLog := filepath.Join(env.workDir, "argv.log")
	t.Setenv("STUB_ARGV_LOG", argvLog)

	sessionID := newSession(t, h)
	mustOK(t, h.responseFor(h.request("session/setConfigOption", map[string]string{
		"sessionId": sessionID, "configId": "model", "value": "Gemini 3.1 Pro (High)",
	})))
	mustOK(t, h.responseFor(promptID(h, sessionID, "multi word prompt")))

	data, err := os.ReadFile(argvLog)
	if err != nil {
		t.Fatalf("stub never recorded its argv: %v", err)
	}
	line := strings.TrimSpace(string(data))
	for _, want := range []string{
		`"--add-dir" ` + `"` + env.workDir + `"`,
		`"--print-timeout" "20m"`,
		`"--model" "Gemini 3.1 Pro (High)"`,
		`"-p" "multi word prompt"`,
	} {
		if !strings.Contains(line, want) {
			t.Fatalf("argv = %s, want it to contain %s", line, want)
		}
	}
}

func TestFetchModelsRunsTheRealBinary(t *testing.T) {
	stub := agyStub(t)

	t.Setenv("STUB_MODELS", "Gemini 3.5 Flash (High)\nGemini 3.1 Pro (Low)\n\n")
	models, err := fetchModels(context.Background(), stub)
	if err != nil {
		t.Fatalf("fetchModels: %v", err)
	}
	if len(models) != 2 || models[0] != "Gemini 3.5 Flash (High)" || models[1] != "Gemini 3.1 Pro (Low)" {
		t.Fatalf("models = %v", models)
	}

	t.Setenv("STUB_MODELS_FAIL", "1")
	if _, err := fetchModels(context.Background(), stub); err == nil {
		t.Fatal("a failing models listing was reported as success")
	}
}

func TestModelProviderEndToEndWithStub(t *testing.T) {
	env := newTestEnv(t)
	stub := agyStub(t)
	t.Setenv("STUB_MODELS", "Stub Model A\nStub Model B")

	provider := NewModelProvider(stub, env.stateDir, func(string, ...any) {})
	raw := provider.ConfigOptions(context.Background(), "Stub Model B")

	var options []struct {
		ID           string `json:"id"`
		CurrentValue string `json:"currentValue"`
		Options      []struct {
			Value string `json:"value"`
		} `json:"options"`
	}
	if err := json.Unmarshal(raw, &options); err != nil {
		t.Fatalf("configOptions = %s: %v", raw, err)
	}
	if len(options) != 1 || len(options[0].Options) != 2 || options[0].CurrentValue != "Stub Model B" {
		t.Fatalf("configOptions = %s", raw)
	}
}

// TestStubSendsTheMachineID is the end-to-end guard for the two shapes agy will not
// take: the display name alone is unstable across releases and the composite
// "id<TAB>name" line `agy models` prints is rejected outright. Clients may pick
// either form, and the child must still be handed the machine id.
func TestStubSendsTheMachineID(t *testing.T) {
	env, h := stubEnv(t, "stream")
	argvLog := filepath.Join(env.workDir, "argv.log")
	t.Setenv("STUB_ARGV_LOG", argvLog)
	t.Setenv("STUB_MODELS", "gemini-3-1-pro-high\tGemini 3.1 Pro (High)\nclaude-sonnet-4-6\tClaude Sonnet 4.6 (Thinking)")

	sessionID := newSession(t, h)

	// Choosing the label rather than the value is what a client that only renders
	// option names does.
	mustOK(t, h.responseFor(h.request("session/setConfigOption", map[string]string{
		"sessionId": sessionID, "configId": "model", "value": "Gemini 3.1 Pro (High)",
	})))
	mustOK(t, h.responseFor(promptID(h, sessionID, "pick a model")))

	data, err := os.ReadFile(argvLog)
	if err != nil {
		t.Fatalf("stub never recorded its argv: %v", err)
	}
	line := string(data)
	if !strings.Contains(line, `"--model" "gemini-3-1-pro-high"`) {
		t.Fatalf("argv = %s, want the machine id after --model", line)
	}
	if strings.Contains(line, "Gemini 3.1 Pro (High)") {
		t.Fatalf("argv = %s, want no display name in the child's argv", line)
	}

	// The persisted session keeps the same id, so a restarted bridge sends the same
	// command line.
	sessions, err := NewStore(env.stateDir).Read()
	if err != nil {
		t.Fatal(err)
	}
	if got := sessions[sessionID].ModelID; got != "gemini-3-1-pro-high" {
		t.Fatalf("stored ModelID = %q, want gemini-3-1-pro-high", got)
	}
}
