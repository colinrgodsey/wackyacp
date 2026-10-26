package claudesdk

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/colinrgodsey/wackyacp/internal/acp"
	"github.com/colinrgodsey/wackypub/pkg/agent"
	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
)

const fakeWorkerEnv = "WACKYACP_FAKE_WORKER"

// TestHelperFakeWorker is not a test. It is the canned worker the driver spawns
// when WACKYACP_FAKE_WORKER names a scenario, so the frame protocol, the resume
// retry, cancellation and multi-turn worker reuse are all exercised without node
// or a Claude session.
//
// Scenario "multi" answers every prompt and never exits, and reports the
// snapshot flag it was given as the stop reason, so a test can watch one worker
// serve several turns and see the prompt-cache decision.
func TestHelperFakeWorker(t *testing.T) {
	if os.Getenv(fakeWorkerEnv) == "" {
		t.Skip("helper process for driver tests")
	}
	scenario := os.Getenv(fakeWorkerEnv)
	out := func(v any) { _ = json.NewEncoder(os.Stdout).Encode(v) }

	out(workerEvent{Type: frameReady})

	turn := 0
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var frame map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &frame); err != nil {
			continue
		}
		if frame["type"] == toolResponseFrame {
			if scenario == "scratchpad" {
				text, _ := frame["text"].(string)
				out(workerEvent{Type: frameResult, ID: 1, StopReason: text, SessionID: "sess-1"})
			}
			continue
		}
		switch frame["op"] {
		case opCancel:
			// Nothing to unwind in a canned worker; the driver's own contract is
			// that it stopped waiting.
		case opPrompt:
			id, _ := frame["id"].(float64)
			reqID := int64(id)
			resume, _ := frame["resume"].(bool)
			sid, _ := frame["sessionId"].(string)
			switch scenario {
			case "basic":
				out(workerEvent{Type: frameEvent, ID: reqID, Kind: kindText, Text: "hello "})
				out(workerEvent{Type: frameEvent, ID: reqID, Kind: kindText, Text: "world"})
				out(workerEvent{Type: frameEvent, ID: reqID, Kind: kindToolCall, CallID: "call-1", ToolName: "Bash", ArgsSummary: "ls"})
				out(workerEvent{Type: frameEvent, ID: reqID, Kind: kindToolUpdate, CallID: "call-1", Status: "completed"})
				out(workerEvent{Type: frameResult, ID: reqID, SessionID: sid, StopReason: "end_turn",
					Usage: &usageFrame{InputTokens: 3, OutputTokens: 4, TotalTokens: 7}})
				os.Exit(0)
			case "multi":
				turn++
				out(workerEvent{Type: frameEvent, ID: reqID, Kind: kindText, Text: fmt.Sprintf("turn-%d", turn)})
				snapshot := false
				if spec, ok := frame["systemPrompt"].(map[string]any); ok {
					snapshot, _ = spec["snapshot"].(bool)
				}
				if sid == "" {
					sid = "sess-multi"
				}
				out(workerEvent{Type: frameResult, ID: reqID, SessionID: sid,
					StopReason: fmt.Sprintf("snapshot=%v", snapshot)})
			case "scratchpad":
				out(workerEvent{Type: frameToolRequest, RequestID: 1, Name: "create_scratchpad",
					Args: map[string]any{"text": "deposited by claude", "createdBy": "unit-test"}})
			case "model":
				if v, ok := frame["model"]; ok {
					out(workerEvent{Type: frameResult, ID: reqID, SessionID: sid, StopReason: "model=" + fmt.Sprint(v)})
				} else {
					out(workerEvent{Type: frameResult, ID: reqID, SessionID: sid, StopReason: "model-absent"})
				}
				os.Exit(0)
			case "resume-fail":
				if resume {
					out(workerEvent{Type: frameError, ID: reqID, Code: codeResumeFailed, Message: "no transcript at this id"})
					continue
				}
				out(workerEvent{Type: frameResult, ID: reqID, SessionID: sid, StopReason: "retried-fresh"})
				os.Exit(0)
			case "hang":
				// deliberately never answers, so the caller has to cancel
			}
		}
	}
}

// spawnRecorder captures what the driver handed to Spawn, so a test can prove
// the worker is bound to the bridge lifetime and not to one turn.
type spawnRecorder struct {
	mu       sync.Mutex
	ctx      context.Context
	killed   int
	scenario string
}

func (r *spawnRecorder) set(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ctx = ctx
}

func (r *spawnRecorder) spawnedContext() context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ctx
}

// spawn binds the child to the context it is given, exactly as production does
// with exec.CommandContext. This is what makes the worker-lifetime contract
// testable: hand it a per-turn context and the child dies with that turn.
func (r *spawnRecorder) spawn(ctx context.Context) (*exec.Cmd, error) {
	r.set(ctx)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=TestHelperFakeWorker")
	cmd.Env = append(os.Environ(), fakeWorkerEnv+"="+r.scenario)
	return cmd, nil
}

// newTestDriver keeps the single-turn tests terse; the spawn is still bound to
// whatever context the driver passes, so production semantics apply.
func newTestDriver(t *testing.T, scenario string, sessionID string) *Driver {
	return newRecordingDriver(t, scenario, func(cfg *Config) {
		cfg.SessionID = sessionID
	})
}

func newRecordingDriver(t *testing.T, scenario string, extra func(*Config)) *Driver {
	t.Helper()
	agentDir := filepath.Join(t.TempDir(), "claude")
	writeFile(t, filepath.Join(agentDir, "AGENTS.md"), "test agent"+nl)
	rec := &spawnRecorder{scenario: scenario}
	cfg := Config{
		AgentFolder: agentDir,
		Spawn:       rec.spawn,
	}
	if extra != nil {
		extra(&cfg)
	}
	d, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func TestDriverPromptMapsChunksToolsAndUsage(t *testing.T) {
	d := newTestDriver(t, "basic", "")

	var chunks []string
	var tools []string
	var statuses []string
	cbs := acp.TurnCallbacks{
		OnChunk:          func(text string) error { chunks = append(chunks, text); return nil },
		OnToolCall:       func(c *agentv1.ToolCall) error { tools = append(tools, c.ToolName); return nil },
		OnToolCallUpdate: func(u *agentv1.ToolCallUpdate) error { statuses = append(statuses, u.Status); return nil },
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	result, err := d.Prompt(ctx, "", "hi there", cbs)
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if got := chunks; len(got) != 2 || got[0]+got[1] != "hello world" {
		t.Errorf("chunks = %q, want two framing hello world", got)
	}
	if len(tools) != 1 || tools[0] != "Bash" {
		t.Errorf("tool calls = %v, want [Bash]", tools[0:0])
	}
	if len(statuses) != 1 || statuses[0] != acp.ToolStatusCompleted {
		t.Errorf("tool updates = %v, want [completed]", statuses)
	}
	if result.StopReason != "end_turn" {
		t.Errorf("stop reason = %q", result.StopReason)
	}
	if result.Usage.PromptTokens != 3 || result.Usage.CompletionTokens != 4 || result.Usage.TotalTokens != 7 {
		t.Errorf("usage = %+v", result.Usage)
	}
	if result.Usage.Backend != "claude-agent-sdk" {
		t.Errorf("usage backend = %q", result.Usage.Backend)
	}
	if d.SessionID() == "" {
		t.Errorf("driver did not adopt a session id")
	}
}

// TestDriverReusesOneWorkerAcrossTurns is the guard for the worker lifetime
// contract. Each turn gets its own context that is cancelled when the turn
// returns, which is what D112 does with a gRPC stream context. If the worker is
// bound to that context it dies at cancel(), the next turn silently starts a new
// one, and the killed process is only reclaimed by luck.
func TestDriverReusesOneWorkerAcrossTurns(t *testing.T) {
	d := newRecordingDriver(t, "multi", nil)

	firstPID := 0
	for i := 1; i <= 3; i++ {
		var got string
		cbs := acp.TurnCallbacks{OnChunk: func(text string) error { got += text; return nil }}

		turnCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		res, err := d.Prompt(turnCtx, "", fmt.Sprintf("turn %d", i), cbs)
		cancel()
		if err != nil {
			t.Fatalf("turn %d: %v", i, err)
		}
		if want := fmt.Sprintf("turn-%d", i); got != want {
			t.Fatalf("turn %d echoed %q, want %q", i, got, want)
		}
		if res.StopReason == "" {
			t.Fatalf("turn %d returned no stop reason", i)
		}
		pid := d.workerPID()
		if pid == 0 {
			t.Fatalf("turn %d: driver lost track of the worker process", i)
		}
		if firstPID == 0 {
			firstPID = pid
		} else if pid != firstPID {
			t.Fatalf("turn %d ran on worker pid %d, turn 1 used pid %d: the worker is bound to the per-turn context, so every later turn restarts or leaks a node process", i, pid, firstPID)
		}
	}

	// The context the worker was spawned with must still be live after three
	// turns whose own contexts are all cancelled.
	if ctx := d.spawnedWorkerContext(); ctx == nil {
		t.Errorf("driver never recorded a spawn context")
	} else if ctx.Err() != nil {
		t.Errorf("worker context is already done (%v); it is bound to a turn, not the bridge", ctx.Err())
	}

	if err := d.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if processExists(firstPID) {
		t.Errorf("worker pid %d survived Close: orphaned process", firstPID)
	}
}

// TestDriverResumesAfterWorkerDeath proves a dead worker is recoverable and that
// the per-worker handshake channels are real: the driver must wait for the NEW
// worker's ready frame instead of returning a stale closed-channel signal.
func TestDriverResumesAfterWorkerDeath(t *testing.T) {
	d := newRecordingDriver(t, "multi", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := d.Prompt(ctx, "", "first", acp.TurnCallbacks{}); err != nil {
		t.Fatalf("first turn: %v", err)
	}
	pid := d.workerPID()
	if pid == 0 {
		t.Fatalf("no worker pid")
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		t.Fatalf("FindProcess %d: %v", pid, err)
	}
	if err := proc.Kill(); err != nil {
		t.Fatalf("kill worker %d: %v", pid, err)
	}
	// Give the read loop a moment to reap it, as a real crash would.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && d.workerPID() == pid {
		time.Sleep(10 * time.Millisecond)
	}

	if _, err := d.Prompt(ctx, "", "second after crash", acp.TurnCallbacks{}); err != nil {
		t.Fatalf("turn after worker death must respawn and succeed, got %v", err)
	}
	if newPID := d.workerPID(); newPID == pid {
		t.Errorf("driver reported the dead worker pid %d again", pid)
	}
}

func TestDriverScratchpadToolRoundTrip(t *testing.T) {
	d := newTestDriver(t, "scratchpad", "")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	result, err := d.Prompt(ctx, "", "deposit this", acp.TurnCallbacks{})
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	// The canned worker returns whatever text the Go tool handler produced, so
	// the stop reason is the created entry's JSON.
	if result.StopReason == "" {
		t.Fatalf("worker got no answer from the scratchpad tool handler")
	}
	items, count, _, err := agent.ListScratchpads(d.cfg.AgentFolder)
	if err != nil {
		t.Fatalf("ListScratchpads: %v", err)
	}
	if count != 1 {
		t.Fatalf("scratchpad count = %d, want 1", count)
	}
	text, err := agent.GetScratchpad(d.cfg.AgentFolder, items[0].ID, nil, nil)
	if err != nil {
		t.Fatalf("GetScratchpad: %v", err)
	}
	if text != "deposited by claude" {
		t.Errorf("entry text = %q", text)
	}
}

func TestDriverRetriesOnceWhenResumeFails(t *testing.T) {
	d := newTestDriver(t, "resume-fail", "persisted-session-id")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	result, err := d.Prompt(ctx, "persisted-session-id", "hi", acp.TurnCallbacks{})
	if err != nil {
		t.Fatalf("resume failure must be retried, got %v", err)
	}
	if result.StopReason != "retried-fresh" {
		t.Errorf("stop reason = %q, want the fresh-session attempt", result.StopReason)
	}
}

func TestDriverContextCancelReturnsAndDoesNotHang(t *testing.T) {
	d := newTestDriver(t, "hang", "")
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := d.Prompt(ctx, "", "never answered", acp.TurnCallbacks{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context.DeadlineExceeded", err)
	}
	// The worker must survive a cancelled turn: cancellation is not a crash.
	if d.workerPID() == 0 {
		t.Errorf("worker went away with the cancelled turn context")
	}
}

func TestDriverConfigOptionSurfaceIsExplicitlyEmpty(t *testing.T) {
	d := newTestDriver(t, "basic", "")
	if got := string(d.LastConfigOptions()); got != `{"configOptions":[]}` {
		t.Errorf("LastConfigOptions = %s", got)
	}
	if _, err := d.SetConfigOption(context.Background(), "s", "mode", "yolo"); err == nil {
		t.Errorf("SetConfigOption must refuse in SDK-direct mode, not accept silently")
	}
}

func processExists(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// TestDriverReportsLostSessionContext proves the fresh-session retry is visible
// to the caller. Silently dropping conversation context is the failure mode a
// reviewer cannot see in a green turn record.
func TestDriverReportsLostSessionContext(t *testing.T) {
	d := newTestDriver(t, "resume-fail", "persisted-session-id")

	var warnings []string
	cbs := acp.TurnCallbacks{OnWarning: func(w string) error { warnings = append(warnings, w); return nil }}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := d.Prompt(ctx, "persisted-session-id", "hi", cbs); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	found := false
	for _, w := range warnings {
		if strings.Contains(w, "session context lost") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected an OnWarning about lost session context, got %v", warnings)
	}
}

// TestPromptDigestSurvivesDriverRestart pins the prompt-cache discipline: the
// first turn of a bridge has nothing cached, later turns must snapshot, and a
// restarted bridge must remember the digest instead of forcing a miss.
func TestPromptDigestSurvivesDriverRestart(t *testing.T) {
	agentDir := filepath.Join(t.TempDir(), "claude")
	writeFile(t, filepath.Join(agentDir, "AGENTS.md"), "test agent"+nl)

	newDriver := func() *Driver {
		rec := &spawnRecorder{scenario: "multi"}
		d, err := New(Config{AgentFolder: agentDir, Spawn: rec.spawn})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { _ = d.Close() })
		return d
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	first := newDriver()
	res, err := first.Prompt(ctx, "", "one", acp.TurnCallbacks{})
	if err != nil {
		t.Fatalf("first bridge turn 1: %v", err)
	}
	if res.StopReason != "snapshot=false" {
		t.Errorf("turn 1 sent %q, want snapshot=false because nothing is cached yet", res.StopReason)
	}
	res, err = first.Prompt(ctx, "", "two", acp.TurnCallbacks{})
	if err != nil {
		t.Fatalf("first bridge turn 2: %v", err)
	}
	if res.StopReason != "snapshot=true" {
		t.Errorf("turn 2 sent %q, want snapshot=true because the prompt is unchanged", res.StopReason)
	}

	second := newDriver()
	res, err = second.Prompt(ctx, "", "three", acp.TurnCallbacks{})
	if err != nil {
		t.Fatalf("restarted bridge turn: %v", err)
	}
	if res.StopReason != "snapshot=true" {
		t.Errorf("a restarted bridge sent %q: it lost the prompt digest and forced a cache miss", res.StopReason)
	}
}

// TestDriverSendsModelOnlyWhenConfigured is the frame-level contract for
// --model: configured means the field arrives, unconfigured means the key is
// absent so Claude keeps its own default resolution.
func TestDriverSendsModelOnlyWhenConfigured(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	set := newRecordingDriver(t, "model", func(cfg *Config) { cfg.Model = "claude-opus-4-1" })
	res, err := set.Prompt(ctx, "", "which model", acp.TurnCallbacks{})
	if err != nil {
		t.Fatalf("prompt with a configured model: %v", err)
	}
	if res.StopReason != "model=claude-opus-4-1" {
		t.Errorf("worker saw %q, want the configured model to arrive in the request frame", res.StopReason)
	}

	unset := newRecordingDriver(t, "model", nil)
	res, err = unset.Prompt(ctx, "", "which model", acp.TurnCallbacks{})
	if err != nil {
		t.Fatalf("prompt without a model: %v", err)
	}
	if res.StopReason != "model-absent" {
		t.Errorf("worker saw %q: an unset model must be omitted from the frame, not sent empty", res.StopReason)
	}
}

func TestWorkerRequestMarshalsModelOnlyWhenSet(t *testing.T) {
	data, err := json.Marshal(workerRequest{ID: 1, Op: opPrompt})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(data), "model") {
		t.Errorf("empty model must not appear in the frame at all, got %s", data)
	}

	data, err = json.Marshal(workerRequest{ID: 1, Op: opPrompt, Model: "claude-sonnet-4"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(data), `"model":"claude-sonnet-4"`) {
		t.Errorf("configured model missing from the frame, got %s", data)
	}
}
