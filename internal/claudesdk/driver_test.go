package claudesdk

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/colinrgodsey/wackyacp/internal/acp"
	"github.com/colinrgodsey/wackypub/pkg/agent"
	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
)

const fakeWorkerEnv = "WACKYACP_FAKE_WORKER"

// TestHelperFakeWorker is not a test. It is the canned worker the driver spawns
// when WACKYACP_FAKE_WORKER names a scenario, so the frame protocol, the resume
// retry and cancellation are all exercised without node or a Claude session.
func TestHelperFakeWorker(t *testing.T) {
	if os.Getenv(fakeWorkerEnv) == "" {
		t.Skip("helper process for driver tests")
	}
	scenario := os.Getenv(fakeWorkerEnv)
	out := func(v any) { _ = json.NewEncoder(os.Stdout).Encode(v) }

	out(workerEvent{Type: frameReady})

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var frame map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &frame); err != nil {
			continue
		}
		if frame["type"] == toolResponseFrame {
			if scenario == "scratchpad" {
				text, _ := frame["text"].(string)
				id, _ := frame["requestId"].(float64)
				_ = id
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
			case "scratchpad":
				out(workerEvent{Type: frameToolRequest, RequestID: 1, Name: "create_scratchpad",
					Args: map[string]any{"text": "deposited by claude", "createdBy": "unit-test"}})
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

func newTestDriver(t *testing.T, scenario string, sessionID string) *Driver {
	t.Helper()
	ws := t.TempDir()
	agentDir := filepath.Join(ws, "claude")
	writeFile(t, filepath.Join(agentDir, "AGENTS.md"), "test agent"+nl)
	d, err := New(Config{
		AgentFolder: agentDir,
		SessionID:   sessionID,
		Spawn: func(ctx context.Context) (*exec.Cmd, error) {
			cmd := exec.Command(os.Args[0], "-test.run=TestHelperFakeWorker")
			cmd.Env = append(os.Environ(), fakeWorkerEnv+"="+scenario)
			return cmd, nil
		},
	})
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
		t.Errorf("tool calls = %v, want [Bash]", tools)
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
