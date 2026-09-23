package acp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/colinrgodsey/wackyacp/internal/harness"
	"github.com/colinrgodsey/wackyacp/internal/session"
)

var (
	shimBinOnce sync.Once
	shimBinPath string
)

func getShimBin(t *testing.T) string {
	shimBinOnce.Do(func() {
		tmpDir, err := os.MkdirTemp("", "acpshimbin-test-*")
		if err != nil {
			t.Fatalf("creating temp dir: %v", err)
		}
		outPath := filepath.Join(tmpDir, "acpshimbin")
		cmd := exec.Command("go", "build", "-o", outPath, "../../testdata/acpshimbin")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("building acpshimbin fixture failed: %v\n%s", err, string(out))
		}
		shimBinPath = outPath
	})
	return shimBinPath
}

func startShimClient(t *testing.T, script string) (*Client, *harness.Process) {
	bin := getShimBin(t)
	ctx := context.Background()

	proc, err := harness.Start(ctx, harness.Config{
		Command: bin,
		Args:    []string{"--script=" + script},
	})
	if err != nil {
		t.Fatalf("starting shim failed: %v", err)
	}

	t.Cleanup(func() {
		_ = proc.Close()
	})

	client := NewClient(proc.Stdin, proc.Stdout)
	return client, proc
}

func TestClient_Initialize(t *testing.T) {
	client, proc := startShimClient(t, "normal")
	defer proc.Close()

	ctx := context.Background()
	res, err := client.Initialize(ctx)
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	if res.ProtocolVersion != 1 {
		t.Errorf("expected protocolVersion 1, got %d", res.ProtocolVersion)
	}
	if client.Capabilities.SessionCapabilities.Resume == nil {
		t.Errorf("expected resume capability true")
	}
}

func TestClient_EstablishSession_ResumeOk(t *testing.T) {
	client, proc := startShimClient(t, "resume-ok")
	defer proc.Close()

	ctx := context.Background()
	if _, err := client.Initialize(ctx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	agentDir := t.TempDir()
	saved := &session.SessionData{
		SessionID:   "existing-session-123",
		AgentFolder: agentDir,
	}

	sessionID, err := client.EstablishSession(ctx, agentDir, saved)
	if err != nil {
		t.Fatalf("EstablishSession failed: %v", err)
	}
	if sessionID != "existing-session-123" {
		t.Errorf("expected resumed sessionID existing-session-123, got: %s", sessionID)
	}
}

func TestClient_EstablishSession_FailResume_FallbackLoad(t *testing.T) {
	client, proc := startShimClient(t, "fail-resume")
	defer proc.Close()

	ctx := context.Background()
	if _, err := client.Initialize(ctx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	agentDir := t.TempDir()
	saved := &session.SessionData{
		SessionID:   "existing-session-456",
		AgentFolder: agentDir,
	}

	// Should attempt resume, fail, then attempt load and succeed
	sessionID, err := client.EstablishSession(ctx, agentDir, saved)
	if err != nil {
		t.Fatalf("EstablishSession failed: %v", err)
	}
	if sessionID != "existing-session-456" {
		t.Errorf("expected loaded sessionID existing-session-456, got: %s", sessionID)
	}
}

func TestClient_EstablishSession_FailLoad_FallbackNew(t *testing.T) {
	client, proc := startShimClient(t, "fail-load")
	defer proc.Close()

	ctx := context.Background()
	if _, err := client.Initialize(ctx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	agentDir := t.TempDir()
	saved := &session.SessionData{
		SessionID:   "stale-session-789",
		AgentFolder: agentDir,
	}

	// Should attempt resume (fails), load (fails), then new (succeeds)
	sessionID, err := client.EstablishSession(ctx, agentDir, saved)
	if err != nil {
		t.Fatalf("EstablishSession failed: %v", err)
	}
	if sessionID != "shim-session-fail-load" {
		t.Errorf("expected new sessionID shim-session-fail-load, got: %s", sessionID)
	}

	// Verify session file was persisted
	persisted, err := session.ReadSession(agentDir)
	if err != nil {
		t.Fatalf("reading persisted session failed: %v", err)
	}
	if persisted.SessionID != "shim-session-fail-load" {
		t.Errorf("persisted sessionID mismatch: %s", persisted.SessionID)
	}
}

func TestClient_EstablishSession_HarnessMismatchFallsBackToNew(t *testing.T) {
	inReader, inWriter := io.Pipe()
	outReader, outWriter := io.Pipe()

	client := NewClient(outWriter, inReader)
	client.Capabilities.SessionCapabilities.Resume = any(true)

	go func() {
		scanner := bufio.NewScanner(outReader)
		for scanner.Scan() {
			line := scanner.Bytes()
			var req map[string]any
			if err := json.Unmarshal(line, &req); err != nil {
				continue
			}
			method, _ := req["method"].(string)
			id := req["id"]
			switch method {
			case "session/resume":
				// Return a mismatched agent_folder
				resp := map[string]any{
					"jsonrpc": "2.0",
					"id":      id,
					"result": map[string]any{
						"_meta": map[string]any{
							"agent_folder": "/different/agent/folder",
						},
					},
				}
				data, _ := json.Marshal(resp)
				_, _ = fmt.Fprintf(inWriter, "%s\n", data)
			case "session/new":
				resp := map[string]any{
					"jsonrpc": "2.0",
					"id":      id,
					"result": map[string]any{
						"sessionId": "fresh-session-after-mismatch",
					},
				}
				data, _ := json.Marshal(resp)
				_, _ = fmt.Fprintf(inWriter, "%s\n", data)
			}
		}
	}()

	agentDir := t.TempDir()
	saved := &session.SessionData{
		SessionID:   "old-session-123",
		AgentFolder: agentDir,
	}

	sessionID, err := client.EstablishSession(context.Background(), agentDir, saved)
	if err != nil {
		t.Fatalf("EstablishSession failed: %v", err)
	}
	if sessionID != "fresh-session-after-mismatch" {
		t.Errorf("expected fresh-session-after-mismatch, got: %s", sessionID)
	}

	_ = inWriter.Close()
	_ = outWriter.Close()
}

func TestClient_Prompt_EmitUsage(t *testing.T) {
	client, proc := startShimClient(t, "emit-usage")
	defer proc.Close()

	ctx := context.Background()
	if _, err := client.Initialize(ctx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	var chunks []string
	var warnings []string
	callbacks := TurnCallbacks{
		OnChunk: func(text string) error {
			chunks = append(chunks, text)
			return nil
		},
		OnWarning: func(w string) error {
			warnings = append(warnings, w)
			return nil
		},
	}

	res, err := client.Prompt(ctx, "session-usage", "test prompt", callbacks)
	if err != nil {
		t.Fatalf("Prompt failed: %v", err)
	}

	if len(chunks) == 0 {
		t.Fatalf("expected chunks, got none")
	}
	if res.Usage.TotalTokens != 150 {
		t.Errorf("expected TotalTokens 150, got %d", res.Usage.TotalTokens)
	}
	if res.Usage.PromptTokens != 100 {
		t.Errorf("expected PromptTokens 100, got %d", res.Usage.PromptTokens)
	}
	if res.Usage.CompletionTokens != 50 {
		t.Errorf("expected CompletionTokens 50, got %d", res.Usage.CompletionTokens)
	}
}

func TestClient_Prompt_AutoDenyPermission(t *testing.T) {
	client, proc := startShimClient(t, "hang-on-permission")
	defer proc.Close()

	ctx := context.Background()
	if _, err := client.Initialize(ctx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	var chunks []string
	var warnings []string
	callbacks := TurnCallbacks{
		OnChunk: func(text string) error {
			chunks = append(chunks, text)
			return nil
		},
		OnWarning: func(w string) error {
			warnings = append(warnings, w)
			return nil
		},
	}

	res, err := client.Prompt(ctx, "session-perm", "test prompt", callbacks)
	if err != nil {
		t.Fatalf("Prompt failed: %v", err)
	}

	if res.StopReason != "end_turn" {
		t.Errorf("expected stopReason end_turn, got: %s", res.StopReason)
	}

	// Verify that permission request auto-denial surfaced a warning
	if len(warnings) == 0 {
		t.Fatalf("expected auto-deny warning, got none")
	}
	foundDenyWarning := false
	for _, w := range warnings {
		if strings.Contains(w, "auto-denied") {
			foundDenyWarning = true
			break
		}
	}
	if !foundDenyWarning {
		t.Errorf("expected auto-deny warning, got: %v", warnings)
	}

	// Verify that the shim got the deny response and reported the outcome
	foundOutcomeChunk := false
	for _, c := range chunks {
		if strings.Contains(c, "permission outcome:") {
			foundOutcomeChunk = true
			break
		}
	}
	if !foundOutcomeChunk {
		t.Errorf("expected outcome chunk from shim, got: %v", chunks)
	}
}

type pipeMockHarness struct {
	t        *testing.T
	inWriter *io.PipeWriter
	scanner  *bufio.Scanner
}

func newPipeMockHarness(t *testing.T) (*Client, *pipeMockHarness, func()) {
	inReader, inWriter := io.Pipe()
	outReader, outWriter := io.Pipe()

	client := NewClient(outWriter, inReader)
	scanner := bufio.NewScanner(outReader)

	h := &pipeMockHarness{
		t:        t,
		inWriter: inWriter,
		scanner:  scanner,
	}

	cleanup := func() {
		_ = inWriter.Close()
		_ = outWriter.Close()
	}

	return client, h, cleanup
}

func (h *pipeMockHarness) readRequest() (map[string]any, int64) {
	if !h.scanner.Scan() {
		h.t.Fatalf("expected request, got scanner error: %v", h.scanner.Err())
	}
	var req map[string]any
	if err := json.Unmarshal(h.scanner.Bytes(), &req); err != nil {
		h.t.Fatalf("unmarshaling request: %v", err)
	}
	var id int64
	switch v := req["id"].(type) {
	case float64:
		id = int64(v)
	case int64:
		id = v
	}
	return req, id
}

func (h *pipeMockHarness) sendChunk(sessionID, text string) {
	h.sendUpdate(sessionID, map[string]any{
		"sessionUpdate": "agent_message_chunk",
		"content": map[string]any{
			"type": "text",
			"text": text,
		},
	})
}

func (h *pipeMockHarness) sendUpdate(sessionID string, update map[string]any) {
	notif := map[string]any{
		"jsonrpc": "2.0",
		"method":  "session/update",
		"params": map[string]any{
			"sessionId": sessionID,
			"update":    update,
		},
	}
	data, err := json.Marshal(notif)
	if err != nil {
		h.t.Fatalf("marshaling notif: %v", err)
	}
	_, _ = fmt.Fprintf(h.inWriter, "%s\n", data)
}

func (h *pipeMockHarness) sendPromptResponse(id int64, stopReason string) {
	resp := map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"result": map[string]any{
			"stopReason": stopReason,
		},
	}
	data, err := json.Marshal(resp)
	if err != nil {
		h.t.Fatalf("marshaling response: %v", err)
	}
	_, _ = fmt.Fprintf(h.inWriter, "%s\n", data)
}

func TestClient_Coalesce_DeltaFragmentsJoinContiguously(t *testing.T) {
	client, harness, cleanup := newPipeMockHarness(t)
	defer cleanup()

	var (
		chunksMu sync.Mutex
		chunks   []string
	)
	callbacks := TurnCallbacks{
		OnChunk: func(text string) error {
			chunksMu.Lock()
			chunks = append(chunks, text)
			chunksMu.Unlock()
			return nil
		},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, id := harness.readRequest()
		harness.sendChunk("sess-1", "Hello, ")
		harness.sendChunk("sess-1", "world")
		harness.sendChunk("sess-1", "!")
		harness.sendPromptResponse(id, "end_turn")
	}()

	res, err := client.Prompt(context.Background(), "sess-1", "hi", callbacks)
	if err != nil {
		t.Fatalf("Prompt failed: %v", err)
	}
	if res.StopReason != "end_turn" {
		t.Errorf("expected end_turn, got %s", res.StopReason)
	}
	<-done

	chunksMu.Lock()
	defer chunksMu.Unlock()
	if len(chunks) != 1 {
		t.Fatalf("expected 1 coalesced chunk, got %d: %v", len(chunks), chunks)
	}
	if chunks[0] != "Hello, world!" {
		t.Errorf("expected 'Hello, world!', got %q", chunks[0])
	}
}

func TestClient_Coalesce_BoundaryFlushOnToolCall(t *testing.T) {
	client, harness, cleanup := newPipeMockHarness(t)
	defer cleanup()

	var (
		chunksMu sync.Mutex
		chunks   []string
	)
	callbacks := TurnCallbacks{
		OnChunk: func(text string) error {
			chunksMu.Lock()
			chunks = append(chunks, text)
			chunksMu.Unlock()
			return nil
		},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, id := harness.readRequest()
		// First logical message part
		harness.sendChunk("sess-1", "I will check ")
		harness.sendChunk("sess-1", "the file.")
		// Boundary 1: tool_call arrives
		harness.sendUpdate("sess-1", map[string]any{
			"sessionUpdate": "tool_call",
			"toolCallId":    "call-1",
			"title":         "read_file",
		})
		// Boundary 2: tool_call_update arrives
		harness.sendUpdate("sess-1", map[string]any{
			"sessionUpdate": "tool_call_update",
			"toolCallId":    "call-1",
			"status":        "completed",
		})
		// Second logical message part
		harness.sendChunk("sess-1", "The file has ")
		harness.sendChunk("sess-1", "3 lines.")
		// Boundary 3: usage_update arrives
		harness.sendUpdate("sess-1", map[string]any{
			"sessionUpdate": "usage_update",
			"used":          42,
		})
		// Third logical message part
		harness.sendChunk("sess-1", "All done.")
		harness.sendPromptResponse(id, "end_turn")
	}()

	res, err := client.Prompt(context.Background(), "sess-1", "do work", callbacks)
	if err != nil {
		t.Fatalf("Prompt failed: %v", err)
	}
	if res.StopReason != "end_turn" {
		t.Errorf("expected end_turn, got %s", res.StopReason)
	}
	<-done

	chunksMu.Lock()
	defer chunksMu.Unlock()
	want := []string{
		"I will check the file.",
		"The file has 3 lines.",
		"All done.",
	}
	if len(chunks) != len(want) {
		t.Fatalf("expected %d chunks, got %d: %v", len(want), len(chunks), chunks)
	}
	for i := range want {
		if chunks[i] != want[i] {
			t.Errorf("chunk %d: expected %q, got %q", i, want[i], chunks[i])
		}
	}
}

func TestClient_Coalesce_BoundaryFlushAtTurnEnd(t *testing.T) {
	client, harness, cleanup := newPipeMockHarness(t)
	defer cleanup()

	var (
		chunksMu sync.Mutex
		chunks   []string
	)
	callbacks := TurnCallbacks{
		OnChunk: func(text string) error {
			chunksMu.Lock()
			chunks = append(chunks, text)
			chunksMu.Unlock()
			return nil
		},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, id := harness.readRequest()
		harness.sendChunk("sess-1", "Single part across ")
		harness.sendChunk("sess-1", "multiple streaming deltas.")
		// No tool call or usage update - turn end should flush the buffer
		harness.sendPromptResponse(id, "end_turn")
	}()

	res, err := client.Prompt(context.Background(), "sess-1", "tell me", callbacks)
	if err != nil {
		t.Fatalf("Prompt failed: %v", err)
	}
	if res.StopReason != "end_turn" {
		t.Errorf("expected end_turn, got %s", res.StopReason)
	}
	<-done

	chunksMu.Lock()
	defer chunksMu.Unlock()
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk at turn end, got %d: %v", len(chunks), chunks)
	}
	want := "Single part across multiple streaming deltas."
	if chunks[0] != want {
		t.Errorf("expected %q, got %q", want, chunks[0])
	}
}

func TestClient_Coalesce_PerTurnResetNoBleed(t *testing.T) {
	client, harness, cleanup := newPipeMockHarness(t)
	defer cleanup()

	// Turn 1
	var (
		chunks1Mu sync.Mutex
		chunks1   []string
	)
	callbacks1 := TurnCallbacks{
		OnChunk: func(text string) error {
			chunks1Mu.Lock()
			chunks1 = append(chunks1, text)
			chunks1Mu.Unlock()
			return nil
		},
	}

	done1 := make(chan struct{})
	go func() {
		defer close(done1)
		_, id := harness.readRequest()
		harness.sendChunk("sess-1", "Turn 1 fragment A, ")
		harness.sendChunk("sess-1", "Turn 1 fragment B.")
		harness.sendPromptResponse(id, "end_turn")
	}()

	_, err := client.Prompt(context.Background(), "sess-1", "turn 1", callbacks1)
	if err != nil {
		t.Fatalf("Turn 1 failed: %v", err)
	}
	<-done1

	chunks1Mu.Lock()
	if len(chunks1) != 1 || chunks1[0] != "Turn 1 fragment A, Turn 1 fragment B." {
		t.Fatalf("Turn 1 unexpected chunks: %v", chunks1)
	}
	chunks1Mu.Unlock()

	// Turn 2 on the same client and session
	var (
		chunks2Mu sync.Mutex
		chunks2   []string
	)
	callbacks2 := TurnCallbacks{
		OnChunk: func(text string) error {
			chunks2Mu.Lock()
			chunks2 = append(chunks2, text)
			chunks2Mu.Unlock()
			return nil
		},
	}

	done2 := make(chan struct{})
	go func() {
		defer close(done2)
		_, id := harness.readRequest()
		harness.sendChunk("sess-1", "Turn 2 fresh text.")
		harness.sendPromptResponse(id, "end_turn")
	}()

	_, err = client.Prompt(context.Background(), "sess-1", "turn 2", callbacks2)
	if err != nil {
		t.Fatalf("Turn 2 failed: %v", err)
	}
	<-done2

	chunks2Mu.Lock()
	defer chunks2Mu.Unlock()
	if len(chunks2) != 1 {
		t.Fatalf("expected 1 chunk for Turn 2, got %d: %v", len(chunks2), chunks2)
	}
	if chunks2[0] != "Turn 2 fresh text." {
		t.Errorf("expected 'Turn 2 fresh text.', got %q (buffer bled from Turn 1)", chunks2[0])
	}
}

func TestClient_Coalesce_EmptyDeltaSafety(t *testing.T) {
	client, harness, cleanup := newPipeMockHarness(t)
	defer cleanup()

	var (
		chunksMu sync.Mutex
		chunks   []string
	)
	callbacks := TurnCallbacks{
		OnChunk: func(text string) error {
			chunksMu.Lock()
			chunks = append(chunks, text)
			chunksMu.Unlock()
			return nil
		},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, id := harness.readRequest()
		// Interleave empty deltas
		harness.sendChunk("sess-1", "")
		harness.sendChunk("sess-1", "alpha")
		harness.sendChunk("sess-1", "")
		harness.sendChunk("sess-1", " ")
		harness.sendChunk("sess-1", "")
		harness.sendChunk("sess-1", "beta")
		harness.sendChunk("sess-1", "")
		harness.sendPromptResponse(id, "end_turn")
	}()

	res, err := client.Prompt(context.Background(), "sess-1", "empty test", callbacks)
	if err != nil {
		t.Fatalf("Prompt failed: %v", err)
	}
	if res.StopReason != "end_turn" {
		t.Errorf("expected end_turn, got %s", res.StopReason)
	}
	<-done

	chunksMu.Lock()
	defer chunksMu.Unlock()
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d: %v", len(chunks), chunks)
	}
	if chunks[0] != "alpha beta" {
		t.Errorf("expected 'alpha beta', got %q", chunks[0])
	}

	// Also verify that a turn with ONLY empty deltas emits zero chunks
	var emptyTurnChunks []string
	emptyCallbacks := TurnCallbacks{
		OnChunk: func(text string) error {
			emptyTurnChunks = append(emptyTurnChunks, text)
			return nil
		},
	}

	doneEmpty := make(chan struct{})
	go func() {
		defer close(doneEmpty)
		_, id := harness.readRequest()
		harness.sendChunk("sess-1", "")
		harness.sendChunk("sess-1", "")
		harness.sendPromptResponse(id, "end_turn")
	}()

	_, err = client.Prompt(context.Background(), "sess-1", "all empty", emptyCallbacks)
	if err != nil {
		t.Fatalf("Prompt failed: %v", err)
	}
	<-doneEmpty

	if len(emptyTurnChunks) != 0 {
		t.Errorf("expected 0 chunks for all-empty turn, got %d: %v", len(emptyTurnChunks), emptyTurnChunks)
	}
}

func TestClient_Coalesce_CancelSafety(t *testing.T) {
	client, harness, cleanup := newPipeMockHarness(t)
	defer cleanup()

	var (
		chunksMu sync.Mutex
		chunks   []string
	)
	callbacks := TurnCallbacks{
		OnChunk: func(text string) error {
			chunksMu.Lock()
			chunks = append(chunks, text)
			chunksMu.Unlock()
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	reqReceived := make(chan struct{})
	trailingDone := make(chan struct{})
	promptDone := make(chan error, 1)

	go func() {
		_, _ = harness.readRequest()
		close(reqReceived)

		// Send delta before cancel
		harness.sendChunk("sess-cancel", "pre-cancel text")

		// Wait until cancel is triggered by main goroutine
		<-ctx.Done()

		// Send trailing notifications after cancel has occurred
		harness.sendChunk("sess-cancel", "trailing chunk after cancel")
		harness.sendUpdate("sess-cancel", map[string]any{
			"sessionUpdate": "tool_call",
			"title":         "trailing_tool",
		})
		close(trailingDone)
	}()

	go func() {
		_, err := client.Prompt(ctx, "sess-cancel", "cancel me", callbacks)
		promptDone <- err
	}()

	<-reqReceived
	// Give harness goroutine a moment to send the pre-cancel chunk
	time.Sleep(10 * time.Millisecond)

	cancel()

	err := <-promptDone
	if err == nil {
		t.Fatalf("expected error on cancelled Prompt, got nil")
	}
	<-trailingDone

	// Verify pre-cancel text was flushed cleanly and no data race occurred
	chunksMu.Lock()
	defer chunksMu.Unlock()
	for _, c := range chunks {
		if c == "" {
			t.Errorf("unexpected empty chunk emitted")
		}
	}
}

func TestClient_Permission_DisallowBug(t *testing.T) {
	client, harness, cleanup := newPipeMockHarness(t)
	defer cleanup()

	client.PermissionMode = "approve"

	req := map[string]any{
		"jsonrpc": "2.0",
		"id":      100,
		"method":  "session/request_permission",
		"params": map[string]any{
			"sessionId": "sess-perm-1",
			"toolCall": map[string]any{
				"toolCallId": "call_1",
				"title":      "Delete Everything",
			},
			"options": []map[string]any{
				{
					"optionId": "disallow_once",
					"name":     "Disallow",
					"kind":     "",
				},
			},
		},
	}
	data, _ := json.Marshal(req)
	_, _ = fmt.Fprintf(harness.inWriter, "%s\n", data)

	resp, id := harness.readRequest()
	if id != 100 {
		t.Fatalf("expected id 100, got %d", id)
	}
	result, ok := resp["result"].(map[string]any)
	if !ok {
		t.Fatalf("expected result object, got %v", resp)
	}
	outcome, ok := result["outcome"].(map[string]any)
	if !ok {
		t.Fatalf("expected outcome object, got %v", result)
	}
	if outcome["outcome"] != "cancelled" {
		t.Errorf("expected cancelled for Disallow option in approve mode, got %v", outcome["outcome"])
	}
	if outcome["optionId"] != nil {
		t.Errorf("expected nil optionId, got %v", outcome["optionId"])
	}
}

func TestClient_Permission_KindPriorityAndAllowMatch(t *testing.T) {
	client, harness, cleanup := newPipeMockHarness(t)
	defer cleanup()

	client.PermissionMode = "approve"

	// 1. Kind matches even if Name has nothing to do with allow
	req1 := map[string]any{
		"jsonrpc": "2.0",
		"id":      101,
		"method":  "session/request_permission",
		"params": map[string]any{
			"sessionId": "sess-perm-2",
			"toolCall": map[string]any{
				"toolCallId": "call_2",
				"title":      "Run tool",
			},
			"options": []map[string]any{
				{
					"optionId": "opt_kind_allow",
					"name":     "Proceed With Action",
					"kind":     "allow_once",
				},
			},
		},
	}
	data1, _ := json.Marshal(req1)
	_, _ = fmt.Fprintf(harness.inWriter, "%s\n", data1)

	resp1, _ := harness.readRequest()
	result1 := resp1["result"].(map[string]any)
	outcome1 := result1["outcome"].(map[string]any)
	if outcome1["outcome"] != "selected" || outcome1["optionId"] != "opt_kind_allow" {
		t.Errorf("expected opt_kind_allow selected, got %v", outcome1)
	}

	// 2. Token match on "Allow" when Kind is missing
	req2 := map[string]any{
		"jsonrpc": "2.0",
		"id":      102,
		"method":  "session/request_permission",
		"params": map[string]any{
			"sessionId": "sess-perm-2",
			"toolCall": map[string]any{
				"toolCallId": "call_3",
				"title":      "Run tool 2",
			},
			"options": []map[string]any{
				{
					"optionId": "opt_token_allow",
					"name":     "Allow this once",
					"kind":     "",
				},
			},
		},
	}
	data2, _ := json.Marshal(req2)
	_, _ = fmt.Fprintf(harness.inWriter, "%s\n", data2)

	resp2, _ := harness.readRequest()
	result2 := resp2["result"].(map[string]any)
	outcome2 := result2["outcome"].(map[string]any)
	if outcome2["outcome"] != "selected" || outcome2["optionId"] != "opt_token_allow" {
		t.Errorf("expected opt_token_allow selected, got %v", outcome2)
	}
}

func TestClient_Permission_DecodeError(t *testing.T) {
	client, harness, cleanup := newPipeMockHarness(t)
	defer cleanup()

	var warnings []string
	var warningsMu sync.Mutex
	callbacks := TurnCallbacks{
		OnWarning: func(w string) error {
			warningsMu.Lock()
			warnings = append(warnings, w)
			warningsMu.Unlock()
			return nil
		},
	}

	// Set active turn with callbacks
	client.activeTurnMu.Lock()
	client.activeTurn = &activeTurn{
		sessionID: "sess-err",
		callbacks: callbacks,
	}
	client.activeTurnMu.Unlock()

	// Send request with malformed params (options is string instead of array)
	req := map[string]any{
		"jsonrpc": "2.0",
		"id":      103,
		"method":  "session/request_permission",
		"params": map[string]any{
			"sessionId": "sess-err",
			"options":   "not-an-array",
		},
	}
	data, _ := json.Marshal(req)
	_, _ = fmt.Fprintf(harness.inWriter, "%s\n", data)

	resp, id := harness.readRequest()
	if id != 103 {
		t.Fatalf("expected id 103, got %d", id)
	}
	errObj, ok := resp["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected error object in response, got %v", resp)
	}
	code, _ := errObj["code"].(float64)
	if int(code) != CodeInvalidParams {
		t.Errorf("expected error code %d, got %v", CodeInvalidParams, code)
	}

	warningsMu.Lock()
	defer warningsMu.Unlock()
	for _, w := range warnings {
		if strings.Contains(w, "auto-approved: ") || strings.Contains(w, "auto-denied: ") {
			t.Errorf("unexpected auto-approval/denial warning on decode failure: %q", w)
		}
	}
}

func TestClient_Prompt_ResponseDecodeError(t *testing.T) {
	client, harness, cleanup := newPipeMockHarness(t)
	defer cleanup()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, id := harness.readRequest()
		// Respond with malformed result (not an object matching promptResponse)
		resp := map[string]any{
			"jsonrpc": "2.0",
			"id":      id,
			"result":  "this-is-not-a-valid-json-object-for-prompt-response",
		}
		data, _ := json.Marshal(resp)
		_, _ = fmt.Fprintf(harness.inWriter, "%s\n", data)
	}()

	_, err := client.Prompt(context.Background(), "sess-decode-err", "test prompt", TurnCallbacks{})
	<-done
	if err == nil {
		t.Fatalf("expected Prompt to fail on malformed response, got nil")
	}
	if !strings.Contains(err.Error(), "decoding session/prompt response") {
		t.Errorf("expected error message to mention decoding session/prompt response, got: %v", err)
	}
}
