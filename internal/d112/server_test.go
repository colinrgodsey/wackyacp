package d112

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/colinrgodsey/wackyacp/internal/acp"
	"github.com/colinrgodsey/wackypub/pkg/agent"
	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type mockACPDriver struct {
	chunks            []string
	warnings          []string
	toolCalls         []*agentv1.ToolCall
	toolCallUpdates   []*agentv1.ToolCallUpdate
	usage             acp.UsageMetrics
	err               error
	promptHook        func(ctx context.Context)
	cancelCh          chan string
	canceledSessionID string
	lastConfigOpts    json.RawMessage
	setConfigResp     json.RawMessage
	setConfigErr      error
	lastSetConfigCall struct {
		sessionID string
		configID  string
		value     string
	}
	promptCallCount int
}

func (m *mockACPDriver) Prompt(ctx context.Context, sessionID, promptText string, callbacks acp.TurnCallbacks) (*acp.PromptResult, error) {
	m.promptCallCount++
	if m.promptHook != nil {
		m.promptHook(ctx)
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if m.err != nil {
		return nil, m.err
	}
	for _, c := range m.chunks {
		if callbacks.OnChunk != nil {
			_ = callbacks.OnChunk(c)
		}
	}
	for _, tc := range m.toolCalls {
		if callbacks.OnToolCall != nil {
			_ = callbacks.OnToolCall(tc)
		}
	}
	for _, tcu := range m.toolCallUpdates {
		if callbacks.OnToolCallUpdate != nil {
			_ = callbacks.OnToolCallUpdate(tcu)
		}
	}
	for _, w := range m.warnings {
		if callbacks.OnWarning != nil {
			_ = callbacks.OnWarning(w)
		}
	}
	return &acp.PromptResult{
		StopReason: "end_turn",
		Usage:      m.usage,
	}, nil
}

func (m *mockACPDriver) Cancel(sessionID string) error {
	m.canceledSessionID = sessionID
	if m.cancelCh != nil {
		m.cancelCh <- sessionID
	}
	return nil
}

func (m *mockACPDriver) SetConfigOption(ctx context.Context, sessionID, configID, value string) (json.RawMessage, error) {
	m.lastSetConfigCall.sessionID = sessionID
	m.lastSetConfigCall.configID = configID
	m.lastSetConfigCall.value = value
	if m.setConfigErr != nil {
		return nil, m.setConfigErr
	}
	return m.setConfigResp, nil
}

func (m *mockACPDriver) LastConfigOptions() json.RawMessage {
	return m.lastConfigOpts
}

func setupTestServer(driver ACPDriver) (agentv1.AgentServiceClient, func()) {
	return setupTestServerWithFolder(driver, "/path/to/agent")
}

func setupTestServerWithFolder(driver ACPDriver, folder string) (agentv1.AgentServiceClient, func()) {
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	d112Srv := NewServer(driver, "test-session-123", folder)
	agentv1.RegisterAgentServiceServer(srv, d112Srv)

	go func() {
		_ = srv.Serve(lis)
	}()

	conn, _ := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
	)

	client := agentv1.NewAgentServiceClient(conn)
	cleanup := func() {
		_ = conn.Close()
		srv.Stop()
		_ = lis.Close()
	}
	return client, cleanup
}

func TestD112_AddAndGenerateTurnStream(t *testing.T) {
	driver := &mockACPDriver{
		chunks:   []string{"Hello ", "world!"},
		warnings: []string{"warning 1"},
		usage: acp.UsageMetrics{
			PromptTokens:     10,
			CompletionTokens: 20,
			TotalTokens:      30,
			Backend:          "wackyacp",
		},
	}

	client, cleanup := setupTestServer(driver)
	defer cleanup()

	stream, err := client.AddAndGenerateTurnStream(context.Background(), &agentv1.AddAndGenerateTurnStreamRequest{
		AgentId:     "agent-test",
		UserMessage: "hi",
	})
	if err != nil {
		t.Fatalf("AddAndGenerateTurnStream failed: %v", err)
	}

	var receivedChunks []string
	var receivedWarnings []string
	var receivedUsage *agentv1.TurnUsage

	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("stream.Recv failed: %v", err)
		}
		if resp.Text != "" {
			receivedChunks = append(receivedChunks, resp.Text)
		}
		if resp.Warning != "" {
			receivedWarnings = append(receivedWarnings, resp.Warning)
		}
		if resp.Usage != nil {
			receivedUsage = resp.Usage
		}
	}

	if len(receivedChunks) != 2 || receivedChunks[0] != "Hello " || receivedChunks[1] != "world!" {
		t.Errorf("unexpected chunks: %v", receivedChunks)
	}
	if len(receivedWarnings) != 1 || receivedWarnings[0] != "warning 1" {
		t.Errorf("unexpected warnings: %v", receivedWarnings)
	}
	if receivedUsage == nil || receivedUsage.TotalTokens != 30 {
		t.Errorf("unexpected usage: %+v", receivedUsage)
	}
}

func TestD112_GenerateTurn(t *testing.T) {
	driver := &mockACPDriver{
		chunks: []string{"Full ", "response"},
		usage: acp.UsageMetrics{
			TotalTokens: 50,
			Backend:     "wackyacp",
		},
	}

	client, cleanup := setupTestServer(driver)
	defer cleanup()

	resp, err := client.GenerateTurn(context.Background(), &agentv1.GenerateTurnRequest{
		AgentId: "agent-test",
	})
	if err != nil {
		t.Fatalf("GenerateTurn failed: %v", err)
	}

	if resp.Text != "Full response" {
		t.Errorf("expected 'Full response', got: %q", resp.Text)
	}
	if resp.Usage == nil || resp.Usage.TotalTokens != 50 {
		t.Errorf("expected usage 50, got: %+v", resp.Usage)
	}
}

func TestD112_UnimplementedMethods(t *testing.T) {
	driver := &mockACPDriver{}
	client, cleanup := setupTestServer(driver)
	defer cleanup()

	ctx := context.Background()

	// AddMedia should be Unimplemented
	_, err := client.AddMedia(ctx, &agentv1.AddMediaRequest{
		AgentId: "agent-test",
	})
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("expected codes.Unimplemented for AddMedia, got: %v", err)
	}

	// StripSignatures should be Unimplemented
	_, err = client.StripSignatures(ctx, &agentv1.StripSignaturesRequest{
		AgentId: "agent-test",
	})
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("expected codes.Unimplemented for StripSignatures, got: %v", err)
	}

	// CompactSession should be Unimplemented
	_, err = client.CompactSession(ctx, &agentv1.CompactSessionRequest{
		AgentId: "agent-test",
	})
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("expected codes.Unimplemented for CompactSession, got: %v", err)
	}
}

func TestD112_ErrorMapping(t *testing.T) {
	// 1. Harness died error
	driver1 := &mockACPDriver{err: acp.ErrHarnessDied}
	client1, cleanup1 := setupTestServer(driver1)
	defer cleanup1()

	_, err := client1.GenerateTurn(context.Background(), &agentv1.GenerateTurnRequest{
		AgentId: "agent-test",
	})
	if status.Code(err) != codes.Unavailable {
		t.Errorf("expected codes.Unavailable for dead harness, got: %v", err)
	}

	// 2. ACP turn error
	driver2 := &mockACPDriver{err: acp.ErrTurnFailed}
	client2, cleanup2 := setupTestServer(driver2)
	defer cleanup2()

	_, err = client2.GenerateTurn(context.Background(), &agentv1.GenerateTurnRequest{
		AgentId: "agent-test",
	})
	if status.Code(err) != codes.Internal {
		t.Errorf("expected codes.Internal for ACP turn error, got: %v", err)
	}
}

func TestD112_StreamCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancelCh := make(chan string, 1)
	driver := &mockACPDriver{
		cancelCh: cancelCh,
		promptHook: func(promptCtx context.Context) {
			cancel()
			<-promptCtx.Done()
		},
	}

	client, cleanup := setupTestServer(driver)
	defer cleanup()

	stream, err := client.AddAndGenerateTurnStream(ctx, &agentv1.AddAndGenerateTurnStreamRequest{
		AgentId:     "agent-test",
		UserMessage: "hello",
	})
	if err != nil {
		t.Fatalf("AddAndGenerateTurnStream failed: %v", err)
	}

	_, err = stream.Recv()
	if status.Code(err) != codes.Canceled {
		t.Errorf("expected codes.Canceled, got: %v", err)
	}

	select {
	case sid := <-cancelCh:
		if sid != "test-session-123" {
			t.Errorf("expected driver.Cancel to be called with test-session-123, got: %q", sid)
		}
	case <-time.After(1 * time.Second):
		t.Errorf("timed out waiting for driver.Cancel to be called")
	}
}

// TestD112_AsideQuestion_Unsupported pins Colin's decision: the ACP bridge cannot fork
// accumulated context, so AsideQuestion responds with codes.Unimplemented and an explicit
// message - callers get a structured error and can fall back to the local agent path.
func TestD112_AsideQuestion_Unsupported(t *testing.T) {
	driver := &mockACPDriver{chunks: []string{"never reached"}}
	client, cleanup := setupTestServer(driver)
	defer cleanup()

	_, err := client.AsideQuestion(context.Background(), &agentv1.AsideQuestionRequest{
		AgentId:  "agent-test",
		Question: "what is the state?",
	})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("expected codes.Unimplemented for ACP-bridged aside, got: %v", err)
	}
	if !strings.Contains(err.Error(), "aside") {
		t.Fatalf("error should mention aside explicitly, got: %v", err)
	}
}

// TestD112_SecondConcurrentTurnBusy pins the server-side half of
// bugs/wackypub/bridge-concurrent-prompt-lock: a second prompt turn while one is
// in flight on the same bridge process gets a structured ResourceExhausted error,
// never a corrupted stream / died-bridge failure.
func TestD112_SecondConcurrentTurnBusy(t *testing.T) {
	release := make(chan struct{})
	driver := &mockACPDriver{
		chunks: []string{"first turn"},
		promptHook: func(ctx context.Context) {
			<-release
		},
	}
	client, cleanup := setupTestServer(driver)
	defer cleanup()

	// Start turn 1; it blocks inside the driver prompt hook until released.
	first, err := client.AddAndGenerateTurnStream(context.Background(), &agentv1.AddAndGenerateTurnStreamRequest{
		AgentId:     "agent-test",
		UserMessage: "hello one",
	})
	if err != nil {
		t.Fatalf("first AddAndGenerateTurnStream failed: %v", err)
	}

	// Allow the first stream to reach the in-flight region, then fire the second.
	time.Sleep(50 * time.Millisecond)
	second, err := client.AddAndGenerateTurnStream(context.Background(), &agentv1.AddAndGenerateTurnStreamRequest{
		AgentId:     "agent-test",
		UserMessage: "hello two",
	})
	if err == nil {
		_, recvErr := second.Recv()
		if recvErr == nil {
			t.Fatal("expected second concurrent turn to be refused")
		}
		if status.Code(recvErr) != codes.ResourceExhausted {
			t.Errorf("expected codes.ResourceExhausted on second concurrent turn, got: %v", recvErr)
		}
	}

	// Release turn 1 and consume it normally.
	close(release)
	for {
		_, recvErr := first.Recv()
		if recvErr != nil {
			if recvErr != io.EOF {
				t.Fatalf("first turn stream error after release: %v", recvErr)
			}
			break
		}
	}

	// After turn 1 completes, a fresh turn must succeed (slot released).
	driver.promptHook = nil
	third, err := client.AddAndGenerateTurnStream(context.Background(), &agentv1.AddAndGenerateTurnStreamRequest{
		AgentId:     "agent-test",
		UserMessage: "hello three",
	})
	if err != nil {
		t.Fatalf("third AddAndGenerateTurnStream failed after turn 1: %v", err)
	}
	for {
		_, recvErr := third.Recv()
		if recvErr != nil {
			if recvErr != io.EOF {
				t.Fatalf("third turn stream error: %v", recvErr)
			}
			break
		}
	}
}

func TestD112_ToolCallStreaming(t *testing.T) {
	tc := &agentv1.ToolCall{
		CallId:      "tc-1",
		ToolName:    "bash",
		ArgsSummary: "command=ls",
	}
	tcu := &agentv1.ToolCallUpdate{
		CallId:      "tc-1",
		ToolName:    "bash",
		Status:      "completed",
		ResultBytes: 10,
		ResultHead:  "output",
	}

	driver := &mockACPDriver{
		chunks:          []string{"Calling tool..."},
		toolCalls:       []*agentv1.ToolCall{tc},
		toolCallUpdates: []*agentv1.ToolCallUpdate{tcu},
	}

	client, cleanup := setupTestServer(driver)
	defer cleanup()

	t.Run("GenerateTurnStream", func(t *testing.T) {
		stream, err := client.GenerateTurnStream(context.Background(), &agentv1.GenerateTurnStreamRequest{
			AgentId: "agent-test",
		})
		if err != nil {
			t.Fatalf("GenerateTurnStream failed: %v", err)
		}
		var gotCall *agentv1.ToolCall
		var gotUpdate *agentv1.ToolCallUpdate
		for {
			resp, err := stream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				t.Fatalf("Recv failed: %v", err)
			}
			if resp.ToolCall != nil {
				gotCall = resp.ToolCall
			}
			if resp.ToolCallUpdate != nil {
				gotUpdate = resp.ToolCallUpdate
			}
		}
		if gotCall == nil || gotCall.CallId != "tc-1" || gotCall.ToolName != "bash" {
			t.Errorf("unexpected ToolCall: %+v", gotCall)
		}
		if gotUpdate == nil || gotUpdate.CallId != "tc-1" || gotUpdate.Status != "completed" {
			t.Errorf("unexpected ToolCallUpdate: %+v", gotUpdate)
		}
	})

	t.Run("AddAndGenerateTurnStream", func(t *testing.T) {
		stream, err := client.AddAndGenerateTurnStream(context.Background(), &agentv1.AddAndGenerateTurnStreamRequest{
			AgentId:     "agent-test",
			UserMessage: "run",
		})
		if err != nil {
			t.Fatalf("AddAndGenerateTurnStream failed: %v", err)
		}
		var gotCall *agentv1.ToolCall
		var gotUpdate *agentv1.ToolCallUpdate
		for {
			resp, err := stream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				t.Fatalf("Recv failed: %v", err)
			}
			if resp.ToolCall != nil {
				gotCall = resp.ToolCall
			}
			if resp.ToolCallUpdate != nil {
				gotUpdate = resp.ToolCallUpdate
			}
		}
		if gotCall == nil || gotCall.CallId != "tc-1" || gotCall.ToolName != "bash" {
			t.Errorf("unexpected ToolCall: %+v", gotCall)
		}
		if gotUpdate == nil || gotUpdate.CallId != "tc-1" || gotUpdate.Status != "completed" {
			t.Errorf("unexpected ToolCallUpdate: %+v", gotUpdate)
		}
	})
}

func TestAddAndGenerateTurnStream_ModelQuery(t *testing.T) {
	driver := &mockACPDriver{
		lastConfigOpts: json.RawMessage(`[
			{
				"id": "model",
				"currentValue": "sonnet",
				"options": [
					{"value": "sonnet", "name": "Claude 3.5 Sonnet"},
					{"value": "opus", "name": "Claude 3 Opus"}
				]
			}
		]`),
	}
	client, cleanup := setupTestServer(driver)
	defer cleanup()

	stream, err := client.AddAndGenerateTurnStream(context.Background(), &agentv1.AddAndGenerateTurnStreamRequest{
		AgentId:     "test-agent",
		UserMessage: "/model",
	})
	if err != nil {
		t.Fatalf("AddAndGenerateTurnStream failed: %v", err)
	}

	var textParts []string
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("stream recv failed: %v", err)
		}
		if resp.Text != "" {
			textParts = append(textParts, resp.Text)
		}
	}

	if driver.promptCallCount != 0 {
		t.Errorf("driver.Prompt should not be called for /model query, got %d calls", driver.promptCallCount)
	}

	fullText := strings.Join(textParts, "")
	if !strings.Contains(fullText, "Current model: **sonnet** (Claude 3.5 Sonnet)") {
		t.Errorf("expected current model in output, got: %s", fullText)
	}
	if !strings.Contains(fullText, "• **opus** — Claude 3 Opus") {
		t.Errorf("expected opus option in output, got: %s", fullText)
	}
}

func TestAddAndGenerateTurnStream_ModelSetSuccess(t *testing.T) {
	driver := &mockACPDriver{
		setConfigResp: json.RawMessage(`[
			{
				"id": "model",
				"currentValue": "opus",
				"options": [
					{"value": "sonnet", "name": "Claude 3.5 Sonnet"},
					{"value": "opus", "name": "Claude 3 Opus"}
				]
			}
		]`),
	}
	client, cleanup := setupTestServer(driver)
	defer cleanup()

	stream, err := client.AddAndGenerateTurnStream(context.Background(), &agentv1.AddAndGenerateTurnStreamRequest{
		AgentId:     "test-agent",
		UserMessage: "/model opus",
	})
	if err != nil {
		t.Fatalf("AddAndGenerateTurnStream failed: %v", err)
	}

	var textParts []string
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("stream recv failed: %v", err)
		}
		if resp.Text != "" {
			textParts = append(textParts, resp.Text)
		}
	}

	if driver.promptCallCount != 0 {
		t.Errorf("driver.Prompt should not be called for /model set, got %d calls", driver.promptCallCount)
	}
	if driver.lastSetConfigCall.sessionID != "test-session-123" {
		t.Errorf("sessionID = %q, want test-session-123", driver.lastSetConfigCall.sessionID)
	}
	if driver.lastSetConfigCall.configID != "model" {
		t.Errorf("configID = %q, want model", driver.lastSetConfigCall.configID)
	}
	if driver.lastSetConfigCall.value != "opus" {
		t.Errorf("value = %q, want opus", driver.lastSetConfigCall.value)
	}

	fullText := strings.Join(textParts, "")
	if fullText != "Model set to **opus** (Claude 3 Opus)." {
		t.Errorf("unexpected output: %q", fullText)
	}
}

func TestAddAndGenerateTurnStream_ModelSetError(t *testing.T) {
	driver := &mockACPDriver{
		setConfigErr: &acp.ConfigOptionError{
			Code:    -32602,
			Message: "unknown model: bad-model",
		},
	}
	client, cleanup := setupTestServer(driver)
	defer cleanup()

	stream, err := client.AddAndGenerateTurnStream(context.Background(), &agentv1.AddAndGenerateTurnStreamRequest{
		AgentId:     "test-agent",
		UserMessage: "/model bad-model",
	})
	if err != nil {
		t.Fatalf("AddAndGenerateTurnStream failed: %v", err)
	}

	var textParts []string
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("stream recv should not fail with RPC error on set error, got: %v", err)
		}
		if resp.Text != "" {
			textParts = append(textParts, resp.Text)
		}
	}

	fullText := strings.Join(textParts, "")
	if !strings.Contains(fullText, "Failed to set model to \"bad-model\": unknown model: bad-model") {
		t.Errorf("expected error message in text stream, got: %q", fullText)
	}
}

func TestAddAndGenerateTurn_ModelUnary(t *testing.T) {
	driver := &mockACPDriver{
		lastConfigOpts: json.RawMessage(`[
			{
				"id": "model",
				"currentValue": "sonnet",
				"options": [
					{"value": "sonnet", "name": "Claude 3.5 Sonnet"}
				]
			}
		]`),
		setConfigResp: json.RawMessage(`[
			{
				"id": "model",
				"currentValue": "sonnet",
				"options": [
					{"value": "sonnet", "name": "Claude 3.5 Sonnet"}
				]
			}
		]`),
	}
	client, cleanup := setupTestServer(driver)
	defer cleanup()

	// Query via unary AddAndGenerateTurn
	respQuery, err := client.AddAndGenerateTurn(context.Background(), &agentv1.AddAndGenerateTurnRequest{
		AgentId:     "test-agent",
		UserMessage: "/model",
	})
	if err != nil {
		t.Fatalf("AddAndGenerateTurn query failed: %v", err)
	}
	if !strings.Contains(respQuery.Text, "Current model: **sonnet** (Claude 3.5 Sonnet)") {
		t.Errorf("unexpected query text: %q", respQuery.Text)
	}

	// Set via unary AddAndGenerateTurn
	respSet, err := client.AddAndGenerateTurn(context.Background(), &agentv1.AddAndGenerateTurnRequest{
		AgentId:     "test-agent",
		UserMessage: "/model set sonnet",
	})
	if err != nil {
		t.Fatalf("AddAndGenerateTurn set failed: %v", err)
	}
	if respSet.Text != "Model set to **sonnet** (Claude 3.5 Sonnet)." {
		t.Errorf("unexpected set text: %q", respSet.Text)
	}

	if driver.promptCallCount != 0 {
		t.Errorf("driver.Prompt should not be called, got %d calls", driver.promptCallCount)
	}
}

func TestD112_Scratchpad_FullLifecycle(t *testing.T) {
	tmpWs := t.TempDir()
	agentDir := filepath.Join(tmpWs, "testagent")
	if err := os.MkdirAll(agentDir, 0755); err != nil {
		t.Fatalf("mkdir agentDir: %v", err)
	}

	driver := &mockACPDriver{}
	client, cleanup := setupTestServerWithFolder(driver, agentDir)
	defer cleanup()

	ctx := context.Background()

	// 1. Create text scratchpad
	createResp, err := client.CreateScratchpad(ctx, &agentv1.CreateScratchpadRequest{
		AgentId:   "testagent",
		Text:      "line 1\nline 2\nline 3\nline 4\nline 5\n",
		CreatedBy: "test",
	})
	if err != nil {
		t.Fatalf("CreateScratchpad failed: %v", err)
	}
	entry1 := createResp.GetEntry()
	if entry1.GetEntryId() == "" {
		t.Fatalf("expected non-empty EntryId")
	}
	if entry1.GetLines() != 5 {
		t.Errorf("expected 5 lines, got %d", entry1.GetLines())
	}

	// 2. GetScratchpad full
	getResp, err := client.GetScratchpad(ctx, &agentv1.GetScratchpadRequest{
		AgentId: "testagent",
		EntryId: entry1.GetEntryId(),
	})
	if err != nil {
		t.Fatalf("GetScratchpad failed: %v", err)
	}
	if getResp.GetText() != "line 1\nline 2\nline 3\nline 4\nline 5\n" {
		t.Errorf("unexpected text: %q", getResp.GetText())
	}

	// 3. GetScratchpad paginated (skip 1 line, take 2 lines)
	skip := int32(1)
	num := int32(2)
	getPagedResp, err := client.GetScratchpad(ctx, &agentv1.GetScratchpadRequest{
		AgentId:   "testagent",
		EntryId:   entry1.GetEntryId(),
		SkipLines: &skip,
		NumLines:  &num,
	})
	if err != nil {
		t.Fatalf("GetScratchpad paginated failed: %v", err)
	}
	if getPagedResp.GetText() != "line 2\nline 3" {
		t.Errorf("unexpected paginated text: %q", getPagedResp.GetText())
	}

	// 4. Create second entry with macro referencing entry1 (D90 macro expansion)
	macroText := fmt.Sprintf("prefix\n<SCRATCHPAD_DATA id=\"%s\" skip_lines=\"1\" num_lines=\"1\" />\nsuffix\n", entry1.GetEntryId())
	createMacroResp, err := client.CreateScratchpad(ctx, &agentv1.CreateScratchpadRequest{
		AgentId: "testagent",
		Text:    macroText,
	})
	if err != nil {
		t.Fatalf("CreateScratchpad with macro failed: %v", err)
	}
	entry2 := createMacroResp.GetEntry()
	getMacroResp, err := client.GetScratchpad(ctx, &agentv1.GetScratchpadRequest{
		AgentId: "testagent",
		EntryId: entry2.GetEntryId(),
	})
	if err != nil {
		t.Fatalf("GetScratchpad macro entry failed: %v", err)
	}
	expectedMacroExpanded := "prefix\nline 2\nsuffix\n"
	if getMacroResp.GetText() != expectedMacroExpanded {
		t.Errorf("macro expansion failed, want %q got %q", expectedMacroExpanded, getMacroResp.GetText())
	}

	// 5. SearchScratchpad
	searchResp, err := client.SearchScratchpad(ctx, &agentv1.SearchScratchpadRequest{
		AgentId: "testagent",
		EntryId: entry1.GetEntryId(),
		Query:   "line 3",
	})
	if err != nil {
		t.Fatalf("SearchScratchpad failed: %v", err)
	}
	if searchResp.GetTotalMatches() != 1 {
		t.Errorf("expected 1 match, got %d", searchResp.GetTotalMatches())
	}
	if len(searchResp.GetMatches()) != 1 || searchResp.GetMatches()[0].GetLine() != 3 {
		t.Errorf("unexpected match: %+v", searchResp.GetMatches())
	}

	// 6. ListScratchpads
	listResp, err := client.ListScratchpads(ctx, &agentv1.ListScratchpadsRequest{
		AgentId: "testagent",
	})
	if err != nil {
		t.Fatalf("ListScratchpads failed: %v", err)
	}
	if listResp.GetTotalEntries() != 2 {
		t.Errorf("expected 2 entries, got %d", listResp.GetTotalEntries())
	}

	// 7. DiffScratchpadEntries
	diffResp, err := client.DiffScratchpadEntries(ctx, &agentv1.DiffScratchpadEntriesRequest{
		AgentId:       "testagent",
		BeforeEntryId: entry1.GetEntryId(),
		AfterEntryId:  entry2.GetEntryId(),
	})
	if err != nil {
		t.Fatalf("DiffScratchpadEntries failed: %v", err)
	}
	if diffResp.GetDiff() == "" {
		t.Errorf("expected non-empty diff")
	}

	// 8. CreateBinaryScratchpad and ensure GetScratchpad rejects it
	binData := []byte{0x00, 0x01, 0x02, 0x03, 'P', 'N', 'G'}
	createBinResp, err := client.CreateScratchpad(ctx, &agentv1.CreateScratchpadRequest{
		AgentId:  "testagent",
		Data:     binData,
		MimeType: "application/octet-stream",
	})
	if err != nil {
		t.Fatalf("CreateBinaryScratchpad failed: %v", err)
	}
	binEntry := createBinResp.GetEntry()
	if !binEntry.GetIsBinary() {
		t.Errorf("expected IsBinary=true")
	}
	_, err = client.GetScratchpad(ctx, &agentv1.GetScratchpadRequest{
		AgentId: "testagent",
		EntryId: binEntry.GetEntryId(),
	})
	if err == nil {
		t.Errorf("expected GetScratchpad to reject binary entry, got nil error")
	}

	// 9. DeleteScratchpad
	_, err = client.DeleteScratchpad(ctx, &agentv1.DeleteScratchpadRequest{
		AgentId: "testagent",
		EntryId: entry1.GetEntryId(),
	})
	if err != nil {
		t.Fatalf("DeleteScratchpad failed: %v", err)
	}
	// Verify deletion
	_, err = client.GetScratchpad(ctx, &agentv1.GetScratchpadRequest{
		AgentId: "testagent",
		EntryId: entry1.GetEntryId(),
	})
	if err == nil {
		t.Errorf("expected GetScratchpad to fail on deleted entry, got nil error")
	}
}

func TestD112_Scratchpad_CrossAgent(t *testing.T) {
	tmpWs := t.TempDir()
	agentA := filepath.Join(tmpWs, "agentA")
	agentB := filepath.Join(tmpWs, "agentB")
	agentC := filepath.Join(tmpWs, "agentC")
	_ = os.MkdirAll(agentA, 0755)
	_ = os.MkdirAll(agentB, 0755)
	_ = os.MkdirAll(agentC, 0755)

	driver := &mockACPDriver{}
	// Server is wired for agentA
	client, cleanup := setupTestServerWithFolder(driver, agentA)
	defer cleanup()

	ctx := context.Background()

	// 1. Same-agent access always allowed without allowlist
	_, err := client.CreateScratchpad(ctx, &agentv1.CreateScratchpadRequest{
		AgentId: "agentA",
		Text:    "own deposit",
	})
	if err != nil {
		t.Fatalf("CreateScratchpad for same agent failed: %v", err)
	}

	// 2. Cross-agent access without allowlist must be DENIED (codes.PermissionDenied)
	_, err = client.CreateScratchpad(ctx, &agentv1.CreateScratchpadRequest{
		AgentId: "agentB",
		Text:    "unauthorized deposit",
	})
	if err == nil {
		t.Fatalf("expected CreateScratchpad for agentB to fail with no allowlist, got nil")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied for agentB with no allowlist, got: %v (code: %v)", err, status.Code(err))
	}

	_, err = client.GetScratchpad(ctx, &agentv1.GetScratchpadRequest{
		AgentId: "agentB",
		EntryId: "1234",
	})
	if err == nil {
		t.Fatalf("expected GetScratchpad for agentB to fail with no allowlist, got nil")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied for GetScratchpad, got: %v", err)
	}

	_, err = client.ListScratchpads(ctx, &agentv1.ListScratchpadsRequest{
		AgentId: "agentB",
	})
	if err == nil {
		t.Fatalf("expected ListScratchpads for agentB to fail with no allowlist, got nil")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied for ListScratchpads, got: %v", err)
	}

	// 3. Add allowlist to calling agent folder (agentA) allowing only agentB
	allowlistPath := filepath.Join(agentA, agent.AllowedAgentsFile)
	if err := os.WriteFile(allowlistPath, []byte("# allowlist\nagentB\n"), 0644); err != nil {
		t.Fatalf("failed to write %s: %v", agent.AllowedAgentsFile, err)
	}

	// 4. Cross-agent access to allowlisted target agentB must PASS
	createResp, err := client.CreateScratchpad(ctx, &agentv1.CreateScratchpadRequest{
		AgentId: "agentB",
		Text:    "deposit from agentB",
	})
	if err != nil {
		t.Fatalf("CreateScratchpad for allowlisted agentB failed: %v", err)
	}

	getResp, err := client.GetScratchpad(ctx, &agentv1.GetScratchpadRequest{
		AgentId: "agentB",
		EntryId: createResp.GetEntry().GetEntryId(),
	})
	if err != nil {
		t.Fatalf("GetScratchpad for allowlisted agentB failed: %v", err)
	}
	if getResp.GetText() != "deposit from agentB" {
		t.Errorf("expected 'deposit from agentB', got %q", getResp.GetText())
	}

	// 5. Cross-agent DiffScratchpadEntries for allowlisted agentB must PASS
	createResp2, err := client.CreateScratchpad(ctx, &agentv1.CreateScratchpadRequest{
		AgentId: "agentB",
		Text:    "deposit 2 from agentB",
	})
	if err != nil {
		t.Fatalf("CreateScratchpad 2 for allowlisted agentB failed: %v", err)
	}

	diffResp, err := client.DiffScratchpadEntries(ctx, &agentv1.DiffScratchpadEntriesRequest{
		AgentId:       "agentB",
		BeforeEntryId: createResp.GetEntry().GetEntryId(),
		AfterEntryId:  createResp2.GetEntry().GetEntryId(),
	})
	if err != nil {
		t.Fatalf("DiffScratchpadEntries for allowlisted agentB failed: %v", err)
	}
	if diffResp.GetDiff() == "" {
		t.Errorf("expected non-empty diff, got empty")
	}

	// 6. Cross-agent access to non-allowlisted target agentC must be DENIED
	_, err = client.CreateScratchpad(ctx, &agentv1.CreateScratchpadRequest{
		AgentId: "agentC",
		Text:    "unauthorized deposit to agentC",
	})
	if err == nil {
		t.Fatalf("expected CreateScratchpad for non-allowlisted agentC to fail, got nil")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied for non-allowlisted agentC, got: %v (code: %v)", err, status.Code(err))
	}

	_, err = client.DiffScratchpadEntries(ctx, &agentv1.DiffScratchpadEntriesRequest{
		AgentId:       "agentC",
		BeforeEntryId: "1234",
		AfterEntryId:  "5678",
	})
	if err == nil {
		t.Fatalf("expected DiffScratchpadEntries for non-allowlisted agentC to fail, got nil")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied for non-allowlisted agentC diff, got: %v (code: %v)", err, status.Code(err))
	}
}
