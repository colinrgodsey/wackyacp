package d112

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/colinrgodsey/wackyacp/internal/acp"
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
	usage             acp.UsageMetrics
	err               error
	promptHook        func(ctx context.Context)
	cancelCh          chan string
	canceledSessionID string
}

func (m *mockACPDriver) Prompt(ctx context.Context, sessionID, promptText string, callbacks acp.TurnCallbacks) (*acp.PromptResult, error) {
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

func setupTestServer(driver ACPDriver) (agentv1.AgentServiceClient, func()) {
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	d112Srv := NewServer(driver, "test-session-123", "/path/to/agent")
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
