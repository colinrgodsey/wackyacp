// Command stdiochild is a scripted AgentService-over-stdio stand-in for a
// real wackypub binary, used by serve-mode tests. It speaks the same
// envelope as `wackypub stdio-serve` (gRPC over stdin/stdout, graceful stop
// on stdin EOF) but serves no workspace: behavior is canned and
// deterministic so the dialer, gRPC, and ACP layers can be tested end to
// end without an agent runtime.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"google.golang.org/genai"
	"google.golang.org/grpc"

	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
	"github.com/colinrgodsey/wackypub/pkg/stdio"
)

// scripted behaviors:
//
// prompt "echo: A B C" -> text chunks A, B, C, then a usage chunk (1,2,3)
// prompt "slow"        -> no output until CancelTurn arrives, then the turn ends
// prompt anything else-> a single text chunk "ok"
//
// ReadSession returns three canned turns: a user text, a model thought plus
// a function call, and a user function response for it (id fc1).
//
// ListAgents reports "child<pid>" so a test can tell a fresh child from the
// original.
type service struct {
	agentv1.UnimplementedAgentServiceServer

	mu       sync.Mutex
	cancelCh chan struct{} // closed once when the first CancelTurn arrives
}

func newService() *service { return &service{cancelCh: make(chan struct{})} }

func (s *service) ListAgents(ctx context.Context, req *agentv1.ListAgentsRequest) (*agentv1.ListAgentsResponse, error) {
	return &agentv1.ListAgentsResponse{AgentIds: []string{fmt.Sprintf("child%d", os.Getpid())}}, nil
}

func (s *service) AddAndGenerateTurnStream(req *agentv1.AddAndGenerateTurnStreamRequest, stream agentv1.AgentService_AddAndGenerateTurnStreamServer) error {
	switch {
	case strings.HasPrefix(req.GetUserMessage(), "echo: "):
		for _, tok := range strings.Fields(req.GetUserMessage()) {
			if err := stream.Send(&agentv1.AddAndGenerateTurnStreamResponse{Text: tok}); err != nil {
				return err
			}
		}
		return stream.Send(&agentv1.AddAndGenerateTurnStreamResponse{Usage: &agentv1.TurnUsage{PromptTokens: 1, CompletionTokens: 2, TotalTokens: 3}})
	case req.GetUserMessage() == "slow":
		select {
		case <-s.cancelCh:
			return nil
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	default:
		return stream.Send(&agentv1.AddAndGenerateTurnStreamResponse{Text: "ok"})
	}
}

func (s *service) ReadSession(ctx context.Context, req *agentv1.ReadSessionRequest) (*agentv1.ReadSessionResponse, error) {
	turns := []*agentv1.SessionTurn{
		{Seq: 1, Role: "user", ContentJson: contentJSON(genai.Content{
			Role:  "user",
			Parts: []*genai.Part{{Text: "hello"}},
		})},
		{Seq: 2, Role: "model", ContentJson: contentJSON(genai.Content{
			Role: "model",
			Parts: []*genai.Part{
				{Text: "thinking out loud", Thought: true},
				{FunctionCall: &genai.FunctionCall{ID: "fc1", Name: "run_command", Args: map[string]any{"cmd": "ls"}}},
			},
		})},
		{Seq: 3, Role: "user", ContentJson: contentJSON(genai.Content{
			Role:  "user",
			Parts: []*genai.Part{{FunctionResponse: &genai.FunctionResponse{ID: "fc1", Name: "run_command", Response: map[string]any{"output": "ok output"}}}},
		})},
	}
	return &agentv1.ReadSessionResponse{Turns: turns}, nil
}

func (s *service) CancelTurn(ctx context.Context, req *agentv1.CancelTurnRequest) (*agentv1.CancelTurnResponse, error) {
	s.mu.Lock()
	select {
	case <-s.cancelCh:
	default:
		close(s.cancelCh)
	}
	s.mu.Unlock()
	return &agentv1.CancelTurnResponse{}, nil
}

func contentJSON(c genai.Content) string {
	data, _ := json.Marshal(c)
	return string(data)
}

func main() {
	if len(os.Args) > 1 && os.Args[1] != "stdio-serve" {
		fmt.Fprintf(os.Stderr, "stdiochild: unexpected argument %q (expected stdio-serve)\n", os.Args[1])
		os.Exit(2)
	}
	svc := newService()
	gs := grpc.NewServer()
	agentv1.RegisterAgentServiceServer(gs, svc)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	conn := stdio.NewConn(os.Stdin, os.Stdout)
	if err := stdio.ServeContext(ctx, gs, conn); err != nil {
		fmt.Fprintf(os.Stderr, "stdiochild: %v\n", err)
		os.Exit(1)
	}
}
