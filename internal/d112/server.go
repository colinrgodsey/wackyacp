package d112

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"

	"github.com/colinrgodsey/wackyacp/internal/acp"
	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
	"github.com/colinrgodsey/wackypub/pkg/stdio"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ACPDriver abstracts driving an ACP prompt turn for the D112 server.
type ACPDriver interface {
	Prompt(ctx context.Context, sessionID, promptText string, callbacks acp.TurnCallbacks) (*acp.PromptResult, error)
	Cancel(sessionID string) error
}

// Server implements agentv1.AgentServiceServer for wackyacp.
type Server struct {
	agentv1.UnimplementedAgentServiceServer
	driver      ACPDriver
	sessionID   string
	agentFolder string

	// turnMu + turnsInFlight serialize prompt turns served by THIS bridge process. The ACP
	// harness runs one conversation at a time; a second concurrent prompt would collide on
	// the driver session, so it is refused with a structured BUSY error instead of being
	// let through to corrupt the stream (bugs/wackypub/bridge-concurrent-prompt-lock).
	turnMu        sync.Mutex
	turnsInFlight int
}

// NewServer constructs a new D112 Server backed by an ACP driver.
func NewServer(driver ACPDriver, sessionID, agentFolder string) *Server {
	return &Server{
		driver:      driver,
		sessionID:   sessionID,
		agentFolder: agentFolder,
	}
}

// beginTurn reserves the single in-flight slot. Returns false if another turn is active.
func (s *Server) beginTurn() bool {
	s.turnMu.Lock()
	defer s.turnMu.Unlock()
	if s.turnsInFlight > 0 {
		return false
	}
	s.turnsInFlight++
	return true
}

// endTurn releases the in-flight slot.
func (s *Server) endTurn() {
	s.turnMu.Lock()
	defer s.turnMu.Unlock()
	s.turnsInFlight--
}

func errTurnBusy() error {
	return status.Error(codes.ResourceExhausted, "another prompt turn is already in flight for this agent; retry after it completes")
}

func toProtoUsage(u acp.UsageMetrics) *agentv1.TurnUsage {
	if u.TotalTokens == 0 && u.PromptTokens == 0 && u.CompletionTokens == 0 {
		return nil
	}
	backend := u.Backend
	if backend == "" {
		backend = "wackyacp"
	}
	return &agentv1.TurnUsage{
		PromptTokens:     u.PromptTokens,
		CompletionTokens: u.CompletionTokens,
		TotalTokens:      u.TotalTokens,
		Backend:          backend,
	}
}

func translateError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, err.Error())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	if errors.Is(err, acp.ErrHarnessDied) {
		return status.Errorf(codes.Unavailable, "harness process died: %v", err)
	}
	if errors.Is(err, acp.ErrTurnFailed) {
		return status.Errorf(codes.Internal, "acp turn error: %v", err)
	}
	return status.Errorf(codes.Internal, "turn execution error: %v", err)
}

func (s *Server) GenerateTurnStream(req *agentv1.GenerateTurnStreamRequest, stream agentv1.AgentService_GenerateTurnStreamServer) error {
	if !s.beginTurn() {
		return errTurnBusy()
	}
	defer s.endTurn()
	ctx := stream.Context()
	defer func() {
		if ctx.Err() != nil {
			_ = s.driver.Cancel(s.sessionID)
		}
	}()
	promptText := "" // default continuation

	callbacks := acp.TurnCallbacks{
		OnChunk: func(text string) error {
			return stream.Send(&agentv1.GenerateTurnStreamResponse{
				Text: text,
			})
		},
		OnWarning: func(warning string) error {
			// GenerateTurnStreamResponse has no warning field; nothing to send directly
			return nil
		},
		OnToolCall: func(call *agentv1.ToolCall) error {
			return stream.Send(&agentv1.GenerateTurnStreamResponse{
				ToolCall: call,
			})
		},
		OnToolCallUpdate: func(update *agentv1.ToolCallUpdate) error {
			return stream.Send(&agentv1.GenerateTurnStreamResponse{
				ToolCallUpdate: update,
			})
		},
	}

	res, err := s.driver.Prompt(ctx, s.sessionID, promptText, callbacks)
	if err != nil {
		return translateError(err)
	}

	// Final usage chunk (per D116 §9 and D117)
	if usage := toProtoUsage(res.Usage); usage != nil {
		if err := stream.Send(&agentv1.GenerateTurnStreamResponse{
			Usage: usage,
		}); err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) AddAndGenerateTurnStream(req *agentv1.AddAndGenerateTurnStreamRequest, stream agentv1.AgentService_AddAndGenerateTurnStreamServer) error {
	if !s.beginTurn() {
		return errTurnBusy()
	}
	defer s.endTurn()
	ctx := stream.Context()
	defer func() {
		if ctx.Err() != nil {
			_ = s.driver.Cancel(s.sessionID)
		}
	}()
	promptText := req.GetUserMessage()

	callbacks := acp.TurnCallbacks{
		OnChunk: func(text string) error {
			return stream.Send(&agentv1.AddAndGenerateTurnStreamResponse{
				Text: text,
			})
		},
		OnWarning: func(warning string) error {
			return stream.Send(&agentv1.AddAndGenerateTurnStreamResponse{
				Warning: warning,
			})
		},
		OnToolCall: func(call *agentv1.ToolCall) error {
			return stream.Send(&agentv1.AddAndGenerateTurnStreamResponse{
				ToolCall: call,
			})
		},
		OnToolCallUpdate: func(update *agentv1.ToolCallUpdate) error {
			return stream.Send(&agentv1.AddAndGenerateTurnStreamResponse{
				ToolCallUpdate: update,
			})
		},
	}

	res, err := s.driver.Prompt(ctx, s.sessionID, promptText, callbacks)
	if err != nil {
		return translateError(err)
	}

	// Final usage chunk (per D116 §9 and D117)
	if usage := toProtoUsage(res.Usage); usage != nil {
		if err := stream.Send(&agentv1.AddAndGenerateTurnStreamResponse{
			Usage: usage,
		}); err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) GenerateTurn(ctx context.Context, req *agentv1.GenerateTurnRequest) (*agentv1.GenerateTurnResponse, error) {
	if !s.beginTurn() {
		return nil, errTurnBusy()
	}
	defer s.endTurn()
	defer func() {
		if ctx.Err() != nil {
			_ = s.driver.Cancel(s.sessionID)
		}
	}()
	var sb strings.Builder

	callbacks := acp.TurnCallbacks{
		OnChunk: func(text string) error {
			sb.WriteString(text)
			return nil
		},
	}

	res, err := s.driver.Prompt(ctx, s.sessionID, "", callbacks)
	if err != nil {
		return nil, translateError(err)
	}

	return &agentv1.GenerateTurnResponse{
		Text:  sb.String(),
		Usage: toProtoUsage(res.Usage),
	}, nil
}

func (s *Server) AddAndGenerateTurn(ctx context.Context, req *agentv1.AddAndGenerateTurnRequest) (*agentv1.AddAndGenerateTurnResponse, error) {
	if !s.beginTurn() {
		return nil, errTurnBusy()
	}
	defer s.endTurn()
	defer func() {
		if ctx.Err() != nil {
			_ = s.driver.Cancel(s.sessionID)
		}
	}()
	var sb strings.Builder
	var warnings []string

	callbacks := acp.TurnCallbacks{
		OnChunk: func(text string) error {
			sb.WriteString(text)
			return nil
		},
		OnWarning: func(warning string) error {
			warnings = append(warnings, warning)
			return nil
		},
	}

	res, err := s.driver.Prompt(ctx, s.sessionID, req.GetUserMessage(), callbacks)
	if err != nil {
		return nil, translateError(err)
	}

	return &agentv1.AddAndGenerateTurnResponse{
		Text:     sb.String(),
		Warnings: warnings,
		Usage:    toProtoUsage(res.Usage),
	}, nil
}

// AsideQuestion implements agentv1.AgentServiceServer. ACP bridged harnesses (agy via
// wackyagy, claude via wackyacp) CANNOT support aside with parity: a fresh harness session
// starts with empty accumulated context - it is NOT a fork of the agent's live session, so
// an aside would answer without the very context it exists to query. The RPC stays declared
// in the proto contract so callers get a structured, explicit error (rather than an
// unknown-method failure) and can decide to fall back to the local agent instead.
func (s *Server) AsideQuestion(ctx context.Context, req *agentv1.AsideQuestionRequest) (*agentv1.AsideQuestionResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	return nil, status.Errorf(codes.Unimplemented, "aside is not supported over the ACP bridge: bridged harness sessions cannot fork the agent's accumulated context (use the local agent path for aside)")
}

func (s *Server) ReadSession(ctx context.Context, req *agentv1.ReadSessionRequest) (*agentv1.ReadSessionResponse, error) {
	return &agentv1.ReadSessionResponse{
		Turns: []*agentv1.SessionTurn{
			{
				Role: "assistant",
				Parts: []*agentv1.SessionPart{
					{
						Text: s.sessionID,
					},
				},
			},
		},
	}, nil
}

func (s *Server) InspectAgent(ctx context.Context, req *agentv1.InspectAgentRequest) (*agentv1.InspectAgentResponse, error) {
	return &agentv1.InspectAgentResponse{
		AgentId:        req.GetAgentId(),
		AgentDir:       s.agentFolder,
		AgentDirExists: true,
	}, nil
}

// ServeStdio registers the D112 server and serves gRPC over stdin/stdout until
// the client closes (EOF), ctx cancellation, or the server stops. The conn +
// listener machinery lives in the shared pkg/stdio package so both binaries use
// one implementation; cleanup behavior (graceful stop on EOF) is identical to the
// pre-consolidation copy.
func ServeStdio(ctx context.Context, srv *Server, stdin io.ReadCloser, stdout io.WriteCloser) error {
	grpcServer := grpc.NewServer()
	agentv1.RegisterAgentServiceServer(grpcServer, srv)
	conn := stdio.NewConn(stdin, stdout)
	return stdio.ServeContext(ctx, grpcServer, conn)
}
