package d112

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/colinrgodsey/wackyacp/internal/acp"
	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
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
}

// NewServer constructs a new D112 Server backed by an ACP driver.
func NewServer(driver ACPDriver, sessionID, agentFolder string) *Server {
	return &Server{
		driver:      driver,
		sessionID:   sessionID,
		agentFolder: agentFolder,
	}
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
	ctx := stream.Context()
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
	ctx := stream.Context()
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

// fixedConnListener satisfies net.Listener for a single pre-established net.Conn.
type fixedConnListener struct {
	conn      net.Conn
	once      sync.Once
	closeOnce sync.Once
	closedCh  chan struct{}
}

func newFixedConnListener(conn net.Conn) *fixedConnListener {
	return &fixedConnListener{
		conn:     conn,
		closedCh: make(chan struct{}),
	}
}

func (l *fixedConnListener) Accept() (net.Conn, error) {
	var c net.Conn
	l.once.Do(func() {
		c = l.conn
	})
	if c != nil {
		return c, nil
	}
	<-l.closedCh
	return nil, net.ErrClosed
}

func (l *fixedConnListener) Close() error {
	l.closeOnce.Do(func() {
		close(l.closedCh)
	})
	return l.conn.Close()
}

func (l *fixedConnListener) Addr() net.Addr {
	return l.conn.LocalAddr()
}

// ServerStdioConn adapts process stdin (read) and stdout (write) to net.Conn.
type ServerStdioConn struct {
	stdin     io.ReadCloser
	stdout    io.WriteCloser
	onEOF     func()
	closeOnce sync.Once
	closeErr  error
}

func NewServerStdioConn(stdin io.ReadCloser, stdout io.WriteCloser, onEOF func()) *ServerStdioConn {
	return &ServerStdioConn{
		stdin:  stdin,
		stdout: stdout,
		onEOF:  onEOF,
	}
}

func (c *ServerStdioConn) Read(b []byte) (int, error) {
	n, err := c.stdin.Read(b)
	if err != nil {
		if c.onEOF != nil {
			c.onEOF()
		}
	}
	return n, err
}

func (c *ServerStdioConn) Write(b []byte) (int, error) {
	return c.stdout.Write(b)
}

func (c *ServerStdioConn) Close() error {
	c.closeOnce.Do(func() {
		inErr := c.stdin.Close()
		outErr := c.stdout.Close()
		c.closeErr = errors.Join(inErr, outErr)
		if c.onEOF != nil {
			c.onEOF()
		}
	})
	return c.closeErr
}

func (c *ServerStdioConn) LocalAddr() net.Addr                { return stdioAddr{} }
func (c *ServerStdioConn) RemoteAddr() net.Addr               { return stdioAddr{} }
func (c *ServerStdioConn) SetDeadline(t time.Time) error      { return nil }
func (c *ServerStdioConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *ServerStdioConn) SetWriteDeadline(t time.Time) error { return nil }

type stdioAddr struct{}

func (stdioAddr) Network() string { return "stdio" }
func (stdioAddr) String() string  { return "stdio" }

// ServeStdio registers the D112 server and serves gRPC over stdin/stdout until EOF or cancellation.
func ServeStdio(ctx context.Context, srv *Server, stdin io.ReadCloser, stdout io.WriteCloser) error {
	grpcServer := grpc.NewServer()
	agentv1.RegisterAgentServiceServer(grpcServer, srv)

	var stopOnce sync.Once
	onEOF := func() {
		stopOnce.Do(func() {
			go func() {
				time.Sleep(30 * time.Millisecond)
				grpcServer.GracefulStop()
			}()
		})
	}

	conn := NewServerStdioConn(stdin, stdout, onEOF)
	listener := newFixedConnListener(conn)

	errCh := make(chan error, 1)
	go func() {
		errCh <- grpcServer.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		grpcServer.GracefulStop()
		_ = listener.Close()
		return ctx.Err()
	case err := <-errCh:
		if err == nil || errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) || errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return err
	}
}
