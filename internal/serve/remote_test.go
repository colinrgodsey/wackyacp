package serve

import (
	"bufio"
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/colinrgodsey/wackyacp/internal/acp"
	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// fakeAgentService stands in for a live wackypub tcp-serve endpoint (wackypub
// #110) in tests: a real AgentService over real TCP, with the narrow surface
// serve mode consumes. token != "" enforces the same bearer contract the real
// server does (authorization: Bearer <token> on every RPC, unary and stream).
type fakeAgentService struct {
	agentv1.UnimplementedAgentServiceServer
	token string

	cancelCh   chan struct{}
	cancelOnce sync.Once
}

func (f *fakeAgentService) ListAgents(ctx context.Context, req *agentv1.ListAgentsRequest) (*agentv1.ListAgentsResponse, error) {
	return &agentv1.ListAgentsResponse{}, nil
}

func (f *fakeAgentService) AddAndGenerateTurnStream(req *agentv1.AddAndGenerateTurnStreamRequest, stream agentv1.AgentService_AddAndGenerateTurnStreamServer) error {
	if strings.HasPrefix(req.GetUserMessage(), "slow:") {
		// Hold the stream open until CancelTurn arrives. The real wackypub SDK
		// ends a canceled turn's stream with the canceled context (surfacing
		// as gRPC code Canceled), not a clean EOF - mirror that so the test
		// pins the Prompt mapping, not a fake-friendlier variant. A stream
		// that dies with the transport (reconnect test) returns the context
		// error instead.
		select {
		case <-f.cancelCh:
			return status.Error(codes.Canceled, "context canceled")
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
	if err := stream.Send(&agentv1.AddAndGenerateTurnStreamResponse{Text: "A"}); err != nil {
		return err
	}
	if err := stream.Send(&agentv1.AddAndGenerateTurnStreamResponse{Text: "B"}); err != nil {
		return err
	}
	return stream.Send(&agentv1.AddAndGenerateTurnStreamResponse{
		Usage: &agentv1.TurnUsage{TotalTokens: 3, PromptTokens: 1, CompletionTokens: 2},
	})
}

func (f *fakeAgentService) ReadSession(ctx context.Context, req *agentv1.ReadSessionRequest) (*agentv1.ReadSessionResponse, error) {
	return &agentv1.ReadSessionResponse{}, nil
}

func (f *fakeAgentService) CancelTurn(ctx context.Context, req *agentv1.CancelTurnRequest) (*agentv1.CancelTurnResponse, error) {
	f.cancelOnce.Do(func() { close(f.cancelCh) })
	return &agentv1.CancelTurnResponse{}, nil
}

// testTokenCheck mirrors the tcp-serve token interceptors (wackypub #110):
// when a token is configured, every RPC must carry "Bearer <token>".
func testTokenCheck(ctx context.Context, token string) error {
	md, _ := metadata.FromIncomingContext(ctx)
	for _, v := range md.Get("authorization") {
		if v == "Bearer "+token {
			return nil
		}
	}
	return status.Error(codes.Unauthenticated, "invalid bearer token")
}

func startFakeServe(t *testing.T, addr, token string) func() {
	t.Helper()
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listening fake serve: %v", err)
	}
	gopts := []grpc.ServerOption{}
	if token != "" {
		tok := token
		gopts = append(gopts,
			grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				if err := testTokenCheck(ctx, tok); err != nil {
					return nil, err
				}
				return handler(ctx, req)
			}),
			grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
				if err := testTokenCheck(ss.Context(), tok); err != nil {
					return err
				}
				return handler(srv, ss)
			}),
		)
	}
	f := &fakeAgentService{token: token, cancelCh: make(chan struct{})}
	srv := grpc.NewServer(gopts...)
	agentv1.RegisterAgentServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	return func() { srv.Stop() }
}

// startRemoteServer is startServer for remote mode: NewServerRemote with the
// agent-id-derived session id instead of a folder-derived one.
func startRemoteServer(t *testing.T, b *Backend, sessionID string) *testClient {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	srv := NewServerRemote(inR, outW, b, sessionID)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Cleanup(func() { _ = inW.Close() })
	go srv.Serve(ctx)
	return &testClient{t: t, w: inW, scanner: bufio.NewScanner(outR)}
}

// TestRemoteE2ERoundTrip drives the full remote stack: ACP frontend over
// pipes -> Backend -> gRPC over real TCP -> fake AgentService. It proves the
// remote mode keeps the ACP surface (initialize/session/prompt/cancel) while
// the backend is a live endpoint, and that the session id is the
// agent-derived one.
func TestRemoteE2ERoundTrip(t *testing.T) {
	free := freePort(t)
	stop := startFakeServe(t, free, "")
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	gc, err := NewRemoteClient(ctx, free, "")
	if err != nil {
		t.Fatalf("NewRemoteClient: %v", err)
	}
	defer func() { _ = gc.Close() }()

	b := NewBackend("agent1", "", "", NewGRPCWackypub(agentv1.NewAgentServiceClient(gc)), nil)
	sid := SessionIDForAgent("agent1")
	c := startRemoteServer(t, b, sid)

	res, e := c.request(t, acp.MethodInitialize, map[string]any{"protocolVersion": 1})
	if e != nil {
		t.Fatalf("initialize: %+v", e)
	}
	if _, ok := res["agentCapabilities"].(map[string]any); !ok {
		t.Fatalf("initialize: missing capabilities: %v", res)
	}

	res, e = c.request(t, acp.MethodSessionNew, map[string]any{"cwd": "/nonexistent/on-this-host-but-valid-absolute"})
	if e != nil {
		t.Fatalf("session/new (remote cwd relaxed): %+v", e)
	}
	if got := res["sessionId"].(string); got != sid {
		t.Fatalf("sessionId: got %s want %s", got, sid)
	}

	res, e = c.request(t, acp.MethodSessionPrompt, promptParams(sid, "hi"))
	if e != nil {
		t.Fatalf("prompt: %+v", e)
	}
	if res["stopReason"] != "end_turn" {
		t.Fatalf("stopReason: got %v", res["stopReason"])
	}
	usage, _ := res["usage"].(map[string]any)
	if usage == nil || usage["totalTokens"] != float64(3) {
		t.Fatalf("usage: got %v", res["usage"])
	}
	ups := c.updatesSeen()
	if len(ups) != 2 || chunkText(ups[0]) != "A" || chunkText(ups[1]) != "B" {
		t.Fatalf("want chunks A B, got %+v", ups)
	}

	// A held prompt plus session/cancel must settle as cancelled: the cancel
	// crosses the wire as a CancelTurn RPC and the fake ends the stream.
	id := c.send(t, acp.MethodSessionPrompt, promptParams(sid, "slow: hang"))
	time.Sleep(50 * time.Millisecond) // let the turn start on the wire
	c.send(t, acp.MethodSessionCancel, map[string]any{"sessionId": sid})
	res, e = c.readResponse(t, id)
	if e != nil {
		t.Fatalf("prompt (cancel): %+v", e)
	}
	if res["stopReason"] != "cancelled" {
		t.Fatalf("stopReason: got %v want cancelled", res["stopReason"])
	}
}

// TestRemoteUnreachableAtStartup pins the fail-fast contract: a remote that
// is already gone is reported at startup with a clean error, not discovered
// on the first user prompt.
func TestRemoteUnreachableAtStartup(t *testing.T) {
	dead := freePort(t)
	start := time.Now()
	gc, err := NewRemoteClient(context.Background(), dead, "")
	if err == nil {
		_ = gc.Close()
		t.Fatalf("expected unreachable error, got nil")
	}
	if !strings.Contains(err.Error(), "not reachable") {
		t.Fatalf("error not actionable: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 4*time.Second {
		t.Fatalf("probe took %v; the unreachable case must fail fast", elapsed)
	}
}

// TestRemoteTokenContract pins the auth surface: refused without a token and
// with a wrong token, accepted with the right one (the server enforces the
// same contract wackypub tcp-serve does, #110).
func TestRemoteTokenContract(t *testing.T) {
	addr := freePort(t)
	stop := startFakeServe(t, addr, "s3cret")
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	for name, tok := range map[string]string{"no token": "", "wrong token": "wrong"} {
		gc, err := NewRemoteClient(ctx, addr, tok)
		if err == nil {
			_ = gc.Close()
			t.Fatalf("%s: expected refusal, got a client", name)
		}
		if !strings.Contains(err.Error(), "Unauthenticated") {
			t.Fatalf("%s: want Unauthenticated, got: %v", name, err)
		}
	}
	gc, err := NewRemoteClient(ctx, addr, "s3cret")
	if err != nil {
		t.Fatalf("valid token refused: %v", err)
	}
	_ = gc.Close()
}

// TestRemoteReconnectsAfterDeath pins the mid-session death behavior: a dead
// remote degrades to transport errors that heal when the remote returns - the
// wackydiscord #23 pattern (the #21 die-on-death design was superseded).
func TestRemoteReconnectsAfterDeath(t *testing.T) {
	addr := freePort(t)
	stop := startFakeServe(t, addr, "")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	gc, err := NewRemoteClient(ctx, addr, "")
	if err != nil {
		t.Fatalf("NewRemoteClient: %v", err)
	}
	defer func() { _ = gc.Close() }()
	b := NewBackend("agent1", "", "", NewGRPCWackypub(agentv1.NewAgentServiceClient(gc)), nil)
	c := startRemoteServer(t, b, SessionIDForAgent("agent1"))
	c.request(t, acp.MethodInitialize, map[string]any{"protocolVersion": 1})

	res, e := c.request(t, acp.MethodSessionPrompt, promptParams(SessionIDForAgent("agent1"), "before death"))
	if e != nil || res["stopReason"] != "end_turn" {
		t.Fatalf("prompt before death: %v %+v", res, e)
	}

	stop() // immediate: pending streams abort, listener closes
	stop = startFakeServe(t, addr, "")
	defer stop()

	// The contract (wackydiscord #23: reconnect, not die-on-death): the serve
	// process survives, and prompts succeed once the remote is back. The
	// prompt that races the death may still ride the dead transport and come
	// back Unavailable - gRPC does not retry streaming RPCs - so the first
	// attempt after the restart is retried here, within a bounded window that
	// covers the redial backoff.
	sid := SessionIDForAgent("agent1")
	deadline := time.Now().Add(10 * time.Second)
	for {
		res, e = c.request(t, acp.MethodSessionPrompt, promptParams(sid, "after death"))
		if e == nil && res["stopReason"] == "end_turn" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("prompt after death did not recover: %v %+v", res, e)
		}
		t.Logf("post-death attempt failed (expected while re-dialing): %v %+v", res, e)
		time.Sleep(250 * time.Millisecond)
	}
}

func freePort(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("finding a free port: %v", err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatalf("releasing port: %v", err)
	}
	return addr
}
