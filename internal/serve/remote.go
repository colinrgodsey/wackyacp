package serve

import (
	"context"
	"fmt"
	"time"

	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// remoteProbeTimeout bounds the startup reachability probe. A dead local port
// fails in milliseconds with connection refused; the deadline covers the
// black-holed case (firewall drop, dead host behind NAT) so serve --remote
// fails fast with a clean error instead of hanging at startup.
const remoteProbeTimeout = 5 * time.Second

// ServeTokenEnv is the environment variable the client falls back to when the
// --token flag is absent. It is the SAME variable the wackypub tcp-serve
// server reads, so a process manager can export it once for both ends of a
// local SSH tunnel.
const ServeTokenEnv = "WACKYPUB_SERVE_TOKEN"

// NewRemoteClient dials a live wackypub tcp-serve endpoint (wackypub #110)
// and verifies it answers before returning. It is the remote-mode counterpart
// of NewDialer: where the spawn dialer owns a child process, this one owns
// nothing - the remote endpoint hosts the workspace, and the serve process
// needs no local workspace at all.
//
// token is the bearer secret the server enforces (empty = the server is
// open, which is what a loopback bind without WACKYPUB_SERVE_TOKEN gives).
//
// Reconnect: the default gRPC dialer re-dials a dead remote automatically,
// so a mid-session remote death degrades to per-RPC transport errors that
// heal when the remote returns - the wackydiscord #23 process-dialer
// pattern (child death is a transport disconnect gRPC heals, not a process
// death; the #21 die-on-death design was superseded for exactly this
// reason). The startup probe below is the fail-fast half: a remote that is
// already gone when serve starts is reported at startup, not on the first
// user prompt.
func NewRemoteClient(ctx context.Context, remoteAddr, token string) (*grpc.ClientConn, error) {
	gopts := []grpc.DialOption{
		// The idle reaper must stay off, for the same reason as the spawn
		// path (runServe): the default 30-minute idle close would drop the
		// transport on quiet periods. Remote mode re-dials, so an idle close
		// would just be churn - but keeping the timeout at 0 matches the
		// stdio path's proven configuration.
		grpc.WithIdleTimeout(0),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	if token != "" {
		gopts = append(gopts, grpc.WithPerRPCCredentials(bearer{token: token}))
	}
	gc, err := grpc.NewClient(remoteAddr, gopts...)
	if err != nil {
		return nil, fmt.Errorf("constructing remote client for %s: %w", remoteAddr, err)
	}
	if err := probeRemote(ctx, gc, remoteAddr); err != nil {
		_ = gc.Close()
		return nil, err
	}
	return gc, nil
}

// probeRemote proves the remote answers an AgentService call. ListAgents is
// the probe because it is read-only - it lists the workspace's agent
// directories and spawns nothing - so the probe costs the remote nothing and
// an unreachable remote surfaces at startup with a clean, actionable error.
func probeRemote(ctx context.Context, gc *grpc.ClientConn, remoteAddr string) error {
	probeCtx, cancel := context.WithTimeout(ctx, remoteProbeTimeout)
	defer cancel()
	if _, err := agentv1.NewAgentServiceClient(gc).ListAgents(probeCtx, &agentv1.ListAgentsRequest{}); err != nil {
		return fmt.Errorf("remote %s is not reachable: %w", remoteAddr, err)
	}
	return nil
}

// bearer attaches "authorization: Bearer <token>" to every RPC (unary and
// stream), matching the tcp-serve token interceptors (wackypub #110).
type bearer struct {
	token string
}

var _ credentials.PerRPCCredentials = (*bearer)(nil)

func (b bearer) RequireTransportSecurity() bool { return false }

func (b bearer) PermissionLevel() string { return "bearer" }

func (b bearer) GetRequestMetadata(ctx context.Context, authority ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}
