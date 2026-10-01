package serve

import (
	"context"

	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
)

// grpcWackypub adapts the generated agentv1.AgentServiceClient to the narrow
// Wackypub interface: the generated stub's AddAndGenerateTurnStream carries a
// variadic ...grpc.CallOption the narrow interface lacks, and returns the
// grpc streaming-client type instead of TurnStream. Serve mode never uses
// per-RPC options, so the adapter drops them; the grpc streaming client
// satisfies TurnStream structurally.
type grpcWackypub struct {
	c agentv1.AgentServiceClient
}

// NewGRPCWackypub wraps a generated AgentServiceClient (as returned by
// agentv1.NewAgentServiceClient) into the Wackypub interface serve mode
// consumes, keeping the rest of the package (and its test fakes) free of
// grpc plumbing.
func NewGRPCWackypub(c agentv1.AgentServiceClient) Wackypub {
	return &grpcWackypub{c: c}
}

func (g *grpcWackypub) AddAndGenerateTurnStream(ctx context.Context, req *agentv1.AddAndGenerateTurnStreamRequest) (TurnStream, error) {
	stream, err := g.c.AddAndGenerateTurnStream(ctx, req)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func (g *grpcWackypub) ReadSession(ctx context.Context, req *agentv1.ReadSessionRequest) (*agentv1.ReadSessionResponse, error) {
	return g.c.ReadSession(ctx, req)
}

func (g *grpcWackypub) CancelTurn(ctx context.Context, req *agentv1.CancelTurnRequest) (*agentv1.CancelTurnResponse, error) {
	return g.c.CancelTurn(ctx, req)
}
