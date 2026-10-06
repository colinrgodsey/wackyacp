package serve

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/colinrgodsey/wackyacp/internal/acp"
	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// forceCancelGrace is the backstop window after a session/cancel arrives but
// the wackypub turn stream does not end on its own. It mirrors the reference
// ACP server's force-cancel grace (30s in claude-agent-acp): a wedged prompt
// loop must not hold the ACP prompt request forever, but a normal cancel
// settles well inside the window. It is a var (not a const) so tests can
// shorten the window.
var forceCancelGrace = 30 * time.Second

// Wackypub is the wackypub protocol surface serve mode needs. It is a narrow
// mirror of agentv1.AgentServiceClient; the generated gRPC client satisfies
// it structurally, and tests substitute a scripted fake.
type Wackypub interface {
	AddAndGenerateTurnStream(ctx context.Context, req *agentv1.AddAndGenerateTurnStreamRequest) (TurnStream, error)
	ReadSession(ctx context.Context, req *agentv1.ReadSessionRequest) (*agentv1.ReadSessionResponse, error)
	CancelTurn(ctx context.Context, req *agentv1.CancelTurnRequest) (*agentv1.CancelTurnResponse, error)
}

// TurnStream is the wackypub turn stream surface serve mode consumes. The
// generated gRPC streaming client satisfies it structurally.
type TurnStream interface {
	Recv() (*agentv1.AddAndGenerateTurnStreamResponse, error)
	CloseSend() error
}

// turnState is one in-flight prompt turn. The agent folder is a single
// shared conversation, so the Backend admits exactly one turn at a time
// (second prompters get a structured busy error, the D112 precedent).
type turnState struct {
	cancelCh   chan struct{} // closed when session/cancel targets this turn
	cancelOnce sync.Once     // cancel reaches the turn from three paths (cancel
	// notification, session/close, connection
	// disconnect); the close must happen exactly once
	owner *Server // the ACP connection that started the turn
}

// Backend is the process-wide wackypub side shared by every ACP connection
// of one serve process. It holds no conversation state: session.jsonl in the
// agent folder is the only store, and this struct only arbitrates turns and
// forwards RPCs.
type Backend struct {
	agentID      string
	workspaceDir string
	agentFolder  string
	svc          Wackypub
	logf         func(format string, args ...any)

	turnMu        sync.Mutex
	turnsInFlight int
	active        *turnState
}

// NewBackend wires a backend for one agent folder. logf receives warnings
// (hook warnings, ignored mcpServers); pass nil for stderr.
func NewBackend(agentID, workspaceDir, agentFolder string, svc Wackypub, logf func(string, ...any)) *Backend {
	if logf == nil {
		logf = func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, format+"\n", args...)
		}
	}
	return &Backend{agentID: agentID, workspaceDir: workspaceDir, agentFolder: agentFolder, svc: svc, logf: logf}
}

// AgentID is the wackypub agent id driven by this serve process.
func (b *Backend) AgentID() string { return b.agentID }

// AgentFolder is the agent folder path this serve process is bound to.
func (b *Backend) AgentFolder() string { return b.agentFolder }

// BeginTurn admits a new prompt turn. It returns the turn state to attach
// cancel notifications to, and false when another turn is already in flight.
func (b *Backend) BeginTurn(owner *Server) (*turnState, bool) {
	b.turnMu.Lock()
	defer b.turnMu.Unlock()
	if b.turnsInFlight > 0 {
		return nil, false
	}
	b.turnsInFlight++
	b.active = &turnState{cancelCh: make(chan struct{}), owner: owner}
	return b.active, true
}

// EndTurn releases a finished turn.
func (b *Backend) EndTurn(ts *turnState) {
	b.turnMu.Lock()
	defer b.turnMu.Unlock()
	if b.active == ts {
		b.active = nil
	}
	b.turnsInFlight--
}

// ActiveTurn reports the in-flight turn, if any (used by connection teardown
// to stop only turns the departing connection owns).
func (b *Backend) ActiveTurn() *turnState {
	b.turnMu.Lock()
	defer b.turnMu.Unlock()
	return b.active
}

// CancelInFlight sends CancelTurn for the agent and unblocks the force-cancel
// grace window of the in-flight turn. It returns false when no turn is in
// flight (nothing to cancel). The CancelTurn RPC is best-effort: if the
// stream is already ending, the cancel races it and loses harmlessly.
func (b *Backend) CancelInFlight(ctx context.Context) bool {
	b.turnMu.Lock()
	ts := b.active
	b.turnMu.Unlock()
	if ts == nil {
		return false
	}
	ts.cancelOnce.Do(func() { close(ts.cancelCh) })
	if _, err := b.svc.CancelTurn(ctx, &agentv1.CancelTurnRequest{
		AgentId:      b.agentID,
		WorkspaceDir: b.workspaceDir,
	}); err != nil && ctx.Err() == nil {
		b.logf("wackyacp serve: CancelTurn for agent %s failed: %v", b.agentID, err)
	}
	return true
}

// Usage is the ACP Usage object: per-turn token counts, carried on the
// PromptResponse (the ACP spec marks it experimental; wackypub's final-chunk
// usage, D116, maps onto it 1:1).
type Usage struct {
	TotalTokens  int `json:"totalTokens"`
	InputTokens  int `json:"inputTokens"`
	OutputTokens int `json:"outputTokens"`
}

// Prompt runs one wackypub turn and forwards each stream event as an ACP
// session/update payload. It returns the ACP stop reason (end_turn or
// cancelled) and the turn usage when the stream delivered one, or an error
// when the turn failed outside of cancellation. ctx cancellation (client
// disconnect) also settles the turn as cancelled.
func (b *Backend) Prompt(ctx context.Context, ts *turnState, promptText string, emit func(Update) error) (stopReason string, usage *Usage, err error) {
	// The stream is bound to the turn, not to the ACP connection: when Prompt
	// returns (finish, error, cancel, or force-cancel) turnCtx is canceled, so
	// a wedged stream cannot hold the pump goroutine in Recv - CloseSend only
	// half-closes and never cancels a server-streaming call.
	turnCtx, cancelTurn := context.WithCancel(ctx)
	defer cancelTurn()

	stream, err := b.svc.AddAndGenerateTurnStream(turnCtx, &agentv1.AddAndGenerateTurnStreamRequest{
		AgentId:      b.agentID,
		UserMessage:  promptText,
		WorkspaceDir: b.workspaceDir,
		A2AMetadata: &agentv1.A2AMetadata{
			CallerId:  "wackyacp-serve",
			CallChain: []string{"wackyacp-serve"},
		},
	})
	if err != nil {
		return "", nil, fmt.Errorf("starting turn: %w", err)
	}
	defer func() { _ = stream.CloseSend() }()

	// Recv runs in a pump goroutine so the select below can race the stream
	// against cancellation and the force-cancel grace timer.
	type streamMsg struct {
		resp *agentv1.AddAndGenerateTurnStreamResponse
		err  error
	}
	msgs := make(chan streamMsg, 1)
	go func() {
		defer close(msgs)
		for {
			resp, rerr := stream.Recv()
			select {
			case msgs <- streamMsg{resp: resp, err: rerr}:
			case <-turnCtx.Done():
				return
			}
			if rerr != nil {
				return
			}
		}
	}()

	canceled := false
	// A closed channel is ready on every select, so the cancel case is niled
	// out after its first fire (nil channels never select); the grace timer is
	// the only pending path left, and it is created exactly once.
	cancelCh := ts.cancelCh
	var graceCh <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			return "cancelled", usage, nil

		case m, ok := <-msgs:
			if !ok {
				return finishStop(canceled), usage, nil
			}
			if m.err != nil {
				if m.err == io.EOF {
					return finishStop(canceled), usage, nil
				}
				// A stream that ends with a canceled-context error while a cancel
				// is in flight is the wackypub side honoring the cancel: the SDK
				// ends the stream with the canceled context, not a clean EOF (the
				// handler is transport-independent, so stdio and tcp behave alike).
				if isCancelErr(m.err) && cancelInFlight(canceled, cancelCh) {
					return "cancelled", usage, nil
				}
				return "", usage, m.err
			}
			u, merr := mapStreamEvent(m.resp, emit, b.logf)
			if merr != nil {
				return "", usage, merr
			}
			if u != nil {
				usage = u
			}

		case <-cancelCh:
			// Give the stream its chance to end on its own (the agent honors
			// the cancel and closes the turn); the grace timer is the
			// backstop for a wedged turn (see forceCancelGrace). A stream
			// that ends after a cancel is a cancelled turn, not a clean one.
			canceled = true
			cancelCh = nil
			graceCh = time.NewTimer(forceCancelGrace).C

		case <-graceCh:
			b.logf("wackyacp serve: force-cancel grace expired for agent %s; abandoning wedged turn", b.agentID)
			return "cancelled", usage, nil
		}
	}
}

// mapStreamEvent translates one wackypub stream event into ACP updates. It
// returns a Usage when the event carried the final-chunk token accounting
// (D116), which the caller attaches to the PromptResponse rather than a
// session update. Hook warnings have no ACP session/update kind; they are
// logged, not dropped silently.
func mapStreamEvent(resp *agentv1.AddAndGenerateTurnStreamResponse, emit func(Update) error, logf func(string, ...any)) (*Usage, error) {
	switch {
	case resp.GetWarning() != "":
		logf("wackyacp serve: hook warning during turn: %s", resp.GetWarning())
	case resp.GetText() != "":
		return nil, emit(ChunkUpdate(acp.UpdateKindAgentMessageChunk, resp.GetText()))
	case resp.GetToolCall() != nil:
		tc := resp.GetToolCall()
		status := acp.ToolStatusPending
		if tc.GetDenied() {
			status = acp.ToolStatusFailed
		}
		return nil, emit(ToolCallUpdatePayload(acp.UpdateKindToolCall, tc.GetCallId(), tc.GetToolName(), status, rawInputFromSummary(tc.GetArgsSummary()), nil))
	case resp.GetToolCallUpdate() != nil:
		tu := resp.GetToolCallUpdate()
		return nil, emit(ToolCallUpdatePayload(acp.UpdateKindToolCallUpdate, tu.GetCallId(), tu.GetToolName(), mapToolStatus(tu.GetStatus()), nil, &tu.ResultHead))
	case resp.GetUsage() != nil:
		u := resp.GetUsage()
		if u.GetTotalTokens() > 0 || u.GetPromptTokens() > 0 || u.GetCompletionTokens() > 0 {
			return &Usage{TotalTokens: int(u.GetTotalTokens()), InputTokens: int(u.GetPromptTokens()), OutputTokens: int(u.GetCompletionTokens())}, nil
		}
	}
	return nil, nil
}

// mapToolStatus translates wackypub tool event statuses (completed | error |
// denied) onto ACP tool call statuses (pending | in_progress | completed |
// failed). Denied is a post-decision failure from the agent side; ACP has no
// denied status, so it surfaces as failed.
func mapToolStatus(s string) string {
	switch s {
	case "completed":
		return acp.ToolStatusCompleted
	case "error", "denied":
		return acp.ToolStatusFailed
	default:
		return acp.ToolStatusPending
	}
}

// finishStop maps a stream that has just ended onto the ACP stop reason:
// a cancel that the agent honored reads as cancelled, a clean end reads as
// end_turn (the ACP value for a normal turn completion).
func finishStop(canceled bool) string {
	if canceled {
		return "cancelled"
	}
	return "end_turn"
}

// isCancelErr reports whether a stream-end error is the gRPC/context
// cancellation signal the wackypub SDK emits when a turn is canceled (the
// handler returns the canceled context, which gRPC surfaces as
// code = Canceled on both stdio and tcp transports).
func isCancelErr(err error) bool {
	if errors.Is(err, context.Canceled) {
		return true
	}
	if s, ok := status.FromError(err); ok {
		return s.Code() == codes.Canceled
	}
	return false
}

// cancelInFlight reports whether a cancel has fired (canceled) or is pending
// on cancelCh. The non-blocking probe never consumes the channel, so the
// select case that nils it out still owns the first fire.
func cancelInFlight(canceled bool, cancelCh <-chan struct{}) bool {
	if canceled {
		return true
	}
	select {
	case <-cancelCh:
		return true
	default:
		return false
	}
}

// rawInputFromSummary parses the redacted args summary into a JSON value for
// the ACP rawInput field, which is typed unknown. A summary that is not JSON
// passes through as a plain string.
func rawInputFromSummary(summary string) any {
	if summary == "" {
		return nil
	}
	var v any
	if err := json.Unmarshal([]byte(summary), &v); err != nil {
		return summary
	}
	return v
}
