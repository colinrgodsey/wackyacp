package d112

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"

	"github.com/colinrgodsey/wackyacp/internal/acp"
	"github.com/colinrgodsey/wackypub/pkg/agent"
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
	SetConfigOption(ctx context.Context, sessionID, configID, value string) (json.RawMessage, error)
	LastConfigOptions() json.RawMessage
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

	if isModel, target, isQuery := acp.IsModelSlashCommand(promptText); isModel {
		msg := s.handleModelCommand(ctx, target, isQuery)
		return stream.Send(&agentv1.AddAndGenerateTurnStreamResponse{
			Text: msg,
		})
	}

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

	promptText := req.GetUserMessage()
	if isModel, target, isQuery := acp.IsModelSlashCommand(promptText); isModel {
		msg := s.handleModelCommand(ctx, target, isQuery)
		return &agentv1.AddAndGenerateTurnResponse{
			Text: msg,
		}, nil
	}

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

	res, err := s.driver.Prompt(ctx, s.sessionID, promptText, callbacks)
	if err != nil {
		return nil, translateError(err)
	}

	return &agentv1.AddAndGenerateTurnResponse{
		Text:     sb.String(),
		Warnings: warnings,
		Usage:    toProtoUsage(res.Usage),
	}, nil
}

func (s *Server) handleModelCommand(ctx context.Context, targetModel string, isQuery bool) string {
	if isQuery {
		raw := s.driver.LastConfigOptions()
		info := acp.ExtractModelInfo(raw, "")
		return acp.FormatModelList(info)
	}

	rawResult, err := s.driver.SetConfigOption(ctx, s.sessionID, "model", targetModel)
	if err != nil {
		return acp.FormatModelSetError(targetModel, err)
	}

	info := acp.ExtractModelInfo(rawResult, targetModel)
	if len(info.Options) == 0 {
		info = acp.ExtractModelInfo(s.driver.LastConfigOptions(), targetModel)
	}
	confirmed := info.CurrentModel
	if confirmed == "" {
		confirmed = targetModel
	}
	return acp.FormatModelSetSuccess(confirmed, info)
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

func (s *Server) resolveAgentDir(agentID, reqWsDir string) string {
	cleanedFolder := filepath.Clean(s.agentFolder)
	if agentID == "" || agentID == filepath.Base(cleanedFolder) {
		if reqWsDir != "" {
			return filepath.Join(reqWsDir, filepath.Base(cleanedFolder))
		}
		return cleanedFolder
	}
	wsDir := reqWsDir
	if wsDir == "" {
		wsDir = filepath.Dir(cleanedFolder)
	}
	return filepath.Join(wsDir, agentID)
}

func (s *Server) CreateScratchpad(ctx context.Context, req *agentv1.CreateScratchpadRequest) (*agentv1.CreateScratchpadResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	text := req.GetText()
	data := req.GetData()
	if text == "" && len(data) == 0 {
		return nil, status.Error(codes.InvalidArgument, "scratchpad content cannot be empty")
	}
	if text != "" && len(data) > 0 {
		return nil, status.Error(codes.InvalidArgument, "scratchpad text and data are mutually exclusive")
	}

	agentDir := s.resolveAgentDir(req.GetAgentId(), req.GetWorkspaceDir())
	createdBy := req.GetCreatedBy()
	if createdBy == "" {
		createdBy = "cli"
	}

	var entry *agent.ScratchpadEntry
	var err error
	if len(data) > 0 {
		entry, err = agent.CreateBinaryScratchpad(agentDir, data, createdBy, req.GetMimeType())
	} else {
		entry, err = agent.CreateScratchpad(agentDir, text, createdBy)
	}
	if err != nil {
		return nil, err
	}

	warnings := entry.Warnings
	if len(warnings) == 0 && entry.Warning != "" {
		warnings = []string{entry.Warning}
	}

	pbEntry := &agentv1.ScratchpadEntry{
		EntryId:   entry.ID,
		Size:      int64(entry.Size),
		Lines:     int32(entry.Lines),
		CreatedBy: entry.CreatedBy,
		Text:      entry.Text,
		IsBinary:  entry.IsBinary,
		MimeType:  entry.MIMEType,
		Warnings:  warnings,
	}
	return &agentv1.CreateScratchpadResponse{
		Entry: pbEntry,
	}, nil
}

func (s *Server) GetScratchpad(ctx context.Context, req *agentv1.GetScratchpadRequest) (*agentv1.GetScratchpadResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	entryID := req.GetEntryId()
	if entryID == "" {
		return nil, status.Error(codes.InvalidArgument, "entryId cannot be empty")
	}

	agentDir := s.resolveAgentDir(req.GetAgentId(), req.GetWorkspaceDir())

	var skipPtr, numPtr *int
	if req.SkipLines != nil {
		skip := int(*req.SkipLines)
		skipPtr = &skip
	}
	if req.NumLines != nil {
		num := int(*req.NumLines)
		numPtr = &num
	}

	text, err := agent.GetScratchpad(agentDir, entryID, skipPtr, numPtr)
	if err != nil {
		return nil, err
	}
	return &agentv1.GetScratchpadResponse{
		Text: text,
	}, nil
}

func (s *Server) ListScratchpads(ctx context.Context, req *agentv1.ListScratchpadsRequest) (*agentv1.ListScratchpadsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	agentDir := s.resolveAgentDir(req.GetAgentId(), req.GetWorkspaceDir())

	items, count, capVal, err := agent.ListScratchpads(agentDir)
	if err != nil {
		return nil, err
	}

	entries := make([]*agentv1.ScratchpadEntry, len(items))
	for i, item := range items {
		entries[i] = &agentv1.ScratchpadEntry{
			EntryId:   item.ID,
			Size:      int64(item.Size),
			Lines:     int32(item.Lines),
			CreatedBy: item.CreatedBy,
			IsBinary:  item.IsBinary,
			MimeType:  item.MIMEType,
		}
	}

	return &agentv1.ListScratchpadsResponse{
		Entries:      entries,
		TotalEntries: int32(count),
		MaxCapacity:  int32(capVal),
	}, nil
}

func (s *Server) SearchScratchpad(ctx context.Context, req *agentv1.SearchScratchpadRequest) (*agentv1.SearchScratchpadResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	entryID := req.GetEntryId()
	if entryID == "" {
		return nil, status.Error(codes.InvalidArgument, "entryId cannot be empty")
	}
	query := req.GetQuery()
	if query == "" {
		return nil, status.Error(codes.InvalidArgument, "query cannot be empty")
	}

	agentDir := s.resolveAgentDir(req.GetAgentId(), req.GetWorkspaceDir())

	var caseSensPtr *bool
	if req.CaseSensitive != nil {
		val := *req.CaseSensitive
		caseSensPtr = &val
	}

	res, err := agent.SearchScratchpad(agentDir, entryID, query, caseSensPtr, req.GetUseRegex(), int(req.GetMaxResults()))
	if err != nil {
		return nil, err
	}

	matches := make([]*agentv1.ScratchpadMatch, len(res.Matches))
	for i, m := range res.Matches {
		matches[i] = &agentv1.ScratchpadMatch{
			Line:      int32(m.Line),
			SkipLines: int32(m.SkipLines),
			Text:      m.Text,
		}
	}

	return &agentv1.SearchScratchpadResponse{
		EntryId:      res.ID,
		Query:        res.Query,
		TotalMatches: int32(res.TotalMatches),
		MaxResults:   int32(res.MaxResults),
		Matches:      matches,
	}, nil
}

func (s *Server) DiffScratchpadEntries(ctx context.Context, req *agentv1.DiffScratchpadEntriesRequest) (*agentv1.DiffScratchpadEntriesResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	beforeID := req.GetBeforeEntryId()
	afterID := req.GetAfterEntryId()
	if beforeID == "" || afterID == "" {
		return nil, status.Error(codes.InvalidArgument, "beforeEntryId and afterEntryId cannot be empty")
	}

	cleanedFolder := filepath.Clean(s.agentFolder)
	wsDir := req.GetWorkspaceDir()
	if wsDir == "" {
		wsDir = filepath.Dir(cleanedFolder)
	}
	agentID := req.GetAgentId()
	if agentID == "" {
		agentID = filepath.Base(cleanedFolder)
	}

	diff, err := agent.DiffScratchpadEntries(wsDir, agentID, beforeID, afterID)
	if err != nil {
		return nil, err
	}
	return &agentv1.DiffScratchpadEntriesResponse{
		Diff: diff,
	}, nil
}

func (s *Server) DeleteScratchpad(ctx context.Context, req *agentv1.DeleteScratchpadRequest) (*agentv1.DeleteScratchpadResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	entryID := req.GetEntryId()
	if entryID == "" {
		return nil, status.Error(codes.InvalidArgument, "entryId cannot be empty")
	}

	agentDir := s.resolveAgentDir(req.GetAgentId(), req.GetWorkspaceDir())
	if err := agent.DeleteScratchpad(agentDir, entryID); err != nil {
		return nil, err
	}
	return &agentv1.DeleteScratchpadResponse{}, nil
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
