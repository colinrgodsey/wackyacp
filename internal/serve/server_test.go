package serve

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colinrgodsey/wackyacp/internal/acp"
	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
	"google.golang.org/genai"
)

// ---------------------------------------------------------------------------
// Fake wackypub backend
// ---------------------------------------------------------------------------

type blocker struct {
	once sync.Once
	done chan struct{}
}

func (b *blocker) release() { b.once.Do(func() { close(b.done) }) }

type fakeStream struct {
	ch  chan *agentv1.AddAndGenerateTurnStreamResponse
	ctx context.Context
}

func (s *fakeStream) Recv() (*agentv1.AddAndGenerateTurnStreamResponse, error) {
	select {
	case r, ok := <-s.ch:
		if !ok {
			return nil, io.EOF
		}
		return r, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *fakeStream) CloseSend() error { return nil }

// fakeWackypub is a scripted Wackypub for server tests. The default script
// is empty; block, when set, holds the turn open until CancelTurn releases
// it (mirroring a real agent honoring a cancel).
type fakeWackypub struct {
	mu        sync.Mutex
	promptReq *agentv1.AddAndGenerateTurnStreamRequest
	script    []*agentv1.AddAndGenerateTurnStreamResponse
	readResp  *agentv1.ReadSessionResponse
	block     *blocker

	starts     int
	cancels    int
	lastStream *fakeStream
}

func (f *fakeWackypub) stream() *fakeStream {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastStream
}

func (f *fakeWackypub) AddAndGenerateTurnStream(ctx context.Context, req *agentv1.AddAndGenerateTurnStreamRequest) (TurnStream, error) {
	f.mu.Lock()
	f.promptReq = req
	script := append([]*agentv1.AddAndGenerateTurnStreamResponse(nil), f.script...)
	block := f.block
	f.starts++
	f.mu.Unlock()

	st := &fakeStream{ch: make(chan *agentv1.AddAndGenerateTurnStreamResponse, len(script)+1), ctx: ctx}
	f.mu.Lock()
	f.lastStream = st
	f.mu.Unlock()
	go func() {
		defer close(st.ch)
		for _, r := range script {
			select {
			case st.ch <- r:
			case <-ctx.Done():
				return
			}
		}
		if block != nil {
			select {
			case <-block.done:
			case <-ctx.Done():
			}
		}
	}()
	return st, nil
}

func (f *fakeWackypub) ReadSession(ctx context.Context, req *agentv1.ReadSessionRequest) (*agentv1.ReadSessionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.readResp != nil {
		return f.readResp, nil
	}
	return &agentv1.ReadSessionResponse{}, nil
}

func (f *fakeWackypub) CancelTurn(ctx context.Context, req *agentv1.CancelTurnRequest) (*agentv1.CancelTurnResponse, error) {
	f.mu.Lock()
	f.cancels++
	block := f.block
	f.mu.Unlock()
	if block != nil {
		block.release()
	}
	return &agentv1.CancelTurnResponse{}, nil
}

func (f *fakeWackypub) startedTurns() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.starts
}

func (f *fakeWackypub) cancelCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cancels
}

func (f *fakeWackypub) lastPromptReq() *agentv1.AddAndGenerateTurnStreamRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.promptReq
}

// ---------------------------------------------------------------------------
// Test-side ACP client over pipes
// ---------------------------------------------------------------------------

// testClient speaks the ACP client side against a Server over in-memory
// pipes: JSON-RPC lines out, frames in. session/update notifications are
// recorded in arrival order; responses are read by request id. Single reader
// per client: tests that need a held-open request use send plus a later
// readResponse instead of a second concurrent reader.
type testClient struct {
	t       *testing.T
	w       *io.PipeWriter
	scanner *bufio.Scanner
	nextID  int64

	updatesMu sync.Mutex
	updates   []Update
	pending   []json.RawMessage // id frames for other in-flight requests
}

func startServer(t *testing.T, b *Backend, folder string) *testClient {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	srv := NewServer(inR, outW, b, folder)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Cleanup(func() { _ = inW.Close() })
	go srv.Serve(ctx)
	return &testClient{t: t, w: inW, scanner: bufio.NewScanner(outR)}
}

// nextIDFrame consumes lines until a frame with an id arrives, recording
// session/update notifications on the way.
func (c *testClient) nextIDFrame(t *testing.T) json.RawMessage {
	for c.scanner.Scan() {
		line := c.scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var frame map[string]json.RawMessage
		if err := json.Unmarshal(line, &frame); err != nil {
			t.Fatalf("malformed frame from server: %v (line %q)", err, line)
		}
		methodRaw, hasMethod := frame["method"]
		if hasMethod {
			var method string
			if err := json.Unmarshal(methodRaw, &method); err != nil {
				t.Fatalf("malformed method: %v", err)
			}
			if method == "session/update" {
				var params struct {
					SessionID string `json:"sessionId"`
					Update    Update `json:"update"`
				}
				if err := json.Unmarshal(frame["params"], &params); err != nil {
					t.Fatalf("malformed session/update params: %v", err)
				}
				c.updatesMu.Lock()
				c.updates = append(c.updates, params.Update)
				c.updatesMu.Unlock()
				continue
			}
		}
		if id, ok := frame["id"]; ok && len(id) > 0 {
			return append(json.RawMessage(nil), line...)
		}
	}
	t.Fatalf("server closed the connection: %v", c.scanner.Err())
	return nil
}

func (c *testClient) write(t *testing.T, method string, id any, params any) {
	t.Helper()
	obj := map[string]any{"jsonrpc": "2.0", "method": method, "params": params}
	if id != nil {
		obj["id"] = id
	}
	data, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal %s: %v", method, err)
	}
	if _, err := c.w.Write(append(data, '\n')); err != nil {
		t.Fatalf("write %s: %v", method, err)
	}
}

// send writes an ACP request and returns its id without reading the response.
func (c *testClient) send(t *testing.T, method string, params any) int64 {
	t.Helper()
	id := atomic.AddInt64(&c.nextID, 1)
	c.write(t, method, id, params)
	return id
}

// readResponse blocks until the response for wantID arrives, stashing id
// frames belonging to other in-flight requests so an out-of-order response
// (a held-open prompt settling before a busy error) is not lost.
func (c *testClient) readResponse(t *testing.T, wantID int64) (map[string]any, *RPCError) {
	t.Helper()
	for {
		for i, line := range c.pending {
			if idOf(t, line) == wantID {
				c.pending = append(c.pending[:i], c.pending[i+1:]...)
				return parseResponse(t, line)
			}
		}
		line := c.nextIDFrame(t)
		if idOf(t, line) == wantID {
			return parseResponse(t, line)
		}
		c.pending = append(c.pending, line)
	}
}

func idOf(t *testing.T, line json.RawMessage) int64 {
	t.Helper()
	var frame map[string]json.RawMessage
	if err := json.Unmarshal(line, &frame); err != nil {
		t.Fatalf("malformed frame: %v", err)
	}
	var id int64
	if err := json.Unmarshal(frame["id"], &id); err != nil {
		t.Fatalf("frame without numeric id: %v", err)
	}
	return id
}

func parseResponse(t *testing.T, line json.RawMessage) (map[string]any, *RPCError) {
	t.Helper()
	var frame map[string]json.RawMessage
	if err := json.Unmarshal(line, &frame); err != nil {
		t.Fatalf("malformed response: %v", err)
	}
	if errRaw, ok := frame["error"]; ok {
		var e RPCError
		if err := json.Unmarshal(errRaw, &e); err != nil {
			t.Fatalf("malformed error object: %v", err)
		}
		return nil, &e
	}
	var res map[string]any
	if err := json.Unmarshal(frame["result"], &res); err != nil {
		t.Fatalf("malformed result: %v (frame %s)", err, line)
	}
	return res, nil
}

// request sends an ACP request and blocks until its response arrives.
func (c *testClient) request(t *testing.T, method string, params any) (map[string]any, *RPCError) {
	t.Helper()
	id := c.send(t, method, params)
	return c.readResponse(t, id)
}

func (c *testClient) notify(t *testing.T, method string, params any) {
	c.write(t, method, nil, params)
}

func (c *testClient) updatesSeen() []Update {
	c.updatesMu.Lock()
	defer c.updatesMu.Unlock()
	return append([]Update(nil), c.updates...)
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func contentJSON(t *testing.T, c genai.Content) string {
	t.Helper()
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshaling content: %v", err)
	}
	return string(data)
}

func chunkText(u Update) string {
	content, _ := u["content"].(map[string]any)
	if content == nil {
		return ""
	}
	text, _ := content["text"].(string)
	return text
}

func promptParams(sessionID string, text string) any {
	return map[string]any{
		"sessionId": sessionID,
		"prompt":    []any{map[string]any{"type": "text", "text": text}},
	}
}

// ---------------------------------------------------------------------------
// Golden ACP round-trips
// ---------------------------------------------------------------------------

func TestInitializeAdvertisesV1(t *testing.T) {
	f := &fakeWackypub{}
	folder := t.TempDir()
	b := NewBackend("agent1", filepath.Dir(folder), folder, f, nil)
	c := startServer(t, b, folder)

	res, e := c.request(t, acp.MethodInitialize, map[string]any{"protocolVersion": 1})
	if e != nil {
		t.Fatalf("initialize: %+v", e)
	}
	if got := res["protocolVersion"]; got != float64(1) {
		t.Fatalf("protocolVersion: got %v want 1", got)
	}
	caps, _ := res["agentCapabilities"].(map[string]any)
	if caps == nil {
		t.Fatal("missing agentCapabilities")
	}
	if got, _ := caps["loadSession"].(bool); !got {
		t.Error("loadSession must be advertised")
	}
	sc, _ := caps["sessionCapabilities"].(map[string]any)
	if sc == nil || sc["close"] != true {
		t.Errorf("session/close capability: got %v", caps["sessionCapabilities"])
	}
	pi, _ := res["agentInfo"].(map[string]any)
	if pi == nil || pi["name"] != "wackyacp" || pi["version"] != Version {
		t.Fatalf("agentInfo: got %v", pi)
	}
	for _, k := range []string{"auth", "providers", "nes", "document", "mcpCapabilities", "positionEncoding"} {
		if _, ok := caps[k]; ok {
			t.Errorf("capability %q must not be advertised in v1", k)
		}
	}
	pc, _ := caps["promptCapabilities"].(map[string]any)
	if pc == nil {
		t.Fatal("missing promptCapabilities")
	}
	if got, _ := pc["image"].(bool); got {
		t.Error("image prompt capability must not be advertised in v1")
	}
}

func TestSessionNewStableIDAndValidation(t *testing.T) {
	f := &fakeWackypub{}
	folder := t.TempDir()
	b := NewBackend("agent1", filepath.Dir(folder), folder, f, nil)
	c := startServer(t, b, folder)

	_, e := c.request(t, acp.MethodSessionNew, map[string]any{"cwd": "relative/path"})
	if e == nil || e.Code != acp.CodeInvalidParams {
		t.Fatalf("relative cwd: want invalidParams, got %+v", e)
	}
	if !strings.Contains(e.Message, "absolute") {
		t.Errorf("relative cwd message should be actionable: %s", e.Message)
	}

	_, e = c.request(t, acp.MethodSessionNew, map[string]any{"cwd": filepath.Join(folder, "nope")})
	if e == nil || e.Code != acp.CodeInvalidParams {
		t.Fatalf("missing cwd: want invalidParams, got %+v", e)
	}

	res, e := c.request(t, acp.MethodSessionNew, map[string]any{"cwd": folder, "mcpServers": []any{}})
	if e != nil {
		t.Fatalf("valid new: %+v", e)
	}
	want := SessionID(folder)
	if res["sessionId"] != want {
		t.Fatalf("sessionId: got %v want %s", res["sessionId"], want)
	}
	res2, e := c.request(t, acp.MethodSessionNew, map[string]any{"cwd": folder})
	if e != nil {
		t.Fatalf("second new: %+v", e)
	}
	if res2["sessionId"] != want {
		t.Fatalf("second new must attach to the same session: got %v want %s", res2["sessionId"], want)
	}
}

func TestSessionLoadReplaysContentJSON(t *testing.T) {
	f := &fakeWackypub{readResp: &agentv1.ReadSessionResponse{Turns: []*agentv1.SessionTurn{
		{Seq: 1, Role: "user", ContentJson: contentJSON(t, genai.Content{
			Role:  "user",
			Parts: []*genai.Part{{Text: "hello"}},
		})},
		{Seq: 2, Role: "model", ContentJson: contentJSON(t, genai.Content{
			Role: "model",
			Parts: []*genai.Part{
				{Text: "thinking out loud", Thought: true},
				{FunctionCall: &genai.FunctionCall{ID: "fc1", Name: "run_command", Args: map[string]any{"cmd": "ls"}}},
			},
		})},
		{Seq: 3, Role: "user", ContentJson: contentJSON(t, genai.Content{
			Role:  "user",
			Parts: []*genai.Part{{FunctionResponse: &genai.FunctionResponse{ID: "fc1", Name: "run_command", Response: map[string]any{"output": "ok output"}}}},
		})},
	}}}
	folder := t.TempDir()
	b := NewBackend("agent1", filepath.Dir(folder), folder, f, nil)
	c := startServer(t, b, folder)

	res, e := c.request(t, acp.MethodSessionLoad, map[string]any{"sessionId": SessionID(folder), "cwd": folder})
	if e != nil {
		t.Fatalf("load: %+v", e)
	}
	if len(res) != 0 {
		t.Fatalf("load must respond with an empty object, got %v", res)
	}

	ups := c.updatesSeen()
	if len(ups) != 4 {
		t.Fatalf("want 4 replay updates, got %d: %+v", len(ups), ups)
	}
	if ups[0]["sessionUpdate"] != acp.UpdateKindUserMessageChunk || chunkText(ups[0]) != "hello" {
		t.Errorf("update 0: %+v", ups[0])
	}
	if ups[1]["sessionUpdate"] != acp.UpdateKindAgentThoughtChunk || chunkText(ups[1]) != "thinking out loud" {
		t.Errorf("update 1: %+v", ups[1])
	}
	tc := ups[2]
	if tc["sessionUpdate"] != acp.UpdateKindToolCall || tc["toolCallId"] != "fc1" || tc["name"] != "run_command" || tc["status"] != acp.ToolStatusPending {
		t.Errorf("update 2: %+v", tc)
	}
	ri, _ := tc["rawInput"].(map[string]any)
	if ri == nil || ri["cmd"] != "ls" {
		t.Errorf("update 2 rawInput: %v", tc["rawInput"])
	}
	tu := ups[3]
	if tu["sessionUpdate"] != acp.UpdateKindToolCallUpdate || tu["toolCallId"] != "fc1" || tu["status"] != acp.ToolStatusCompleted {
		t.Errorf("update 3: %+v", tu)
	}
	content, _ := tu["content"].([]any)
	if len(content) != 1 {
		t.Fatalf("update 3 content: %+v", tu["content"])
	}
	inner, _ := content[0].(map[string]any)
	blk, _ := inner["content"].(map[string]any)
	if blk["text"] != `{"output":"ok output"}` {
		t.Errorf("update 3 result text: %v", blk["text"])
	}
}

func TestUnknownSessionRejected(t *testing.T) {
	f := &fakeWackypub{}
	folder := t.TempDir()
	b := NewBackend("agent1", filepath.Dir(folder), folder, f, nil)
	c := startServer(t, b, folder)

	_, e := c.request(t, acp.MethodSessionLoad, map[string]any{"sessionId": "wackyacp:deadbeefdeadbeef"})
	if e == nil || e.Code != acp.CodeInvalidParams {
		t.Fatalf("load unknown session: want invalidParams, got %+v", e)
	}
	_, e = c.request(t, acp.MethodSessionPrompt, map[string]any{
		"sessionId": "nope",
		"prompt":    []any{map[string]any{"type": "text", "text": "hi"}},
	})
	if e == nil || e.Code != acp.CodeInvalidParams {
		t.Fatalf("prompt unknown session: want invalidParams, got %+v", e)
	}
	if f.startedTurns() != 0 {
		t.Fatal("a rejected prompt must not start a turn")
	}
}

func TestSessionPromptStreamGolden(t *testing.T) {
	f := &fakeWackypub{script: []*agentv1.AddAndGenerateTurnStreamResponse{
		{Text: "Hel"},
		{Text: "lo"},
		{ToolCall: &agentv1.ToolCall{CallId: "c1", ToolName: "run_command", ArgsSummary: `{"cmd":"ls"}`}},
		{ToolCallUpdate: &agentv1.ToolCallUpdate{CallId: "c1", ToolName: "run_command", Status: "completed", ResultHead: "ok"}},
		{Usage: &agentv1.TurnUsage{PromptTokens: 1, CompletionTokens: 2, TotalTokens: 3}},
	}}
	folder := t.TempDir()
	b := NewBackend("agent1", filepath.Dir(folder), folder, f, nil)
	c := startServer(t, b, folder)

	res, e := c.request(t, acp.MethodSessionPrompt, map[string]any{
		"sessionId": SessionID(folder),
		"prompt": []any{
			map[string]any{"type": "text", "text": "say hi"},
			map[string]any{"type": "resource_link", "uri": "file:///tmp/a.txt", "name": "a.txt"},
		},
	})
	if e != nil {
		t.Fatalf("prompt: %+v", e)
	}
	if res["stopReason"] != "end_turn" {
		t.Fatalf("stopReason: got %v want end_turn", res["stopReason"])
	}
	usage, _ := res["usage"].(map[string]any)
	if usage == nil || usage["totalTokens"] != float64(3) || usage["inputTokens"] != float64(1) || usage["outputTokens"] != float64(2) {
		t.Fatalf("usage: got %v", res["usage"])
	}

	ups := c.updatesSeen()
	if len(ups) != 4 {
		t.Fatalf("want 4 updates, got %d: %+v", len(ups), ups)
	}
	if ups[0]["sessionUpdate"] != acp.UpdateKindAgentMessageChunk || chunkText(ups[0]) != "Hel" {
		t.Errorf("update 0: %+v", ups[0])
	}
	if chunkText(ups[1]) != "lo" {
		t.Errorf("update 1: %+v", ups[1])
	}
	if ups[2]["sessionUpdate"] != acp.UpdateKindToolCall || ups[2]["toolCallId"] != "c1" || ups[2]["status"] != acp.ToolStatusPending {
		t.Errorf("update 2: %+v", ups[2])
	}
	ri, _ := ups[2]["rawInput"].(map[string]any)
	if ri == nil || ri["cmd"] != "ls" {
		t.Errorf("update 2 rawInput: %v", ups[2]["rawInput"])
	}
	if ups[3]["sessionUpdate"] != acp.UpdateKindToolCallUpdate || ups[3]["status"] != acp.ToolStatusCompleted {
		t.Errorf("update 3: %+v", ups[3])
	}

	req := f.lastPromptReq()
	if req == nil {
		t.Fatal("no turn recorded on the backend")
	}
	if want := "say hi\n[resource: a.txt (file:///tmp/a.txt)]"; req.GetUserMessage() != want {
		t.Errorf("user message: got %q want %q", req.GetUserMessage(), want)
	}
	md := req.GetA2AMetadata()
	if md == nil || md.GetCallerId() != "wackyacp-serve" {
		t.Errorf("a2a metadata: %+v", md)
	}
	if req.GetWorkspaceDir() != filepath.Dir(folder) {
		t.Errorf("workspace dir: got %q", req.GetWorkspaceDir())
	}
	if req.GetAgentId() != "agent1" {
		t.Errorf("agent id: got %q", req.GetAgentId())
	}
}

func TestSessionPromptBusyAndCancel(t *testing.T) {
	f := &fakeWackypub{block: &blocker{done: make(chan struct{})}}
	folder := t.TempDir()
	b := NewBackend("agent1", filepath.Dir(folder), folder, f, nil)
	c := startServer(t, b, folder)
	want := SessionID(folder)

	// The first prompt goes in flight and is held open; a second prompter
	// gets the structured busy error, not a stream.
	id1 := c.send(t, acp.MethodSessionPrompt, promptParams(want, "slow"))
	waitFor(t, "first prompt to begin", func() bool { return f.startedTurns() >= 1 })

	id2 := c.send(t, acp.MethodSessionPrompt, promptParams(want, "second"))
	_, e := c.readResponse(t, id2)
	if e == nil || e.Code != -32000 {
		t.Fatalf("want busy error -32000, got %+v", e)
	}
	if f.startedTurns() != 1 {
		t.Fatalf("busy prompt must not start a turn: %d", f.startedTurns())
	}

	// Cancel settles the in-flight turn as cancelled.
	c.notify(t, acp.MethodSessionCancel, map[string]any{"sessionId": want})
	res, e := c.readResponse(t, id1)
	if e != nil {
		t.Fatalf("first prompt: %+v", e)
	}
	if res["stopReason"] != "cancelled" {
		t.Fatalf("stopReason: got %v want cancelled", res["stopReason"])
	}
	if f.cancelCount() < 1 {
		t.Fatal("CancelTurn must have reached the backend")
	}

	// After the cancel, the agent admits a new prompt.
	res, e = c.request(t, acp.MethodSessionPrompt, promptParams(want, "again"))
	if e != nil {
		t.Fatalf("post-cancel prompt: %+v", e)
	}
	if res["stopReason"] != "end_turn" {
		t.Fatalf("post-cancel stopReason: got %v want end_turn", res["stopReason"])
	}
}

func TestSessionClose(t *testing.T) {
	f := &fakeWackypub{block: &blocker{done: make(chan struct{})}}
	folder := t.TempDir()
	b := NewBackend("agent1", filepath.Dir(folder), folder, f, nil)
	c := startServer(t, b, folder)
	want := SessionID(folder)

	res, e := c.request(t, acp.MethodSessionClose, map[string]any{"sessionId": want})
	if e != nil {
		t.Fatalf("close (no turn): %+v", e)
	}
	if len(res) != 0 {
		t.Fatalf("close must respond with an empty object, got %v", res)
	}

	// With a turn in flight on this connection, close cancels it.
	id1 := c.send(t, acp.MethodSessionPrompt, promptParams(want, "slow"))
	waitFor(t, "prompt to begin", func() bool { return f.startedTurns() >= 1 })

	_, e = c.request(t, acp.MethodSessionClose, map[string]any{"sessionId": want})
	if e != nil {
		t.Fatalf("close (with turn): %+v", e)
	}
	res, e = c.readResponse(t, id1)
	if e != nil {
		t.Fatalf("prompt: %+v", e)
	}
	if res["stopReason"] != "cancelled" {
		t.Fatalf("stopReason after close: got %v want cancelled", res["stopReason"])
	}
}

// TestSessionCancelAsRequest pins the defensive form: a non-conformant
// client sends session/cancel WITH an id (a request, not a notification).
// It must be handled exactly like the notification form and answered with
// an empty object - not dropped and not answered method-not-found.
func TestSessionCancelAsRequest(t *testing.T) {
	f := &fakeWackypub{block: &blocker{done: make(chan struct{})}}
	folder := t.TempDir()
	b := NewBackend("agent1", filepath.Dir(folder), folder, f, nil)
	c := startServer(t, b, folder)
	want := SessionID(folder)

	id1 := c.send(t, acp.MethodSessionPrompt, promptParams(want, "slow"))
	waitFor(t, "prompt to begin", func() bool { return f.startedTurns() >= 1 })

	cid := c.send(t, acp.MethodSessionCancel, map[string]any{"sessionId": want})
	res, e := c.readResponse(t, cid)
	if e != nil {
		t.Fatalf("cancel-as-request: %+v", e)
	}
	if len(res) != 0 {
		t.Fatalf("cancel-as-request must respond with an empty object, got %v", res)
	}
	if f.cancelCount() < 1 {
		t.Fatal("CancelTurn must have reached the backend")
	}

	res, e = c.readResponse(t, id1)
	if e != nil {
		t.Fatalf("prompt: %+v", e)
	}
	if res["stopReason"] != "cancelled" {
		t.Fatalf("stopReason: got %v want cancelled", res["stopReason"])
	}
}

func TestDisconnectCancelsOwnedTurn(t *testing.T) {
	f := &fakeWackypub{block: &blocker{done: make(chan struct{})}}
	folder := t.TempDir()
	b := NewBackend("agent1", filepath.Dir(folder), folder, f, nil)
	c := startServer(t, b, folder)

	id1 := c.send(t, acp.MethodSessionPrompt, promptParams(SessionID(folder), "slow"))
	waitFor(t, "prompt to begin", func() bool { return f.startedTurns() >= 1 })

	// The frontend dies mid-turn: the connection EOF must cancel the turn
	// this connection owns.
	_ = c.w.Close()
	waitFor(t, "CancelTurn on disconnect", func() bool { return f.cancelCount() >= 1 })

	// The prompt's response still arrives on the reply pipe, whose write
	// side the server keeps open until the turn settles.
	res, e := c.readResponse(t, id1)
	if e != nil {
		t.Fatalf("prompt: %+v", e)
	}
	if res["stopReason"] != "cancelled" {
		t.Fatalf("stopReason after disconnect: got %v want cancelled", res["stopReason"])
	}
}

func TestUnknownMethodAndMalformedLine(t *testing.T) {
	f := &fakeWackypub{}
	folder := t.TempDir()
	b := NewBackend("agent1", filepath.Dir(folder), folder, f, nil)
	c := startServer(t, b, folder)

	_, e := c.request(t, "session/set_mode", map[string]any{"sessionId": "x"})
	if e == nil || e.Code != acp.CodeMethodNotFound {
		t.Fatalf("unknown method: want method-not-found, got %+v", e)
	}
	if !strings.Contains(e.Message, "session/set_mode") {
		t.Errorf("method-not-found message should name the method: %s", e.Message)
	}

	if _, err := c.w.Write([]byte("this is not json\n")); err != nil {
		t.Fatalf("write malformed: %v", err)
	}
	// The parse-error response carries no usable id (id null): read the
	// raw frame instead of going through readResponse.
	for c.scanner.Scan() {
		line := c.scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var frame map[string]json.RawMessage
		if err := json.Unmarshal(line, &frame); err != nil {
			t.Fatalf("malformed frame from server: %v (line %q)", err, line)
		}
		errRaw, ok := frame["error"]
		if !ok {
			t.Fatalf("want an error frame, got %s", line)
		}
		var pe RPCError
		if err := json.Unmarshal(errRaw, &pe); err != nil {
			t.Fatalf("malformed error: %v", err)
		}
		if pe.Code != acp.CodeParseError {
			t.Fatalf("parse error code: got %+v", pe)
		}
		return
	}
	t.Fatalf("no parse-error frame; scanner: %v", c.scanner.Err())
}

// TestCancelInFlightIsIdempotent pins the double-cancel safety: the same turn
// can be cancelled from the session/cancel notification, session/close, and
// connection disconnect concurrently. Only the first cancel may close the
// channel; later ones must be no-ops, not a close-of-closed-channel panic.
func TestCancelInFlightIsIdempotent(t *testing.T) {
	f := &fakeWackypub{block: &blocker{done: make(chan struct{})}}
	b := NewBackend("agent1", "/tmp", t.TempDir(), f, nil)
	ts, ok := b.BeginTurn(nil)
	if !ok {
		t.Fatal("BeginTurn")
	}
	ctx := context.Background()
	for i, want := range []bool{true, true, true} {
		if got := b.CancelInFlight(ctx); got != want {
			t.Fatalf("cancel %d: got %v want %v", i+1, got, want)
		}
	}
	if f.cancelCount() != 3 {
		t.Fatalf("each cancel must still send its CancelTurn RPC: got %d", f.cancelCount())
	}
	b.EndTurn(ts)
}

// TestPromptForceCancelFreesStream pins the pump-leak fix: when the
// force-cancel grace expires on a wedged stream, Prompt must return and the
// stream must be canceled with it, so the pump goroutine is not left blocked
// in Recv (CloseSend only half-closes; it never frees a pump).
func TestPromptForceCancelFreesStream(t *testing.T) {
	oldGrace := forceCancelGrace
	forceCancelGrace = 100 * time.Millisecond
	t.Cleanup(func() { forceCancelGrace = oldGrace })

	f := &fakeWackypub{block: &blocker{done: make(chan struct{})}} // never released: wedged agent
	b := NewBackend("agent1", "/tmp", t.TempDir(), f, nil)
	ts, ok := b.BeginTurn(nil)
	if !ok {
		t.Fatal("BeginTurn")
	}

	type promptResult struct {
		stop string
		err  error
	}
	done := make(chan promptResult, 1)
	start := time.Now()
	go func() {
		stop, _, err := b.Prompt(context.Background(), ts, "hang", func(Update) error { return nil })
		done <- promptResult{stop, err}
	}()
	waitFor(t, "prompt to begin", func() bool { return f.startedTurns() >= 1 })
	b.CancelInFlight(context.Background())

	var res promptResult
	select {
	case res = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Prompt did not return after the force-cancel grace expired")
	}
	elapsed := time.Since(start)
	if res.stop != "cancelled" || res.err != nil {
		t.Fatalf("stopReason=%q err=%v, want cancelled/nil", res.stop, res.err)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("grace expiry took %v", elapsed)
	}
	b.EndTurn(ts)

	// The stream must be canceled: a fresh Recv returns immediately instead of
	// blocking on the wedged stream, which is what frees the pump.
	st := f.stream()
	if st == nil {
		t.Fatal("no stream recorded")
	}
	recvd := make(chan error, 1)
	go func() { _, rerr := st.Recv(); recvd <- rerr }()
	select {
	case rerr := <-recvd:
		if rerr == nil {
			t.Fatal("Recv returned a nil error after cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream.Recv still blocked after Prompt returned: pump goroutine would leak")
	}
}
