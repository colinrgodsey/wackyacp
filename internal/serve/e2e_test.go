package serve

import (
	"context"
	"testing"

	"github.com/colinrgodsey/wackyacp/internal/acp"
	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
)

// TestE2EFrontendToWackypub runs the full inverted-ACP stack: a real stdio
// child process (the wackypub protocol side, scripted) behind the Dialer, the
// ACP server over pipes, and golden client round-trips on top. This is the
// shape of a real frontend (e.g. Zed) connecting to `wackyacp serve`.
func TestE2EFrontendToWackypub(t *testing.T) {
	child := testChild(t)
	wsDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	d := NewDialer(ctx, child, wsDir)
	t.Cleanup(func() { _ = d.Close() })
	gc := stdioConn(t, d)

	b := NewBackend("agent1", wsDir, wsDir, NewGRPCWackypub(agentv1.NewAgentServiceClient(gc)), nil)
	c := startServer(t, b, wsDir)

	res, e := c.request(t, acp.MethodInitialize, map[string]any{"protocolVersion": 1})
	if e != nil {
		t.Fatalf("initialize: %+v", e)
	}
	if _, ok := res["agentCapabilities"].(map[string]any); !ok {
		t.Fatalf("initialize: missing capabilities: %v", res)
	}

	res, e = c.request(t, acp.MethodSessionNew, map[string]any{"cwd": wsDir})
	if e != nil {
		t.Fatalf("session/new: %+v", e)
	}
	sessionID := res["sessionId"].(string)
	if sessionID != SessionID(wsDir) {
		t.Fatalf("sessionId: got %s want %s", sessionID, SessionID(wsDir))
	}

	// A live prompt against the real (scripted) child: the echo protocol
	// proves text chunks, the final usage chunk, and the stop reason across
	// the whole stack.
	res, e = c.request(t, acp.MethodSessionPrompt, promptParams(sessionID, "echo: A B C"))
	if e != nil {
		t.Fatalf("prompt: %+v", e)
	}
	if res["stopReason"] != "end_turn" {
		t.Fatalf("stopReason: got %v want end_turn", res["stopReason"])
	}
	usage, _ := res["usage"].(map[string]any)
	if usage == nil || usage["totalTokens"] != float64(3) {
		t.Fatalf("usage: got %v", res["usage"])
	}
	ups := c.updatesSeen()
	if len(ups) != 4 {
		t.Fatalf("want 4 text updates, got %d: %+v", len(ups), ups)
	}
	for i, want := range []string{"echo:", "A", "B", "C"} {
		if chunkText(ups[i]) != want {
			t.Errorf("update %d: got %q want %q", i, chunkText(ups[i]), want)
		}
	}

	// A second connection to the same backend (the TCP listener shape):
	// load replays the canned history from the child's ReadSession.
	c2 := startServer(t, b, wsDir)
	if _, e := c2.request(t, acp.MethodInitialize, map[string]any{"protocolVersion": 1}); e != nil {
		t.Fatalf("initialize (conn2): %+v", e)
	}
	if _, e := c2.request(t, acp.MethodSessionNew, map[string]any{"cwd": wsDir}); e != nil {
		t.Fatalf("session/new (conn2): %+v", e)
	}
	if _, e := c2.request(t, acp.MethodSessionLoad, map[string]any{"sessionId": sessionID}); e != nil {
		t.Fatalf("session/load (conn2): %+v", e)
	}
	ups2 := c2.updatesSeen()
	if len(ups2) != 4 {
		t.Fatalf("want 4 replay updates, got %d: %+v", len(ups2), ups2)
	}
	if ups2[0]["sessionUpdate"] != acp.UpdateKindUserMessageChunk || chunkText(ups2[0]) != "hello" {
		t.Errorf("replay update 0: %+v", ups2[0])
	}
	if ups2[1]["sessionUpdate"] != acp.UpdateKindAgentThoughtChunk {
		t.Errorf("replay update 1: %+v", ups2[1])
	}
	if ups2[2]["sessionUpdate"] != acp.UpdateKindToolCall || ups2[2]["toolCallId"] != "fc1" {
		t.Errorf("replay update 2: %+v", ups2[2])
	}
	if ups2[3]["sessionUpdate"] != acp.UpdateKindToolCallUpdate || ups2[3]["status"] != acp.ToolStatusCompleted {
		t.Errorf("replay update 3: %+v", ups2[3])
	}
}
