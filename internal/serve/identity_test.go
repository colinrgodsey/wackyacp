package serve

import (
	"encoding/hex"
	"strings"
	"testing"
)

func TestSessionIDFormat(t *testing.T) {
	id := SessionID("/abs/path/to/agent")
	if !strings.HasPrefix(id, "wackyacp:") {
		t.Fatalf("missing prefix: %s", id)
	}
	body := strings.TrimPrefix(id, "wackyacp:")
	if len(body) != 16 {
		t.Fatalf("want 16 hex chars, got %d: %s", len(body), body)
	}
	if _, err := hex.DecodeString(body); err != nil {
		t.Fatalf("body is not hex: %v", err)
	}
}

func TestSessionIDStableAndDistinct(t *testing.T) {
	a := SessionID("/ws/alpha")
	if a != SessionID("/ws/alpha") {
		t.Fatal("same folder must give the same id")
	}
	if b := SessionID("/ws/beta"); b == a {
		t.Fatal("different folders must give different ids")
	}
	// Path normalization: trailing slashes and redundant elements are the
	// same path and must give the same id.
	if SessionID("/ws/alpha/") != a {
		t.Fatal("trailing slash must not change the id")
	}
	if SessionID("/ws/../ws/alpha") != a {
		t.Fatal("redundant elements must not change the id")
	}
}

func TestSessionIDForAgent(t *testing.T) {
	id := SessionIDForAgent("agent1")
	if !strings.HasPrefix(id, "wackyacp:") {
		t.Fatalf("missing prefix: %s", id)
	}
	if id != SessionIDForAgent("agent1") {
		t.Fatal("same agent id must give the same session id")
	}
	if id == SessionIDForAgent("agent2") {
		t.Fatal("different agent ids must give different session ids")
	}
	// The two derivations must stay disjoint: an agent-id-derived id must
	// never equal a folder-derived id, so a local serve and a remote serve
	// of the same agent never look like each other's sessions.
	if id == SessionID("agent1") || id == SessionID("agent:agent1") {
		t.Fatal("agent-id derivation collides with folder derivation")
	}
}
