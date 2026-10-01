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
