package acp

import (
	"context"
	"strings"
	"testing"
)

// TestClient_SetConfigOption_ForwardsAndParsesConfirmation drives the real client against
// the acpshimbin fixture: session/setConfigOption(cinfo "model", value) must return the
// harness's configOptions array verbatim, confirming the session-scoped model change.
func TestClient_SetConfigOption_ForwardsAndParsesConfirmation(t *testing.T) {
	client, proc := startShimClient(t, "")
	defer proc.Close()

	ctx := context.Background()
	if _, err := client.Initialize(ctx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	raw, err := client.SetConfigOption(ctx, "shim-session-", "model", "sonnet")
	if err != nil {
		t.Fatalf("SetConfigOption failed: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("expected non-empty configOptions result")
	}
	s := string(raw)
	if !strings.Contains(s, "sonnet") {
		t.Errorf("configOptions should confirm the requested model, got %s", s)
	}
	if !strings.Contains(s, "\"key\":\"model\"") {
		t.Errorf("configOptions should carry the model key, got %s", s)
	}
}

// TestClient_SetConfigOption_UnsupportedConfigIDSurfacesHarnessError pins that unknown
// config ids are NOT invented server-side - the harness error tunnels through.
func TestClient_SetConfigOption_UnsupportedConfigIDSurfacesHarnessError(t *testing.T) {
	client, proc := startShimClient(t, "")
	defer proc.Close()

	ctx := context.Background()
	if _, err := client.Initialize(ctx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	if _, err := client.SetConfigOption(ctx, "shim-session-", "temperature", "0.5"); err == nil {
		t.Fatal("expected error for unsupported configId, got nil")
	}
}
