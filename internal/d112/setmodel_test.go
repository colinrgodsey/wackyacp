package d112

import (
	"context"
	"strings"
	"testing"

	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
)

// TestSetModelConfig_ForwardsToHarness pins the session-scoped passthrough: the bridged
// session's model change is forwarded to the harness's session/setConfigOption with the
// driver session id and the requested model, and the harness's configOptions confirmation
// is returned verbatim.
func TestD112_SetModelConfig_ForwardsToHarness(t *testing.T) {
	driver := &mockACPDriver{}
	srv := NewServer(driver, "sess-abc", "/path/to/agent")

	resp, err := srv.SetModelConfig(context.Background(), &agentv1.SetModelConfigRequest{
		AgentId: "bridgedagent",
		Model:   "sonnet",
	})
	if err != nil {
		t.Fatalf("SetModelConfig: %v", err)
	}
	if driver.setConfigCalls != 1 {
		t.Fatalf("driver.SetConfigOption called %d times, want 1", driver.setConfigCalls)
	}
	if driver.setConfigValue != "sonnet" {
		t.Errorf("driver.SetConfigOption value = %q, want sonnet", driver.setConfigValue)
	}
	if resp.GetModel() != "sonnet" {
		t.Errorf("response model = %q, want sonnet", resp.GetModel())
	}
	if !strings.Contains(resp.GetConfigOptions(), "sonnet") {
		t.Errorf("config_options should carry the harness confirmation, got %q", resp.GetConfigOptions())
	}
}

// TestD112_SetModelConfig_EmptyModelRejected pins validation before any harness call.
func TestD112_SetModelConfig_EmptyModelRejected(t *testing.T) {
	driver := &mockACPDriver{}
	srv := NewServer(driver, "sess-abc", "/path/to/agent")

	if _, err := srv.SetModelConfig(context.Background(), &agentv1.SetModelConfigRequest{
		AgentId: "bridgedagent",
	}); err == nil {
		t.Fatal("expected empty-model error, got nil")
	}
	if driver.setConfigCalls != 0 {
		t.Fatalf("driver.SetConfigOption should not be called with empty model, got %d calls", driver.setConfigCalls)
	}
}
