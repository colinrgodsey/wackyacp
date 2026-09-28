package d112

import (
	"context"
	"strings"
	"testing"

	"errors"
	"github.com/colinrgodsey/wackyacp/internal/acp"
	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// TestD112_SetModelConfig_HarnessRejectionMapsToInvalidArgument pins phoebe's error-wrapping
// nit: an ACP harness rejection (unknown model / unsupported config id) is a CLIENT error -
// the bridge must surface codes.InvalidArgument (not Internal) and keep the chain so
// errors.As to *acp.ConfigOptionError / errors.Is against ErrTurnFailed still work.
func TestD112_SetModelConfig_HarnessRejectionMapsToInvalidArgument(t *testing.T) {
	driver := &mockACPDriver{
		setConfigErr: &acp.ConfigOptionError{Code: -32602, Message: "unsupported configId: temperature"},
	}
	srv := NewServer(driver, "sess-abc", "/path/to/agent")

	_, err := srv.SetModelConfig(context.Background(), &agentv1.SetModelConfigRequest{
		AgentId: "bridgedagent",
		Model:   "temperature",
	})
	if err == nil {
		t.Fatal("expected harness rejection error, got nil")
	}
	if st, ok := status.FromError(err); ok {
		if st.Code() != codes.InvalidArgument {
			t.Errorf("harness rejection code = %v, want InvalidArgument", st.Code())
		}
	} else {
		t.Fatalf("expected a status error, got: %v", err)
	}
	// status errors cannot carry error chains, so the ConfigOptionError reachability is
	// asserted at the CLIENT layer (TestClient_SetConfigOption_UnsupportedConfigID...);
	// here we pin that the mapping fired and surfaced the harness's explanation.
	if !strings.Contains(err.Error(), "session/setConfigOption: [-32602]") {
		t.Errorf("rejection message should carry the harness code/message, got %v", err)
	}
}

// TestD112_SetModelConfig_OtherDriverFailureStaysInternal pins that non-harness driver
// failures (transport, unexpected) remain codes.Internal with the cause reachable.
func TestD112_SetModelConfig_OtherDriverFailureStaysInternal(t *testing.T) {
	driver := &mockACPDriver{setConfigErr: errors.New("transport broke")}
	srv := NewServer(driver, "sess-abc", "/path/to/agent")

	_, err := srv.SetModelConfig(context.Background(), &agentv1.SetModelConfigRequest{
		AgentId: "bridgedagent",
		Model:   "sonnet",
	})
	if err == nil {
		t.Fatal("expected driver failure error, got nil")
	}
	if st, ok := status.FromError(err); ok {
		if st.Code() != codes.Internal {
			t.Errorf("plain driver failure code = %v, want Internal", st.Code())
		}
	} else {
		t.Fatalf("expected a status error, got: %v", err)
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
