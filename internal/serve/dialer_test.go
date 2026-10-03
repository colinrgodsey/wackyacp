package serve

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	childOnce sync.Once
	childBin  string
	childErr  error
)

// testChild builds the scripted stdio child (testdata/stdiochild) once per
// test binary, in place of a real wackypub.
func testChild(t *testing.T) string {
	t.Helper()
	childOnce.Do(func() {
		dir, err := os.MkdirTemp("", "wa-stdiochild-")
		if err != nil {
			childErr = err
			return
		}
		childBin = filepath.Join(dir, "stdiochild")
		out, err := exec.Command("go", "build", "-o", childBin, "../../testdata/stdiochild").CombinedOutput()
		if err != nil {
			childErr = fmt.Errorf("building testdata/stdiochild: %v\n%s", err, out)
			return
		}
	})
	if childErr != nil {
		t.Fatalf("%v", childErr)
	}
	return childBin
}

// stdioConn wires a gRPC client over the dialer, mirroring the production
// wiring: no idle timeout (the reaper would kill the child), a fast initial
// backoff so a test sees the respawn quickly. MinConnectTimeout must be set
// explicitly: WithConnectParams replaces it with the ConnectParams field, and
// a zero field collapses each connect attempt's deadline to the backoff delay
// - too short for a Go child to even write its preface.
func stdioConn(t *testing.T, d *Dialer) *grpc.ClientConn {
	t.Helper()
	gc, err := grpc.NewClient("passthrough:///stdio",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(d.Dial),
		grpc.WithIdleTimeout(0),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 5 * time.Millisecond, Multiplier: 1.5, Jitter: 0.2, MaxDelay: time.Second},
			MinConnectTimeout: 20 * time.Second,
		}),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = gc.Close() })
	return gc
}

func listAgents(t *testing.T, gc *grpc.ClientConn) []string {
	t.Helper()
	ids, err := listAgentsOnce(t, gc)
	if err != nil {
		t.Fatalf("ListAgents: %v", err)
	}
	return ids
}

func listAgentsOnce(t *testing.T, gc *grpc.ClientConn) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := agentv1.NewAgentServiceClient(gc).ListAgents(ctx, &agentv1.ListAgentsRequest{})
	if err != nil {
		return nil, err
	}
	return resp.GetAgentIds(), nil
}

func TestDialerRespawnsAfterChildDeath(t *testing.T) {
	child := testChild(t)
	wsDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	d := NewDialer(ctx, child, wsDir)
	t.Cleanup(func() { _ = d.Close() })
	gc := stdioConn(t, d)

	ids := listAgents(t, gc)
	if len(ids) != 1 || !strings.HasPrefix(ids[0], "child") {
		t.Fatalf("ListAgents: got %v", ids)
	}
	cmd1 := d.ActiveCmd()
	if cmd1 == nil || cmd1.Process == nil {
		t.Fatal("no active child after the first RPC")
	}
	pid1 := cmd1.Process.Pid

	// Kill the child: the transport breaks, and the next RPC must respawn a
	// fresh child and heal.
	if err := cmd1.Process.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	waitFor(t, "the dead child to be reaped", func() bool { return !d.ChildAlive() })

	// The RPC racing the kill can hit the dying transport; retry until a
	// fresh child answers (bounded, like a user retrying).
	var ids2 []string
	var err error
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ids2, err = listAgentsOnce(t, gc)
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("no ListAgents succeeded after the kill (last error: %v)", err)
	}
	if len(ids2) != 1 || !strings.HasPrefix(ids2[0], "child") {
		t.Fatalf("ListAgents after death: got %v", ids2)
	}
	cmd2 := d.ActiveCmd()
	if cmd2 == nil || cmd2.Process == nil {
		t.Fatal("no active child after respawn")
	}
	if cmd2.Process.Pid == pid1 {
		t.Fatalf("respawned child reused the dead pid %d", pid1)
	}
}

func TestDialerFailedSpawnLeavesNoHandle(t *testing.T) {
	wsDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	d := NewDialer(ctx, filepath.Join(wsDir, "no-such-bin"), wsDir)
	t.Cleanup(func() { _ = d.Close() })
	gc := stdioConn(t, d)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	if _, err := agentv1.NewAgentServiceClient(gc).ListAgents(ctx2, &agentv1.ListAgentsRequest{}); err == nil {
		t.Fatal("want an RPC error, got nil")
	}
	if d.LastDialError() == nil {
		t.Fatal("want a recorded spawn error")
	}
	if c := d.ActiveCmd(); c != nil && c.Process != nil {
		t.Fatalf("failed spawn left a child handle: %v", c)
	}
}
