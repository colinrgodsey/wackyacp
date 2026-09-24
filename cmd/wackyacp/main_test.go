package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/colinrgodsey/wackyacp/internal/session"
	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
	"github.com/colinrgodsey/wackypub/pkg/stdio"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var (
	e2eMarker = fmt.Sprintf("wackyacp-e2e-canary-%d-%d", os.Getpid(), time.Now().UnixNano())

	wackyacpBinOnce sync.Once
	wackyacpBinPath string

	shimBinOnce sync.Once
	shimBinPath string
)

func newTestAgentFolder(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), e2eMarker)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("creating test agent folder: %v", err)
	}
	return dir
}

func getWackyacpBin(t *testing.T) string {
	wackyacpBinOnce.Do(func() {
		tmpDir, err := os.MkdirTemp("", "wackyacp-test-*")
		if err != nil {
			t.Fatalf("creating temp dir: %v", err)
		}
		outPath := filepath.Join(tmpDir, "wackyacp")
		cmd := exec.Command("go", "build", "-o", outPath, ".")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("building wackyacp failed: %v\n%s", err, string(out))
		}
		wackyacpBinPath = outPath
	})
	return wackyacpBinPath
}

func getShimBin(t *testing.T) string {
	shimBinOnce.Do(func() {
		tmpDir, err := os.MkdirTemp("", "acpshimbin-test-*")
		if err != nil {
			t.Fatalf("creating temp dir: %v", err)
		}
		outPath := filepath.Join(tmpDir, "acpshimbin")
		cmd := exec.Command("go", "build", "-o", outPath, "../../testdata/acpshimbin")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("building acpshimbin failed: %v\n%s", err, string(out))
		}
		shimBinPath = outPath
	})
	return shimBinPath
}

func spawnBridge(t *testing.T, ctx context.Context, agentFolder, script string) (agentv1.AgentServiceClient, func()) {
	wackyacpBin := getWackyacpBin(t)
	shimBin := getShimBin(t)

	args := []string{
		"--agent-folder=" + agentFolder,
		"--harness-cmd=" + shimBin,
		"--harness-args=--script=" + script + " -marker=" + e2eMarker,
	}

	cmd := exec.CommandContext(ctx, wackyacpBin, args...)
	// The shared pkg/stdio DialCommand owns the pipes, Setsid process-group
	// isolation, and SIGTERM/SIGKILL escalation on Close (killed client cannot
	// orphan the bridge).
	conn, err := stdio.DialCommand(ctx, cmd, 2*time.Second)
	if err != nil {
		t.Fatalf("DialCommand: %v", err)
	}

	gc, err := grpc.NewClient(
		"passthrough:///wackyacp",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return conn, nil
		}),
	)
	if err != nil {
		_ = conn.Close()
		t.Fatalf("grpc.NewClient failed: %v", err)
	}

	client := agentv1.NewAgentServiceClient(gc)
	cleanup := func() {
		_ = gc.Close()
		_ = conn.Close()
	}

	t.Cleanup(cleanup)

	return client, cleanup
}

func TestE2E_AddAndGenerateTurnStream_UsageAndText(t *testing.T) {
	agentFolder := newTestAgentFolder(t)
	ctx := context.Background()

	client, cleanup := spawnBridge(t, ctx, agentFolder, "emit-usage")
	defer cleanup()

	stream, err := client.AddAndGenerateTurnStream(ctx, &agentv1.AddAndGenerateTurnStreamRequest{
		AgentId:     "test-agent",
		UserMessage: "hello usage",
	})
	if err != nil {
		t.Fatalf("AddAndGenerateTurnStream failed: %v", err)
	}

	var chunks []string
	var finalUsage *agentv1.TurnUsage

	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("stream.Recv error: %v", err)
		}
		if resp.Text != "" {
			chunks = append(chunks, resp.Text)
		}
		if resp.Usage != nil {
			finalUsage = resp.Usage
		}
	}

	if len(chunks) == 0 {
		t.Fatalf("expected streamed text chunks, got none")
	}
	if finalUsage == nil {
		t.Fatalf("expected final TurnUsage chunk, got nil")
	}
	if finalUsage.TotalTokens != 150 {
		t.Errorf("expected TotalTokens 150, got %d", finalUsage.TotalTokens)
	}

	// Verify session file was written to agentFolder
	s, err := session.ReadSession(agentFolder)
	if err != nil {
		t.Fatalf("ReadSession failed: %v", err)
	}
	if s == nil || s.SessionID == "" {
		t.Fatalf("expected non-empty persisted session")
	}
	if s.AgentFolder != agentFolder {
		t.Errorf("expected AgentFolder %s, got %s", agentFolder, s.AgentFolder)
	}
}

func TestE2E_PermissionAutoDeny(t *testing.T) {
	agentFolder := newTestAgentFolder(t)
	ctx := context.Background()

	client, cleanup := spawnBridge(t, ctx, agentFolder, "hang-on-permission")
	defer cleanup()

	stream, err := client.AddAndGenerateTurnStream(ctx, &agentv1.AddAndGenerateTurnStreamRequest{
		AgentId:     "test-agent",
		UserMessage: "run permission check",
	})
	if err != nil {
		t.Fatalf("AddAndGenerateTurnStream failed: %v", err)
	}

	var warnings []string
	var chunks []string

	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("stream.Recv error: %v", err)
		}
		if resp.Text != "" {
			chunks = append(chunks, resp.Text)
		}
		if resp.Warning != "" {
			warnings = append(warnings, resp.Warning)
		}
	}

	if len(warnings) == 0 {
		t.Fatalf("expected warning for auto-denied permission, got none")
	}
	if len(chunks) == 0 {
		t.Fatalf("expected outcome chunk after auto-deny, got none")
	}
}

func TestE2E_SessionFallbackChain(t *testing.T) {
	agentFolder := newTestAgentFolder(t)
	ctx := context.Background()

	// 1. Initial run creates session with "normal"
	client1, cleanup1 := spawnBridge(t, ctx, agentFolder, "normal")
	resp1, err := client1.GenerateTurn(ctx, &agentv1.GenerateTurnRequest{
		AgentId: "test-agent",
	})
	if err != nil {
		t.Fatalf("GenerateTurn 1 failed: %v", err)
	}
	cleanup1()

	if resp1.Text == "" {
		t.Errorf("expected non-empty text")
	}

	s1, err := session.ReadSession(agentFolder)
	if err != nil || s1 == nil {
		t.Fatalf("expected persisted session after first turn")
	}
	originalSessionID := s1.SessionID

	// 2. Second run with resume-ok should resume the existing session
	client2, cleanup2 := spawnBridge(t, ctx, agentFolder, "resume-ok")
	_, err = client2.GenerateTurn(ctx, &agentv1.GenerateTurnRequest{
		AgentId: "test-agent",
	})
	if err != nil {
		t.Fatalf("GenerateTurn 2 failed: %v", err)
	}
	cleanup2()

	s2, _ := session.ReadSession(agentFolder)
	if s2.SessionID != originalSessionID {
		t.Errorf("expected session to be preserved on resume, got %s != %s", s2.SessionID, originalSessionID)
	}

	// 3. Run with fail-load: resume fails, load fails, should create new session
	client3, cleanup3 := spawnBridge(t, ctx, agentFolder, "fail-load")
	_, err = client3.GenerateTurn(ctx, &agentv1.GenerateTurnRequest{
		AgentId: "test-agent",
	})
	if err != nil {
		t.Fatalf("GenerateTurn 3 failed: %v", err)
	}
	cleanup3()

	s3, _ := session.ReadSession(agentFolder)
	if s3.SessionID == originalSessionID {
		t.Errorf("expected new session after fail-load fallback, but stayed %s", originalSessionID)
	}
}

func TestE2E_UnimplementedRPCs(t *testing.T) {
	agentFolder := newTestAgentFolder(t)
	ctx := context.Background()

	client, cleanup := spawnBridge(t, ctx, agentFolder, "normal")
	defer cleanup()

	_, err := client.AddMedia(ctx, &agentv1.AddMediaRequest{
		AgentId: "test-agent",
	})
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("expected Unimplemented for AddMedia, got: %v", err)
	}

	_, err = client.CompactSession(ctx, &agentv1.CompactSessionRequest{
		AgentId: "test-agent",
	})
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("expected Unimplemented for CompactSession, got: %v", err)
	}
}

func TestE2E_FlagValidations(t *testing.T) {
	bin := getWackyacpBin(t)

	// 1. Missing agent-folder
	cmd1 := exec.Command(bin, "--harness-cmd=echo")
	out1, err1 := cmd1.CombinedOutput()
	if err1 == nil {
		t.Fatalf("expected error for missing agent-folder")
	}
	if !strings.Contains(string(out1), "--agent-folder is required") {
		t.Errorf("unexpected output: %s", string(out1))
	}

	// 2. Relative agent-folder (must be rejected - no CWD fallback!)
	cmd2 := exec.Command(bin, "--agent-folder=relative/path", "--harness-cmd=echo")
	out2, err2 := cmd2.CombinedOutput()
	if err2 == nil {
		t.Fatalf("expected error for relative agent-folder")
	}
	if !strings.Contains(string(out2), "no CWD fallback allowed") {
		t.Errorf("unexpected output: %s", string(out2))
	}

	// 3. Missing harness-cmd
	cmd3 := exec.Command(bin, "--agent-folder=/abs/path")
	out3, err3 := cmd3.CombinedOutput()
	if err3 == nil {
		t.Fatalf("expected error for missing harness-cmd")
	}
	if !strings.Contains(string(out3), "--harness-cmd is required") {
		t.Errorf("unexpected output: %s", string(out3))
	}

	// 4. Nonexistent harness-cmd
	cmd4 := exec.Command(bin, "--agent-folder=/abs/path", "--harness-cmd=nonexistent-binary-99999")
	out4, err4 := cmd4.CombinedOutput()
	if err4 == nil {
		t.Fatalf("expected error for nonexistent harness-cmd")
	}
	if !strings.Contains(string(out4), "harness command not found") {
		t.Errorf("unexpected output: %s", string(out4))
	}
}

func TestE2E_LockDiscipline_SerializesTurns(t *testing.T) {
	agentFolder := newTestAgentFolder(t)
	ctx := context.Background()

	// 1. Pre-take lock manually
	lock, err := session.AcquireLock(ctx, agentFolder)
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}
	defer func() {
		_ = lock.Release()
	}()

	wackyacpBin := getWackyacpBin(t)
	shimBin := getShimBin(t)

	// 2. Start wackyacp in background; it should block on lock
	start := time.Now()
	doneCh := make(chan error, 1)

	cmd := exec.Command(wackyacpBin,
		"--agent-folder="+agentFolder,
		"--harness-cmd="+shimBin,
		"--harness-args=-marker="+e2eMarker,
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin pipe failed: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe failed: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("cmd.Start failed: %v", err)
	}

	cmdExited := make(chan struct{})
	go func() {
		// Close stdin so wackyacp exits once it acquires lock and starts D112
		_ = stdin.Close()
		_, _ = io.ReadAll(stdout)
		doneCh <- cmd.Wait()
		close(cmdExited)
	}()

	t.Cleanup(func() {
		_ = stdin.Close()
		_ = stdout.Close()
		select {
		case <-cmdExited:
			return
		default:
			if cmd.Process != nil {
				pgid, err := syscall.Getpgid(cmd.Process.Pid)
				if err != nil {
					pgid = cmd.Process.Pid
				}
				_ = syscall.Kill(-pgid, syscall.SIGTERM)
				select {
				case <-cmdExited:
				case <-time.After(200 * time.Millisecond):
					_ = syscall.Kill(-pgid, syscall.SIGKILL)
					<-cmdExited
				}
			}
		}
	})

	// 3. Hold lock for 200ms, then release
	time.Sleep(200 * time.Millisecond)
	_ = lock.Release()

	// 4. Wait for wackyacp to finish
	select {
	case err := <-doneCh:
		elapsed := time.Since(start)
		if elapsed < 200*time.Millisecond {
			t.Errorf("expected process to wait at least 200ms for lock, elapsed: %v", elapsed)
		}
		if err != nil {
			t.Logf("process exited with: %v (clean EOF exit)", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for wackyacp to acquire lock and exit")
	}
}

func TestE2E_HarnessIgnoreEOF_ExitsWithinWaitDelay(t *testing.T) {
	agentFolder := newTestAgentFolder(t)
	ctx := context.Background()

	// Spawn wackyacp with harness script ignore-eof (which simulates a harness that doesn't exit on stdin EOF)
	client, cleanup := spawnBridge(t, ctx, agentFolder, "ignore-eof")

	// Execute a normal turn over D112 to ensure the bridge and harness are fully operational
	stream, err := client.AddAndGenerateTurnStream(ctx, &agentv1.AddAndGenerateTurnStreamRequest{
		AgentId:     "test-agent",
		UserMessage: "hello ignore eof",
	})
	if err != nil {
		cleanup()
		t.Fatalf("AddAndGenerateTurnStream failed: %v", err)
	}

	for {
		_, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			cleanup()
			t.Fatalf("unexpected stream error: %v", err)
		}
	}

	// Normal turn completed. Now close client connection (closing stdin to wackyacp).
	// wackyacp should terminate the uncooperative harness and exit within WaitDelay bounds.
	cleanupStart := time.Now()
	done := make(chan struct{})
	go func() {
		cleanup()
		close(done)
	}()

	select {
	case <-done:
		elapsed := time.Since(cleanupStart)
		t.Logf("wackyacp exited in %v with uncooperative harness", elapsed)
		// WaitDelay is 5s; with SIGTERM escalation it exits within ~100ms. Bound check at 6s.
		if elapsed > 6*time.Second {
			t.Errorf("cleanup took %v, exceeded WaitDelay bound", elapsed)
		}
	case <-time.After(8 * time.Second):
		t.Fatalf("wackyacp failed to exit within WaitDelay bounds when harness ignored EOF")
	}
}

// TestE2E_ConcurrentDispatches_WaitThenSucceed re-verifies the
// bugs/wackyacp/lock-wait-visibility diagnosis end-to-end: D117 has an exclusive
// flock on acp-session.lock already serializes two concurrent bridge processes.
// The second blocks in AcquireLock at process start (observable via the
// "waiting for" stderr line once) and proceeds after the first bridge process
// exits - NOT corruption.
func TestE2E_ConcurrentDispatches_WaitThenSucceed(t *testing.T) {
	agentFolder := newTestAgentFolder(t)
	wackyacpBin := getWackyacpBin(t)
	shimBin := getShimBin(t)

	// Bridge 1: hold-turn keeps the shim alive ~1s so the flock is observably held.
	// The flock is held for the whole bridge PROCESS lifetime (released when the
	// process exits, i.e. when its stdin EOFs via conn close).
	cmd1 := exec.Command(wackyacpBin, "--agent-folder="+agentFolder, "--harness-cmd="+shimBin, "--harness-args=--script=hold-turn -marker="+e2eMarker)
	conn1, err := stdio.DialCommand(context.Background(), cmd1, 2*time.Second)
	if err != nil {
		t.Fatalf("start bridge1: %v", err)
	}
	g1, _ := grpc.NewClient("passthrough:///w1", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return conn1, nil }))
	client1 := agentv1.NewAgentServiceClient(g1)
	t.Cleanup(func() { _ = g1.Close(); _ = conn1.Close() })

	// Fire bridge1 turn; it blocks ~1s in the shim holding the lock.
	turn1Done := make(chan struct{})
	go func() {
		stream, err := client1.AddAndGenerateTurnStream(context.Background(), &agentv1.AddAndGenerateTurnStreamRequest{AgentId: "agent-a", UserMessage: "first"})
		if err != nil {
			t.Errorf("turn1 stream open failed: %v", err)
			return
		}
		for {
			_, rerr := stream.Recv()
			if rerr != nil {
				close(turn1Done)
				return
			}
		}
	}()

	// Let bridge1 acquire the lock and get into its held turn.
	time.Sleep(300 * time.Millisecond)

	// Bridge 2 on the SAME agent folder: blocks in AcquireLock at startup; the D112
	// server (and therefore the grpc handshake) only comes up after the lock is free.
	stderrGuard := &guardBuffer{mu: &stderrMu, buf: &stderr2}
	cmd2 := exec.Command(wackyacpBin, "--agent-folder="+agentFolder, "--harness-cmd="+shimBin, "--harness-args=--script=normal -marker="+e2eMarker)
	cmd2.Stderr = stderrGuard
	conn2, err := stdio.DialCommand(context.Background(), cmd2, 2*time.Second)
	if err != nil {
		t.Fatalf("start bridge2: %v", err)
	}
	g2, _ := grpc.NewClient("passthrough:///w2", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return conn2, nil }))
	client2 := agentv1.NewAgentServiceClient(g2)
	t.Cleanup(func() { _ = g2.Close(); _ = conn2.Close() })
	defer func() { _ = g2.Close(); _ = conn2.Close() }()

	// Turn2 in a goroutine: grpc lazy-connect blocks until bridge2 acquires the lock
	// (i.e. until bridge1 exits), then succeeds.
	turn2Err := make(chan error, 1)
	turn2Start := time.Now()
	go func() {
		stream, err := client2.AddAndGenerateTurnStream(context.Background(), &agentv1.AddAndGenerateTurnStreamRequest{AgentId: "agent-a", UserMessage: "second"})
		if err != nil {
			turn2Err <- err
			return
		}
		for {
			_, rerr := stream.Recv()
			if rerr != nil {
				turn2Err <- nil
				return
			}
		}
	}()

	// Give bridge2 time to reach the lock-wait loop and print the visibility line.
	time.Sleep(300 * time.Millisecond)
	stderrMu.Lock()
	stderrSnapshot := stderr2.String()
	stderrMu.Unlock()
	if !strings.Contains(stderrSnapshot, "waiting for acp-session.lock") {
		t.Errorf("expected lock-wait visibility on stderr, got:\n%s", stderrText())
	}

	// Wait for turn1 to finish, then tear down bridge1 (close conn, stdin EOF, process
	// exit, flock release), which unblocks bridge2.
	select {
	case <-turn1Done:
		_ = g1.Close()
		_ = conn1.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("turn1 never completed")
	}

	// Turn2 must now complete successfully (wait-then-succeed).
	select {
	case err := <-turn2Err:
		if err != nil {
			t.Fatalf("turn2 failed: %v (stderr:\n%s)", err, stderrText())
		}
	case <-time.After(8 * time.Second):
		t.Fatal("turn2 never completed after bridge1 exited")
	}
	if time.Since(turn2Start) < 300*time.Millisecond {
		t.Errorf("turn2 did not appear to wait for the lock (elapsed %v)", time.Since(turn2Start))
	}
}

// stderrMu and stderr2 guard the captured stderr of the second bridge in the concurrent
// dispatch test; the exec goroutine writes while the test reads (race detector enforced).
var stderrMu sync.Mutex
var stderr2 bytes.Buffer

// guardBuffer is a mutex-guarded io.Writer over a bytes.Buffer for capturing
// subprocess stderr without racing the reader.
type guardBuffer struct {
	mu  *sync.Mutex
	buf *bytes.Buffer
}

func stderrText() string {
	stderrMu.Lock()
	defer stderrMu.Unlock()
	return stderr2.String()
}

func (g *guardBuffer) Write(p []byte) (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.buf.Write(p)
}

type processInfo struct {
	PID  int
	Args string
}

func sweepProcesses(marker string) ([]processInfo, error) {
	out, err := exec.Command("ps", "-eo", "pid,args").Output()
	if err != nil {
		return nil, fmt.Errorf("running ps: %w", err)
	}

	var strays []processInfo
	myPID := os.Getpid()

	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || !strings.Contains(line, marker) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil {
			continue
		}
		if pid == myPID {
			continue
		}
		strays = append(strays, processInfo{
			PID:  pid,
			Args: strings.Join(fields[1:], " "),
		})
	}
	return strays, nil
}

func TestE2E_ProcessHygiene_SimulatedFailure(t *testing.T) {
	t.Run("context_cancellation_kills_process_group", func(t *testing.T) {
		subMarker := fmt.Sprintf("cancel-sim-%d-%d", os.Getpid(), time.Now().UnixNano())
		agentFolder := filepath.Join(t.TempDir(), subMarker)
		if err := os.MkdirAll(agentFolder, 0755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())

		wackyacpBin := getWackyacpBin(t)
		shimBin := getShimBin(t)

		args := []string{
			"--agent-folder=" + agentFolder,
			"--harness-cmd=" + shimBin,
			"--harness-args=--script=normal -marker=" + subMarker,
		}

		cmd := exec.CommandContext(ctx, wackyacpBin, args...)
		conn, err := stdio.DialCommand(ctx, cmd, 2*time.Second)
		if err != nil {
			t.Fatalf("DialCommand: %v", err)
		}
		t.Cleanup(func() {
			_ = conn.Close()
		})

		gc, err := grpc.NewClient(
			"passthrough:///wackyacp",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return conn, nil
			}),
		)
		if err != nil {
			t.Fatalf("grpc.NewClient: %v", err)
		}
		defer gc.Close()

		client := agentv1.NewAgentServiceClient(gc)
		_, err = client.GenerateTurn(ctx, &agentv1.GenerateTurnRequest{
			AgentId: "test-agent",
		})
		if err != nil {
			t.Fatalf("GenerateTurn: %v", err)
		}

		// Cancel context while bridge is still running; verify process group dies
		cancel()

		// Wait briefly for cancellation to take effect
		time.Sleep(200 * time.Millisecond)

		strays, err := sweepProcesses(subMarker)
		if err != nil {
			t.Fatalf("sweepProcesses failed: %v", err)
		}
		if len(strays) > 0 {
			t.Fatalf("expected 0 strays after context cancel, got %d: %+v", len(strays), strays)
		}
	})

	t.Run("t_fatal_cleanup_enforcement_kills_process_group", func(t *testing.T) {
		subMarker := fmt.Sprintf("fatal-sim-%d-%d", os.Getpid(), time.Now().UnixNano())
		agentFolder := filepath.Join(t.TempDir(), subMarker)
		if err := os.MkdirAll(agentFolder, 0755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}

		// Run in a subtest where t.Fatal / early exit happens without manual cleanup
		t.Run("subtest_without_cleanup", func(subT *testing.T) {
			ctx := context.Background()
			client, _ := spawnBridge(subT, ctx, agentFolder, "ignore-eof")

			// Execute a turn
			stream, err := client.AddAndGenerateTurnStream(ctx, &agentv1.AddAndGenerateTurnStreamRequest{
				AgentId:     "test-agent",
				UserMessage: "hello fatal sim",
			})
			if err != nil {
				subT.Fatalf("AddAndGenerateTurnStream: %v", err)
			}
			for {
				_, err := stream.Recv()
				if err != nil {
					break
				}
			}

			// Subtest ends WITHOUT calling cleanup(). subT.Cleanup must kill the process group.
		})

		// Give cleanup a moment to reap processes
		time.Sleep(200 * time.Millisecond)

		strays, err := sweepProcesses(subMarker)
		if err != nil {
			t.Fatalf("sweepProcesses failed: %v", err)
		}
		if len(strays) > 0 {
			t.Fatalf("expected 0 strays after subtest cleanup enforcement, got %d: %+v", len(strays), strays)
		}
	})

	t.Run("uncooperative_harness_cleanup_kills_process_group", func(t *testing.T) {
		subMarker := fmt.Sprintf("uncoop-sim-%d-%d", os.Getpid(), time.Now().UnixNano())
		agentFolder := filepath.Join(t.TempDir(), subMarker)
		if err := os.MkdirAll(agentFolder, 0755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}

		ctx := context.Background()
		client, cleanup := spawnBridge(t, ctx, agentFolder, "ignore-eof")

		stream, err := client.AddAndGenerateTurnStream(ctx, &agentv1.AddAndGenerateTurnStreamRequest{
			AgentId:     "test-agent",
			UserMessage: "hello uncoop",
		})
		if err != nil {
			cleanup()
			t.Fatalf("AddAndGenerateTurnStream: %v", err)
		}
		for {
			_, err := stream.Recv()
			if err != nil {
				break
			}
		}

		cleanup()

		strays, err := sweepProcesses(subMarker)
		if err != nil {
			t.Fatalf("sweepProcesses failed: %v", err)
		}
		if len(strays) > 0 {
			t.Fatalf("expected 0 strays after uncooperative cleanup, got %d: %+v", len(strays), strays)
		}
	})
}

func TestWackyacp_E2E_ToolCallStreaming(t *testing.T) {
	agentFolder := newTestAgentFolder(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, cleanup := spawnBridge(t, ctx, agentFolder, "tool-calls")
	defer cleanup()

	stream, err := client.AddAndGenerateTurnStream(ctx, &agentv1.AddAndGenerateTurnStreamRequest{
		AgentId:     "agent-test",
		UserMessage: "run tool",
	})
	if err != nil {
		t.Fatalf("AddAndGenerateTurnStream failed: %v", err)
	}

	var textChunks []string
	var toolCalls []*agentv1.ToolCall
	var toolCallUpdates []*agentv1.ToolCallUpdate

	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("stream.Recv failed: %v", err)
		}
		if resp.Text != "" {
			textChunks = append(textChunks, resp.Text)
		}
		if resp.ToolCall != nil {
			toolCalls = append(toolCalls, resp.ToolCall)
		}
		if resp.ToolCallUpdate != nil {
			toolCallUpdates = append(toolCallUpdates, resp.ToolCallUpdate)
		}
	}

	if len(textChunks) != 2 || textChunks[0] != "calling tool" || textChunks[1] != "tool call finished" {
		t.Errorf("unexpected text chunks: %v", textChunks)
	}
	if len(toolCalls) != 1 {
		t.Fatalf("expected 1 ToolCall, got %d", len(toolCalls))
	}
	if toolCalls[0].CallId != "call-e2e-1" || toolCalls[0].ToolName != "bash" || toolCalls[0].ArgsSummary != "command=echo hello" {
		t.Errorf("unexpected ToolCall: %+v", toolCalls[0])
	}
	if len(toolCallUpdates) != 1 {
		t.Fatalf("expected 1 ToolCallUpdate, got %d", len(toolCallUpdates))
	}
	if toolCallUpdates[0].CallId != "call-e2e-1" || toolCallUpdates[0].Status != "completed" || toolCallUpdates[0].ResultHead != "hello\n" {
		t.Errorf("unexpected ToolCallUpdate: %+v", toolCallUpdates[0])
	}
}

func TestZZZ_Canary_NoOrphanProcesses(t *testing.T) {
	// Assert no stray wackyacp or harness processes tagged with e2eMarker remain after the suite
	strays, err := sweepProcesses(e2eMarker)
	if err != nil {
		t.Fatalf("sweeping processes for marker %q: %v", e2eMarker, err)
	}
	if len(strays) > 0 {
		var details []string
		for _, s := range strays {
			details = append(details, fmt.Sprintf("PID %d: %s", s.PID, s.Args))
			// Kill stray to avoid polluting the host
			_ = syscall.Kill(-s.PID, syscall.SIGKILL)
			_ = syscall.Kill(s.PID, syscall.SIGKILL)
		}
		t.Fatalf("found %d stray orphaned process(es) matching marker %q:\n%s",
			len(strays), e2eMarker, strings.Join(details, "\n"))
	}
}
