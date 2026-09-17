package main

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/colinrgodsey/wackyacp/internal/session"
	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var (
	wackyacpBinOnce sync.Once
	wackyacpBinPath string

	shimBinOnce sync.Once
	shimBinPath string
)

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

type stdioConn struct {
	stdin  io.WriteCloser
	stdout io.ReadCloser
	cmd    *exec.Cmd
}

func (c *stdioConn) Read(b []byte) (int, error)  { return c.stdout.Read(b) }
func (c *stdioConn) Write(b []byte) (int, error) { return c.stdin.Write(b) }
func (c *stdioConn) Close() error {
	inErr := c.stdin.Close()
	outErr := c.stdout.Close()
	waitErr := c.cmd.Wait()
	return errors.Join(inErr, outErr, waitErr)
}
func (c *stdioConn) LocalAddr() net.Addr                { return stdioAddr{} }
func (c *stdioConn) RemoteAddr() net.Addr               { return stdioAddr{} }
func (c *stdioConn) SetDeadline(t time.Time) error      { return nil }
func (c *stdioConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *stdioConn) SetWriteDeadline(t time.Time) error { return nil }

type stdioAddr struct{}

func (stdioAddr) Network() string { return "stdio" }
func (stdioAddr) String() string  { return "stdio" }

func spawnBridge(t *testing.T, ctx context.Context, agentFolder, script string) (agentv1.AgentServiceClient, func()) {
	wackyacpBin := getWackyacpBin(t)
	shimBin := getShimBin(t)

	args := []string{
		"--agent-folder=" + agentFolder,
		"--harness-cmd=" + shimBin,
		"--harness-args=--script=" + script,
	}

	cmd := exec.CommandContext(ctx, wackyacpBin, args...)
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
		t.Fatalf("starting wackyacp failed: %v", err)
	}

	conn := &stdioConn{stdin: stdin, stdout: stdout, cmd: cmd}

	gc, err := grpc.NewClient(
		"passthrough:///wackyacp",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return conn, nil
		}),
	)
	if err != nil {
		conn.Close()
		t.Fatalf("grpc.NewClient failed: %v", err)
	}

	client := agentv1.NewAgentServiceClient(gc)
	cleanup := func() {
		_ = gc.Close()
		_ = conn.Close()
	}

	return client, cleanup
}

func TestE2E_AddAndGenerateTurnStream_UsageAndText(t *testing.T) {
	agentFolder := t.TempDir()
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
	agentFolder := t.TempDir()
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
	agentFolder := t.TempDir()
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
	agentFolder := t.TempDir()
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
	agentFolder := t.TempDir()
	ctx := context.Background()

	// 1. Pre-take lock manually
	lock, err := session.AcquireLock(ctx, agentFolder)
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}

	wackyacpBin := getWackyacpBin(t)
	shimBin := getShimBin(t)

	// 2. Start wackyacp in background; it should block on lock
	start := time.Now()
	doneCh := make(chan error, 1)

	cmd := exec.Command(wackyacpBin,
		"--agent-folder="+agentFolder,
		"--harness-cmd="+shimBin,
	)

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

	go func() {
		// Close stdin so wackyacp exits once it acquires lock and starts D112
		_ = stdin.Close()
		_, _ = io.ReadAll(stdout)
		doneCh <- cmd.Wait()
	}()

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
	agentFolder := t.TempDir()
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
