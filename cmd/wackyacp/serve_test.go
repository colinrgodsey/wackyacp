package main

import (
	"bufio"
	"bytes"
	"encoding/json"
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
)

var (
	stdioChildOnce sync.Once
	stdioChildPath string
)

func getStdioChildBin(t *testing.T) string {
	t.Helper()
	stdioChildOnce.Do(func() {
		tmpDir, err := os.MkdirTemp("", "stdiochild-test-*")
		if err != nil {
			t.Fatalf("creating temp dir: %v", err)
		}
		outPath := filepath.Join(tmpDir, "stdiochild")
		cmd := exec.Command("go", "build", "-o", outPath, "../../testdata/stdiochild")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("building stdiochild failed: %v\n%s", err, string(out))
		}
		stdioChildPath = outPath
	})
	return stdioChildPath
}

// pidsWithArgv0 returns the pids of running processes whose argv[0] is
// exactly path - the orphan probe (a killed parent's child is re-parented to
// init but keeps its argv, so the scan finds it wherever it lives).
func pidsWithArgv0(t *testing.T, path string) []int {
	t.Helper()
	var out []int
	entries, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatalf("reading /proc: %v", err)
	}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if err != nil {
			continue
		}
		if strings.SplitN(string(b), "\x00", 2)[0] == path {
			out = append(out, pid)
		}
	}
	return out
}

func dialUntil(t *testing.T, host string, port int, timeout time.Duration) (net.Conn, error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		var c net.Conn
		c, lastErr = net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(port)), time.Second)
		if lastErr == nil {
			return c, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil, lastErr
}

// acpRequest sends one ACP request over a live connection and blocks until
// the response carrying the same id arrives, skipping notifications.
func acpRequest(t *testing.T, br *bufio.Reader, w *bufio.Writer, id int, method string, params map[string]any) map[string]any {
	t.Helper()
	line, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	if err != nil {
		t.Fatalf("marshal %s: %v", method, err)
	}
	if _, err := w.Write(append(line, '\n')); err != nil {
		t.Fatalf("write %s: %v", method, err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush %s: %v", method, err)
	}
	for {
		raw, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("reading response to %s: %v", method, err)
		}
		var frame map[string]json.RawMessage
		if err := json.Unmarshal([]byte(raw), &frame); err != nil {
			t.Fatalf("malformed frame for %s: %v (%s)", method, err, raw)
		}
		idRaw, ok := frame["id"]
		if !ok || len(idRaw) == 0 {
			continue
		}
		var got int
		if err := json.Unmarshal(idRaw, &got); err != nil || got != id {
			t.Fatalf("response id mismatch for %s: got %s want %d (%s)", method, idRaw, id, raw)
		}
		if errRaw, ok := frame["error"]; ok {
			t.Fatalf("error frame for %s: %s", method, errRaw)
		}
		var res map[string]any
		if err := json.Unmarshal(frame["result"], &res); err != nil {
			t.Fatalf("malformed result for %s: %v", method, err)
		}
		return res
	}
}

// TestServeTCPShutdownSIGTERM is the live measurement of the shutdown
// invariant, run against the real wackyacp binary: with a turn held in
// flight, SIGTERM must stop serve cleanly (exit 0, so the defers that reap
// the child ran) well inside the reap grace, and no child may outlive it.
// Before the listener-close fix the accept loop never observed the context,
// so SIGTERM was a no-op until the next connect and only a SIGKILL
// escalation ended the process - skipping the child reap.
func TestServeTCPShutdownSIGTERM(t *testing.T) {
	bin := getWackyacpBin(t)
	child := getStdioChildBin(t)
	agentFolder := newTestAgentFolder(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("finding a free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatalf("releasing port: %v", err)
	}

	cmd := exec.Command(bin, "serve", "--agent-folder", agentFolder, "--wackypub-bin", child, "--port", strconv.Itoa(port))
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting serve: %v (stderr: %s)", err, stderr.String())
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()
	terminated := false
	reaped := false
	t.Cleanup(func() {
		if !terminated {
			_ = cmd.Process.Kill()
		}
		if !reaped {
			<-waitDone
		}
		if len(pidsWithArgv0(t, child)) > 0 {
			t.Logf("note: orphaned child found at cleanup; killing")
			for _, pid := range pidsWithArgv0(t, child) {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	})

	conn, err := dialUntil(t, "127.0.0.1", port, 10*time.Second)
	if err != nil {
		t.Fatalf("dialing serve: %v (stderr: %s)", err, stderr.String())
	}
	t.Cleanup(func() { _ = conn.Close() })
	br := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	initRes := acpRequest(t, br, w, 1, "initialize", map[string]any{
		"protocolVersion":    1,
		"clientCapabilities": map[string]any{},
	})
	if _, ok := initRes["agentCapabilities"]; !ok {
		t.Fatalf("initialize result missing capabilities: %v", initRes)
	}
	newRes := acpRequest(t, br, w, 2, "session/new", map[string]any{"cwd": agentFolder})
	sessionID, _ := newRes["sessionId"].(string)
	if sessionID == "" {
		t.Fatalf("session/new result missing sessionId: %v", newRes)
	}

	// Prompt the scripted child with "slow": it spawns on first gRPC use
	// and holds the turn until cancelled - the live scenario.
	promptLine, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 3, "method": "session/prompt",
		"params": map[string]any{"sessionId": sessionID, "prompt": []any{map[string]any{"type": "text", "text": "slow"}}},
	})
	if err != nil {
		t.Fatalf("marshal prompt: %v", err)
	}
	if _, err := w.Write(append(promptLine, '\n')); err != nil {
		t.Fatalf("write prompt: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush prompt: %v", err)
	}

	// The child must actually be alive before the kill, or the no-orphan
	// assertion below is vacuous.
	deadline := time.Now().Add(15 * time.Second)
	for len(pidsWithArgv0(t, child)) == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("child never spawned; stderr: %s", stderr.String())
		}
		time.Sleep(50 * time.Millisecond)
	}

	start := time.Now()
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}
	select {
	case err := <-waitDone:
		terminated = true
		reaped = true
		if err != nil {
			t.Fatalf("serve exited uncleanly after SIGTERM: %v (stderr: %s)", err, stderr.String())
		}
		if elapsed := time.Since(start); elapsed > 10*time.Second {
			t.Fatalf("serve took %v to exit after SIGTERM", elapsed)
		}
		t.Logf("serve exited cleanly %v after SIGTERM", time.Since(start).Round(time.Millisecond))
	case <-time.After(15 * time.Second):
		t.Fatalf("serve still running 15s after SIGTERM (accept loop ignored ctx); stderr: %s", stderr.String())
	}

	// A tiny grace lets a would-be orphan be observed in /proc before the
	// scan; with the fix the child was reaped before serve exited, so the
	// scan must come up empty.
	time.Sleep(100 * time.Millisecond)
	if orphans := pidsWithArgv0(t, child); len(orphans) > 0 {
		t.Fatalf("orphaned child still running after serve exit: pids %v; stderr: %s", orphans, stderr.String())
	}
	t.Logf("no orphaned child after shutdown; stderr: %s", strings.TrimSpace(stderr.String()))
}
