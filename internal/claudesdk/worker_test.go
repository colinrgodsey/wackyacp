package claudesdk

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// requireNode skips LOUDLY when the runner has no Node. These are the only
// tests that start the real worker, so a quiet skip would drop exactly the
// coverage that caught the materialized file-name bug: the skip message names
// what went untested and how to get it back.
func requireNode(t *testing.T) string {
	t.Helper()
	nodeBin, err := exec.LookPath("node")
	if err != nil {
		t.Skipf("NOT COVERED IN THIS RUN: node is not on PATH, so the real %s never started and the worker protocol went unexercised; install Node (CI uses actions/setup-node) before trusting this suite", workerFileName)
	}
	t.Logf("real-worker test using %s", nodeBin)
	return nodeBin
}

// stubSDK stands in for @anthropic-ai/claude-agent-sdk. Its first query hangs
// until cancelled and its interrupt() rejects, which is a state the real SDK's
// types permit and which the Go-stub worker in driver_test.go cannot express,
// because that stub has no interrupt at all.
const stubSDK = `
let calls = 0;

export function query() {
  calls += 1;
  const mine = calls;
  return {
    async *[Symbol.asyncIterator]() {
      if (mine === 1) {
        await new Promise(function () {});
        return;
      }
      yield {
        type: 'result',
        subtype: 'success',
        session_id: 'stub-session',
        usage: { input_tokens: 1, output_tokens: 2 },
      };
    },
    interrupt() {
      return Promise.reject(new Error('interrupt exploded'));
    },
  };
}

export function createSdkMcpServer() { return { name: 'wackypub' }; }
export function tool() { return {}; }
`

// lockedBuffer lets the test read the worker's stderr while the frames are
// flowing, so the rejection report can be asserted without a race.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestWorkerSurvivesRejectedInterrupt is the Node-side counterpart to
// TestDriverContextCancelReturnsAndDoesNotHang. An unhandled rejection inside the
// cancel handler ends the Node process outright, which would take every other
// in-flight request down with it, so the handler must swallow it and keep
// serving.
func TestWorkerSurvivesRejectedInterrupt(t *testing.T) {
	nodeBin := requireNode(t)

	dir := t.TempDir()
	sdkDir := filepath.Join(dir, "sdk")
	writeFile(t, filepath.Join(sdkDir, "sdk.mjs"), stubSDK)
	writeFile(t, filepath.Join(sdkDir, "package.json"), `{"type":"module"}`)

	src, err := workerScript()
	if err != nil {
		t.Fatalf("workerScript: %v", err)
	}
	worker := filepath.Join(dir, "worker.mjs")
	writeFile(t, worker, src)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, nodeBin, worker)
	cmd.Env = append(os.Environ(), sdkDirEnvVar+"="+sdkDir)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout: %v", err)
	}
	stderr := &lockedBuffer{}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	events := make(chan workerEvent, 128)
	go func() {
		defer close(events)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			var ev workerEvent
			if err := json.Unmarshal(scanner.Bytes(), &ev); err == nil {
				events <- ev
			}
		}
	}()

	waitFor := func(what string, match func(workerEvent) bool) workerEvent {
		t.Helper()
		deadline := time.After(30 * time.Second)
		for {
			select {
			case ev, more := <-events:
				if !more {
					t.Fatalf("%s: worker stdout closed early; stderr=%s", what, stderr.String())
				}
				if match(ev) {
					return ev
				}
			case <-deadline:
				t.Fatalf("%s: timed out; stderr=%s", what, stderr.String())
			}
		}
	}

	send := func(req workerRequest) {
		t.Helper()
		if err := json.NewEncoder(stdin).Encode(req); err != nil {
			t.Fatalf("sending %+v: %v", req, err)
		}
	}

	waitFor("ready frame", func(ev workerEvent) bool { return ev.Type == frameReady })

	send(workerRequest{ID: 1, Op: opPrompt, Prompt: "hangs until cancelled", Cwd: dir})
	time.Sleep(500 * time.Millisecond)
	send(workerRequest{ID: 2, Op: opCancel})
	time.Sleep(500 * time.Millisecond)

	// The worker must still be alive and serving after the rejected interrupt.
	send(workerRequest{ID: 3, Op: opPrompt, Prompt: "second turn", Cwd: dir})
	res := waitFor("result for the second prompt", func(ev workerEvent) bool {
		return ev.Type == frameResult && ev.ID == 3
	})
	if res.StopReason != "end_turn" {
		t.Errorf("second prompt stop reason = %q, want end_turn", res.StopReason)
	}
	if got := stderr.String(); !contains(got, "interrupt failed") {
		t.Errorf("the rejected interrupt was swallowed silently; expected a report on stderr, got %q", got)
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
