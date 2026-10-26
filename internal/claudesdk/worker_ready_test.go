package claudesdk

import (
	"context"
	"github.com/colinrgodsey/wackyacp/internal/acp"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestMaterializedWorkerReachesReady is the smoke test the fake-worker suite
// could never provide: it materializes the embedded worker the way a real bridge
// does and makes real node load it. Node derives the module format from the file
// suffix, so a materialized name with the extension anywhere other than the end
// fails here instead of in production with ERR_UNKNOWN_FILE_EXTENSION.
func TestMaterializedWorkerReachesReady(t *testing.T) {
	nodeBin := requireNode(t)

	agentDir := filepath.Join(t.TempDir(), "claude")
	writeFile(t, filepath.Join(agentDir, "AGENTS.md"), "test agent"+nl)

	// A stub SDK keeps this offline and unbilled: reaching the ready frame only
	// needs the worker to load and build its tool server.
	sdkDir := filepath.Join(t.TempDir(), "sdk")
	writeFile(t, filepath.Join(sdkDir, "sdk.mjs"), stubSDK)
	writeFile(t, filepath.Join(sdkDir, "package.json"), `{"type":"module"}`)

	d, err := New(Config{
		AgentFolder: agentDir,
		NodeBin:     nodeBin,
		SDKDir:      sdkDir,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	path, err := d.workerPath()
	if err != nil {
		t.Fatalf("workerPath: %v", err)
	}
	if !strings.HasSuffix(path, workerFileExt) {
		t.Errorf("materialized worker name %q does not end in %s; node will refuse it", path, workerFileExt)
	}
	if strings.Contains(filepath.Base(path), workerFileName+"-") {
		t.Errorf("materialized worker name %q still carries a suffix after the extension", filepath.Base(path))
	}
	src, err := workerScript()
	if err != nil {
		t.Fatalf("workerScript: %v", err)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != src {
		t.Fatalf("materialized worker does not match the embedded script")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := d.ensureWorker(ctx); err != nil {
		t.Fatalf("real worker never became ready: %v", err)
	}
	if d.workerPID() == 0 {
		t.Errorf("worker reported ready but no process is tracked")
	}
}

// TestWorkerPathIsContentAddressed pins the sharing rule: the same embedded
// script maps to the same file, so bridges do not accumulate one temp file each.
func TestWorkerPathIsContentAddressed(t *testing.T) {
	agentDir := filepath.Join(t.TempDir(), "claude")
	writeFile(t, filepath.Join(agentDir, "AGENTS.md"), "test agent"+nl)

	first, err := New(Config{AgentFolder: agentDir, SDKDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	second, err := New(Config{AgentFolder: agentDir, SDKDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New 2: %v", err)
	}
	a, err := first.workerPath()
	if err != nil {
		t.Fatalf("workerPath 1: %v", err)
	}
	b, err := second.workerPath()
	if err != nil {
		t.Fatalf("workerPath 2: %v", err)
	}
	if a != b {
		t.Errorf("two drivers with the same embedded script chose %q and %q; they should share one file", a, b)
	}
}

// stubSDKModel records the options the worker actually passed to query() and
// smuggles the model back through session_id, so a Go assertion can prove the
// --model flag reaches the SDK rather than stopping in a frame.
const stubSDKModel = `
export function query(req) {
  const options = (req && req.options) || {};
  return {
    async *[Symbol.asyncIterator]() {
      yield {
        type: 'result',
        subtype: 'success',
        session_id: options.model ? String(options.model) : 'claude-default',
        usage: { input_tokens: 1, output_tokens: 1 },
      };
    },
    interrupt() { return Promise.resolve(); },
  };
}

export function createSdkMcpServer() { return { name: 'wackypub' }; }
export function tool() { return {}; }
`

// TestWorkerPassesModelToQueryOptions is the end-to-end leg: driver config over
// the real frame protocol into real node and into query options.
func TestWorkerPassesModelToQueryOptions(t *testing.T) {
	nodeBin := requireNode(t)

	agentDir := filepath.Join(t.TempDir(), "claude")
	writeFile(t, filepath.Join(agentDir, "AGENTS.md"), "test agent"+nl)
	sdkDir := filepath.Join(t.TempDir(), "sdk")
	writeFile(t, filepath.Join(sdkDir, "sdk.mjs"), stubSDKModel)
	writeFile(t, filepath.Join(sdkDir, "package.json"), `{"type":"module"}`)

	cases := []struct {
		name  string
		model string
		want  string
	}{
		{"configured", "claude-opus-4-1", "claude-opus-4-1"},
		{"unset leaves the default", "", "claude-default"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, err := New(Config{
				AgentFolder: agentDir,
				NodeBin:     nodeBin,
				SDKDir:      sdkDir,
				Model:       tc.model,
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer func() { _ = d.Close() }()

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			if _, err := d.Prompt(ctx, "", "which model ran", acp.TurnCallbacks{}); err != nil {
				t.Fatalf("Prompt: %v", err)
			}
			if got := d.SessionID(); got != tc.want {
				t.Errorf("query() received model %q, want %q", got, tc.want)
			}
		})
	}
}
