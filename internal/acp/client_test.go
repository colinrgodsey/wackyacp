package acp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/colinrgodsey/wackyacp/internal/harness"
	"github.com/colinrgodsey/wackyacp/internal/session"
)

var (
	shimBinOnce sync.Once
	shimBinPath string
)

func getShimBin(t *testing.T) string {
	shimBinOnce.Do(func() {
		tmpDir, err := os.MkdirTemp("", "acpshimbin-test-*")
		if err != nil {
			t.Fatalf("creating temp dir: %v", err)
		}
		outPath := filepath.Join(tmpDir, "acpshimbin")
		cmd := exec.Command("go", "build", "-o", outPath, "../../testdata/acpshimbin")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("building acpshimbin fixture failed: %v\n%s", err, string(out))
		}
		shimBinPath = outPath
	})
	return shimBinPath
}

func startShimClient(t *testing.T, script string) (*Client, *harness.Process) {
	bin := getShimBin(t)
	ctx := context.Background()

	proc, err := harness.Start(ctx, harness.Config{
		Command: bin,
		Args:    []string{"--script=" + script},
	})
	if err != nil {
		t.Fatalf("starting shim failed: %v", err)
	}

	client := NewClient(proc.Stdin, proc.Stdout)
	return client, proc
}

func TestClient_Initialize(t *testing.T) {
	client, proc := startShimClient(t, "normal")
	defer proc.Close()

	ctx := context.Background()
	res, err := client.Initialize(ctx)
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	if res.ProtocolVersion != 1 {
		t.Errorf("expected protocolVersion 1, got %d", res.ProtocolVersion)
	}
	if !client.Capabilities.SessionCapabilities.Resume {
		t.Errorf("expected resume capability true")
	}
}

func TestClient_EstablishSession_ResumeOk(t *testing.T) {
	client, proc := startShimClient(t, "resume-ok")
	defer proc.Close()

	ctx := context.Background()
	if _, err := client.Initialize(ctx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	agentDir := t.TempDir()
	saved := &session.SessionData{
		SessionID:   "existing-session-123",
		AgentFolder: agentDir,
	}

	sessionID, err := client.EstablishSession(ctx, agentDir, saved)
	if err != nil {
		t.Fatalf("EstablishSession failed: %v", err)
	}
	if sessionID != "existing-session-123" {
		t.Errorf("expected resumed sessionID existing-session-123, got: %s", sessionID)
	}
}

func TestClient_EstablishSession_FailResume_FallbackLoad(t *testing.T) {
	client, proc := startShimClient(t, "fail-resume")
	defer proc.Close()

	ctx := context.Background()
	if _, err := client.Initialize(ctx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	agentDir := t.TempDir()
	saved := &session.SessionData{
		SessionID:   "existing-session-456",
		AgentFolder: agentDir,
	}

	// Should attempt resume, fail, then attempt load and succeed
	sessionID, err := client.EstablishSession(ctx, agentDir, saved)
	if err != nil {
		t.Fatalf("EstablishSession failed: %v", err)
	}
	if sessionID != "existing-session-456" {
		t.Errorf("expected loaded sessionID existing-session-456, got: %s", sessionID)
	}
}

func TestClient_EstablishSession_FailLoad_FallbackNew(t *testing.T) {
	client, proc := startShimClient(t, "fail-load")
	defer proc.Close()

	ctx := context.Background()
	if _, err := client.Initialize(ctx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	agentDir := t.TempDir()
	saved := &session.SessionData{
		SessionID:   "stale-session-789",
		AgentFolder: agentDir,
	}

	// Should attempt resume (fails), load (fails), then new (succeeds)
	sessionID, err := client.EstablishSession(ctx, agentDir, saved)
	if err != nil {
		t.Fatalf("EstablishSession failed: %v", err)
	}
	if sessionID != "shim-session-fail-load" {
		t.Errorf("expected new sessionID shim-session-fail-load, got: %s", sessionID)
	}

	// Verify session file was persisted
	persisted, err := session.ReadSession(agentDir)
	if err != nil {
		t.Fatalf("reading persisted session failed: %v", err)
	}
	if persisted.SessionID != "shim-session-fail-load" {
		t.Errorf("persisted sessionID mismatch: %s", persisted.SessionID)
	}
}

func TestClient_EstablishSession_HarnessMismatchFallsBackToNew(t *testing.T) {
	inReader, inWriter := io.Pipe()
	outReader, outWriter := io.Pipe()

	client := NewClient(outWriter, inReader)
	client.Capabilities.SessionCapabilities.Resume = true

	go func() {
		scanner := bufio.NewScanner(outReader)
		for scanner.Scan() {
			line := scanner.Bytes()
			var req map[string]any
			if err := json.Unmarshal(line, &req); err != nil {
				continue
			}
			method, _ := req["method"].(string)
			id := req["id"]
			switch method {
			case "session/resume":
				// Return a mismatched agent_folder
				resp := map[string]any{
					"jsonrpc": "2.0",
					"id":      id,
					"result": map[string]any{
						"_meta": map[string]any{
							"agent_folder": "/different/agent/folder",
						},
					},
				}
				data, _ := json.Marshal(resp)
				_, _ = fmt.Fprintf(inWriter, "%s\n", data)
			case "session/new":
				resp := map[string]any{
					"jsonrpc": "2.0",
					"id":      id,
					"result": map[string]any{
						"sessionId": "fresh-session-after-mismatch",
					},
				}
				data, _ := json.Marshal(resp)
				_, _ = fmt.Fprintf(inWriter, "%s\n", data)
			}
		}
	}()

	agentDir := t.TempDir()
	saved := &session.SessionData{
		SessionID:   "old-session-123",
		AgentFolder: agentDir,
	}

	sessionID, err := client.EstablishSession(context.Background(), agentDir, saved)
	if err != nil {
		t.Fatalf("EstablishSession failed: %v", err)
	}
	if sessionID != "fresh-session-after-mismatch" {
		t.Errorf("expected fresh-session-after-mismatch, got: %s", sessionID)
	}

	_ = inWriter.Close()
	_ = outWriter.Close()
}

func TestClient_Prompt_EmitUsage(t *testing.T) {
	client, proc := startShimClient(t, "emit-usage")
	defer proc.Close()

	ctx := context.Background()
	if _, err := client.Initialize(ctx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	var chunks []string
	var warnings []string
	callbacks := TurnCallbacks{
		OnChunk: func(text string) error {
			chunks = append(chunks, text)
			return nil
		},
		OnWarning: func(w string) error {
			warnings = append(warnings, w)
			return nil
		},
	}

	res, err := client.Prompt(ctx, "session-usage", "test prompt", callbacks)
	if err != nil {
		t.Fatalf("Prompt failed: %v", err)
	}

	if len(chunks) == 0 {
		t.Fatalf("expected chunks, got none")
	}
	if res.Usage.TotalTokens != 150 {
		t.Errorf("expected TotalTokens 150, got %d", res.Usage.TotalTokens)
	}
	if res.Usage.PromptTokens != 100 {
		t.Errorf("expected PromptTokens 100, got %d", res.Usage.PromptTokens)
	}
	if res.Usage.CompletionTokens != 50 {
		t.Errorf("expected CompletionTokens 50, got %d", res.Usage.CompletionTokens)
	}
}

func TestClient_Prompt_AutoDenyPermission(t *testing.T) {
	client, proc := startShimClient(t, "hang-on-permission")
	defer proc.Close()

	ctx := context.Background()
	if _, err := client.Initialize(ctx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	var chunks []string
	var warnings []string
	callbacks := TurnCallbacks{
		OnChunk: func(text string) error {
			chunks = append(chunks, text)
			return nil
		},
		OnWarning: func(w string) error {
			warnings = append(warnings, w)
			return nil
		},
	}

	res, err := client.Prompt(ctx, "session-perm", "test prompt", callbacks)
	if err != nil {
		t.Fatalf("Prompt failed: %v", err)
	}

	if res.StopReason != "end_turn" {
		t.Errorf("expected stopReason end_turn, got: %s", res.StopReason)
	}

	// Verify that permission request auto-denial surfaced a warning
	if len(warnings) == 0 {
		t.Fatalf("expected auto-deny warning, got none")
	}
	foundDenyWarning := false
	for _, w := range warnings {
		if strings.Contains(w, "auto-denied") {
			foundDenyWarning = true
			break
		}
	}
	if !foundDenyWarning {
		t.Errorf("expected auto-deny warning, got: %v", warnings)
	}

	// Verify that the shim got the deny response and reported the outcome
	foundOutcomeChunk := false
	for _, c := range chunks {
		if strings.Contains(c, "permission outcome:") {
			foundOutcomeChunk = true
			break
		}
	}
	if !foundOutcomeChunk {
		t.Errorf("expected outcome chunk from shim, got: %v", chunks)
	}
}
