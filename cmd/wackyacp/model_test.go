package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLI_ModelSet_ForwardAndConfirm(t *testing.T) {
	wackyacpBin := getWackyacpBin(t)
	shimBin := getShimBin(t)
	agentFolder := newTestAgentFolder(t)

	cmd := exec.Command(wackyacpBin,
		"model", "set", "sonnet",
		"--workspace="+agentFolder,
		"--harness-cmd="+shimBin,
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("model set failed: %v\nOutput: %s", err, string(out))
	}

	var resp struct {
		Model         string          `json:"model"`
		ConfigOptions json.RawMessage `json:"configOptions"`
	}
	if err := json.Unmarshal(out, &resp); err != nil {
		t.Fatalf("expected valid JSON output, got: %s (err: %v)", string(out), err)
	}

	if resp.Model != "sonnet" {
		t.Errorf("expected confirmed model sonnet, got %q", resp.Model)
	}
	if !strings.Contains(string(resp.ConfigOptions), "sonnet") {
		t.Errorf("expected configOptions to contain sonnet, got: %s", string(resp.ConfigOptions))
	}
}

func TestCLI_ModelSet_UnknownModelSurfacesHarnessError(t *testing.T) {
	wackyacpBin := getWackyacpBin(t)
	shimBin := getShimBin(t)
	agentFolder := newTestAgentFolder(t)

	cmd := exec.Command(wackyacpBin,
		"model", "set", "unknown-model",
		"--workspace="+agentFolder,
		"--harness-cmd="+shimBin,
	)

	out, err := cmd.Output()
	if err == nil {
		t.Fatalf("expected non-zero exit for unknown model, got exit 0")
	}

	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("expected ExitError, got %T: %v", err, err)
	}

	stderr := string(exitErr.Stderr)
	if !strings.Contains(stderr, "unknown model: unknown-model") {
		t.Errorf("expected stderr to contain error message, got: %q", stderr)
	}

	var errResp struct {
		Error string `json:"error"`
		Code  int    `json:"code"`
	}
	if err := json.Unmarshal(out, &errResp); err != nil {
		t.Fatalf("expected valid JSON error body on stdout, got: %s (err: %v)", string(out), err)
	}

	if errResp.Code != -32602 {
		t.Errorf("expected JSON-RPC code -32602, got %d", errResp.Code)
	}
	if !strings.Contains(errResp.Error, "unknown model") {
		t.Errorf("expected error field to mention unknown model, got: %q", errResp.Error)
	}
}

func TestCLI_ModelSet_EmptyModelRejected(t *testing.T) {
	wackyacpBin := getWackyacpBin(t)
	agentFolder := newTestAgentFolder(t)

	// Case 1: model set with empty string argument
	cmd1 := exec.Command(wackyacpBin,
		"model", "set", "",
		"--workspace="+agentFolder,
	)

	out1, err1 := cmd1.Output()
	if err1 == nil {
		t.Fatalf("expected non-zero exit for empty model")
	}

	exitErr1, ok := err1.(*exec.ExitError)
	if !ok {
		t.Fatalf("expected ExitError, got: %v", err1)
	}
	if !strings.Contains(string(exitErr1.Stderr), "model cannot be empty") {
		t.Errorf("expected stderr to contain 'model cannot be empty', got: %q", string(exitErr1.Stderr))
	}

	var errResp1 struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(out1, &errResp1); err != nil {
		t.Fatalf("expected valid JSON error body, got: %s", string(out1))
	}
	if errResp1.Error != "model cannot be empty" {
		t.Errorf("expected error 'model cannot be empty', got %q", errResp1.Error)
	}

	// Case 2: model set with no positional model argument at all
	cmd2 := exec.Command(wackyacpBin,
		"model", "set",
		"--workspace="+agentFolder,
	)

	out2, err2 := cmd2.Output()
	if err2 == nil {
		t.Fatalf("expected non-zero exit for missing model argument")
	}

	exitErr2, ok := err2.(*exec.ExitError)
	if !ok {
		t.Fatalf("expected ExitError, got: %v", err2)
	}
	if !strings.Contains(string(exitErr2.Stderr), "model cannot be empty") {
		t.Errorf("expected stderr to contain 'model cannot be empty', got: %q", string(exitErr2.Stderr))
	}

	var errResp2 struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(out2, &errResp2); err != nil {
		t.Fatalf("expected valid JSON error body, got: %s", string(out2))
	}
	if errResp2.Error != "model cannot be empty" {
		t.Errorf("expected error 'model cannot be empty', got %q", errResp2.Error)
	}
}

func TestCLI_ModelGet_Roundtrip(t *testing.T) {
	wackyacpBin := getWackyacpBin(t)
	shimBin := getShimBin(t)
	agentFolder := newTestAgentFolder(t)

	// Set model to "opus"
	cmdSet := exec.Command(wackyacpBin,
		"model", "set", "opus",
		"--workspace="+agentFolder,
		"--harness-cmd="+shimBin,
	)
	outSet, err := cmdSet.CombinedOutput()
	if err != nil {
		t.Fatalf("model set failed: %v\nOutput: %s", err, string(outSet))
	}

	// Get model and verify it returns "opus"
	cmdGet := exec.Command(wackyacpBin,
		"model", "get",
		"--workspace="+agentFolder,
		"--harness-cmd="+shimBin,
	)
	outGet, err := cmdGet.CombinedOutput()
	if err != nil {
		t.Fatalf("model get failed: %v\nOutput: %s", err, string(outGet))
	}

	var resp struct {
		Model         string          `json:"model"`
		ConfigOptions json.RawMessage `json:"configOptions"`
	}
	if err := json.Unmarshal(outGet, &resp); err != nil {
		t.Fatalf("expected valid JSON from model get, got: %s (err: %v)", string(outGet), err)
	}

	if resp.Model != "opus" {
		t.Errorf("expected model opus, got %q", resp.Model)
	}
	if !strings.Contains(string(resp.ConfigOptions), "opus") {
		t.Errorf("expected configOptions to contain opus, got: %s", string(resp.ConfigOptions))
	}
}

func TestCLI_Model_ManifestResolutionAndCWD(t *testing.T) {
	wackyacpBin := getWackyacpBin(t)
	shimBin := getShimBin(t)

	// Create workspace with REMOTE_MANIFEST and agent folder
	wsDir := t.TempDir()
	agentFolder := filepath.Join(wsDir, "myagent")
	if err := os.MkdirAll(agentFolder, 0755); err != nil {
		t.Fatalf("mkdir agent folder: %v", err)
	}

	manifestContent := "myagent: " + wackyacpBin + " --harness-cmd=" + shimBin + " --permission-mode=approve --agent-folder=" + agentFolder + "\n"
	if err := os.WriteFile(filepath.Join(wsDir, "REMOTE_MANIFEST"), []byte(manifestContent), 0644); err != nil {
		t.Fatalf("write REMOTE_MANIFEST: %v", err)
	}

	// 1. Run model get from workspace flag (without explicit --harness-cmd)
	cmd1 := exec.Command(wackyacpBin, "model", "get", "--workspace="+agentFolder)
	out1, err1 := cmd1.CombinedOutput()
	if err1 != nil {
		t.Fatalf("model get with --workspace failed: %v\nOutput: %s", err1, string(out1))
	}

	var resp1 struct {
		Model         string          `json:"model"`
		ConfigOptions json.RawMessage `json:"configOptions"`
	}
	if err := json.Unmarshal(out1, &resp1); err != nil {
		t.Fatalf("expected valid JSON, got: %s", string(out1))
	}
	if resp1.Model == "" {
		t.Errorf("expected non-empty model")
	}

	// 2. Run model get with CWD = agentFolder (no --workspace flag!)
	cmd2 := exec.Command(wackyacpBin, "model", "get")
	cmd2.Dir = agentFolder
	out2, err2 := cmd2.CombinedOutput()
	if err2 != nil {
		t.Fatalf("model get from CWD failed: %v\nOutput: %s", err2, string(out2))
	}

	var resp2 struct {
		Model         string          `json:"model"`
		ConfigOptions json.RawMessage `json:"configOptions"`
	}
	if err := json.Unmarshal(out2, &resp2); err != nil {
		t.Fatalf("expected valid JSON, got: %s", string(out2))
	}
	if resp2.Model != resp1.Model {
		t.Errorf("expected same model from CWD (%q) as from --workspace (%q)", resp2.Model, resp1.Model)
	}

	// 3. Run model set from CWD
	cmd3 := exec.Command(wackyacpBin, "model", "set", "sonnet")
	cmd3.Dir = agentFolder
	out3, err3 := cmd3.CombinedOutput()
	if err3 != nil {
		t.Fatalf("model set from CWD failed: %v\nOutput: %s", err3, string(out3))
	}

	var resp3 struct {
		Model         string          `json:"model"`
		ConfigOptions json.RawMessage `json:"configOptions"`
	}
	if err := json.Unmarshal(out3, &resp3); err != nil {
		t.Fatalf("expected valid JSON, got: %s", string(out3))
	}
	if resp3.Model != "sonnet" {
		t.Errorf("expected confirmed model sonnet, got %q", resp3.Model)
	}
}
