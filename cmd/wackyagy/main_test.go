package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// defaultOptions mirrors what parseFlags yields with no arguments, so buildConfig
// tests exercise the real validation instead of placeholder zero values.
func defaultOptions() options {
	return options{pollInterval: 100 * time.Millisecond, printTimeout: "20m", permissionMode: "deny"}
}

// executableFile writes an executable stand-in for the agent binary.
func executableFile(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestParseFlagsDefaults(t *testing.T) {
	opts, showVersion, err := parseFlags(nil, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if showVersion {
		t.Fatal("-version must not default to true")
	}
	if opts.pollInterval != 100*time.Millisecond {
		t.Fatalf("pollInterval = %s, want 100ms", opts.pollInterval)
	}
	if opts.printTimeout != "20m" {
		t.Fatalf("printTimeout = %q, want 20m", opts.printTimeout)
	}

	if opts.permissionMode != "deny" {
		t.Fatalf("permissionMode = %q, want deny", opts.permissionMode)
	}
	if opts.conversationsDir != "" || opts.stateDir != "" || opts.logDir != "" {
		t.Fatal("directory flags must stay empty so defaults apply")
	}
}

func TestParseFlagsOverrides(t *testing.T) {
	opts, _, err := parseFlags([]string{
		"--agy-bin", "/tmp/agy",
		"--workdir", "/tmp/work",
		"--conversations-dir", "/tmp/conv",
		"--state-dir", "/tmp/state",
		"--log-dir", "/tmp/log",
		"--extra-args", `--model "Two Words" --dry-run`,
		"--print-timeout", "5m",
		"--poll-interval", "25ms",
		"--show-narration",
		"--permission-mode", "approve",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if opts.agyBin != "/tmp/agy" || opts.workDir != "/tmp/work" || opts.conversationsDir != "/tmp/conv" {
		t.Fatalf("opts = %+v", opts)
	}
	if opts.pollInterval != 25*time.Millisecond || opts.printTimeout != "5m" || !opts.showNarration {
		t.Fatalf("opts = %+v", opts)
	}

	opts.agyBin = executableFile(t, t.TempDir(), "agy")
	cfg, err := buildConfig(opts, io.Discard)
	if err != nil {
		t.Fatalf("buildConfig: %v", err)
	}
	want := []string{"--model", "Two Words", "--dry-run"}
	if strings.Join(cfg.ExtraArgs, "|") != strings.Join(want, "|") {
		t.Fatalf("ExtraArgs = %v, want %v", cfg.ExtraArgs, want)
	}
	if cfg.PrintTimeout != "5m" || cfg.PollInterval != 25*time.Millisecond || !cfg.ShowNarration {
		t.Fatalf("cfg = %+v", cfg)
	}

	if cfg.PermissionMode != "approve" {
		t.Fatalf("PermissionMode = %q, want approve", cfg.PermissionMode)
	}
}

func TestEnvironmentSuppliesDefaults(t *testing.T) {
	t.Setenv("AGY_EXTRA_ARGS", "--verbose")
	t.Setenv("AGY_SHOW_NARRATION", "true")

	opts, _, err := parseFlags(nil, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if opts.extraArgs != "--verbose" {
		t.Fatalf("extraArgs = %q, want the environment default", opts.extraArgs)
	}
	if !opts.showNarration {
		t.Fatal("showNarration = false, want the environment default")
	}
}

func TestUnbalancedExtraArgsFails(t *testing.T) {
	cfgOpts, _, err := parseFlags([]string{"--extra-args", `--model "unterminated`}, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if _, err := buildConfig(cfgOpts, io.Discard); err == nil {
		t.Fatal("unbalanced quoting in --extra-args was accepted")
	}
}

func TestBuildConfigRequiresAbsoluteDirectories(t *testing.T) {
	for _, flagName := range []string{"--workdir", "--conversations-dir", "--state-dir", "--log-dir"} {
		opts := defaultOptions()
		switch flagName {
		case "--workdir":
			opts.workDir = "relative/path"
		case "--conversations-dir":
			opts.conversationsDir = "relative/path"
		case "--state-dir":
			opts.stateDir = "relative/path"
		case "--log-dir":
			opts.logDir = "relative/path"
		}
		_, err := buildConfig(opts, io.Discard)
		if err == nil {
			t.Fatalf("%s accepted a relative path", flagName)
		}
		if !strings.Contains(err.Error(), flagName) {
			t.Fatalf("error %q does not name %s", err, flagName)
		}
	}
}

func TestBuildConfigRejectsFileInPlaceOfDirectory(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := buildConfig(options{stateDir: file, pollInterval: 100 * time.Millisecond, printTimeout: "20m"}, io.Discard)
	if err == nil {
		t.Fatal("a regular file was accepted as --state-dir")
	}
	if !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("error = %v", err)
	}
}

func TestBuildConfigAgyBinMustBeExecutable(t *testing.T) {
	dir := t.TempDir()
	regular := filepath.Join(dir, "agy")
	if err := os.WriteFile(regular, []byte("#!/bin/sh\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := buildConfig(options{agyBin: regular}, io.Discard); err == nil {
		t.Fatal("a non-executable file was accepted as --agy-bin")
	}

	executable := filepath.Join(dir, "agy-exec")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg, err := buildConfig(options{agyBin: executable, pollInterval: 100 * time.Millisecond, printTimeout: "20m", permissionMode: "deny"}, io.Discard)
	if err != nil {
		t.Fatalf("buildConfig: %v", err)
	}
	if cfg.AgyBin != executable {
		t.Fatalf("AgyBin = %q, want %q", cfg.AgyBin, executable)
	}

	if _, err := buildConfig(options{agyBin: filepath.Join(dir, "absent"), pollInterval: 100 * time.Millisecond, printTimeout: "20m"}, io.Discard); err == nil {
		t.Fatal("a missing --agy-bin was accepted")
	}
}

func TestBuildConfigFallsBackToPathLookup(t *testing.T) {
	if _, err := os.Stat(defaultAgyBin); err == nil {
		t.Skipf("%s exists on this host, so the PATH fallback cannot be exercised", defaultAgyBin)
	}
	dir := t.TempDir()
	fake := filepath.Join(dir, "agy")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)

	cfg, err := buildConfig(defaultOptions(), io.Discard)
	if err != nil {
		t.Fatalf("buildConfig: %v", err)
	}
	if cfg.AgyBin != fake {
		t.Fatalf("AgyBin = %q, want the PATH candidate %q", cfg.AgyBin, fake)
	}
}

func TestBuildConfigRejectsUnusableAgent(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PATH", dir)
	if _, err := os.Stat(defaultAgyBin); err == nil {
		t.Skipf("%s exists on this host", defaultAgyBin)
	}
	if _, err := buildConfig(defaultOptions(), io.Discard); err == nil {
		t.Fatal("no agent binary was reported")
	}
}

func TestBuildConfigRejectsBadIntervals(t *testing.T) {
	if _, err := buildConfig(options{pollInterval: 0, printTimeout: "20m"}, io.Discard); err == nil {
		t.Fatal("a zero poll interval was accepted")
	}
	if _, err := buildConfig(options{pollInterval: -1, printTimeout: "20m"}, io.Discard); err == nil {
		t.Fatal("a negative poll interval was accepted")
	}
	if _, err := buildConfig(options{pollInterval: 100 * time.Millisecond, printTimeout: ""}, io.Discard); err == nil {
		t.Fatal("an empty print timeout was accepted")
	}
}

func TestBuildConfigDefaultsAndWorkingDirectory(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	agy := filepath.Join(home, "agy")
	if err := os.WriteFile(agy, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	workDir := filepath.Join(home, "work")
	if err := os.MkdirAll(workDir, 0o700); err != nil {
		t.Fatal(err)
	}

	cfg, err := buildConfig(options{agyBin: agy, workDir: workDir, pollInterval: 100 * time.Millisecond, printTimeout: "20m", permissionMode: "deny"}, io.Discard)
	if err != nil {
		t.Fatalf("buildConfig: %v", err)
	}
	if cfg.ConversationsDir != filepath.Join(home, defaultConversationsDir) {
		t.Fatalf("ConversationsDir = %q", cfg.ConversationsDir)
	}
	if cfg.LogDir != filepath.Join(home, defaultLogDir) {
		t.Fatalf("LogDir = %q", cfg.LogDir)
	}
	if cfg.StateDir != filepath.Join(home, defaultStateDir) {
		t.Fatalf("StateDir = %q", cfg.StateDir)
	}
	if cfg.PrintTimeout != "20m" || cfg.PollInterval != 100*time.Millisecond {
		t.Fatalf("cfg = %+v", cfg)
	}
	if cfg.Version != version {
		t.Fatalf("Version = %q, want %q", cfg.Version, version)
	}

	// With no --workdir the process cwd is used, which must still be absolute.
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	fromCwd, err := buildConfig(options{agyBin: agy, pollInterval: 100 * time.Millisecond, printTimeout: "20m", permissionMode: "deny"}, io.Discard)
	if err != nil {
		t.Fatalf("buildConfig with cwd: %v", err)
	}
	if fromCwd.WorkingDir != cwd {
		t.Fatalf("WorkingDir = %q, want %q", fromCwd.WorkingDir, cwd)
	}
}

func TestVersionFlag(t *testing.T) {
	stdout := &bytes.Buffer{}
	if err := run([]string{"-version"}, strings.NewReader(""), stdout, io.Discard); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := strings.TrimSpace(stdout.String()); got != "wackyagy "+version {
		t.Fatalf("version output = %q", got)
	}
}

func TestRunReportsFlagErrors(t *testing.T) {
	stderr := &bytes.Buffer{}
	if err := run([]string{"--agy-bin", "/definitely/not/here"}, strings.NewReader(""), io.Discard, stderr); err == nil {
		t.Fatal("a missing agent binary was accepted")
	}
}

// TestRunAgainstStubBinary drives the whole command line: real flags, real child
// process, real protocol on stdin/stdout.
func TestRunAgainstStubBinary(t *testing.T) {
	stub := buildAgyStub(t)
	envRoot := t.TempDir()
	conversations := filepath.Join(envRoot, "conversations")
	state := filepath.Join(envRoot, "state")
	logs := filepath.Join(envRoot, "log")
	for _, dir := range []string{conversations, state, logs} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}

	t.Setenv("STUB_SCENARIO", "stream")
	t.Setenv("STUB_CONVERSATIONS_DIR", conversations)
	t.Setenv("STUB_LOG_DIR", logs)
	t.Setenv("STUB_MODELS", "Stub Model A")

	// The session id from session/new is needed by the prompt, so the exchange is
	// driven over pipes rather than a fixed script.
	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()

	errs := make(chan error, 1)
	go func() {
		errs <- run([]string{
			"--agy-bin", stub,
			"--workdir", envRoot,
			"--conversations-dir", conversations,
			"--state-dir", state,
			"--log-dir", logs,
			"--poll-interval", "5ms",
			"--extra-args", "--experimental",
		}, stdinReader, stdoutWriter, io.Discard)
	}()

	lines := make(chan string, 32)
	go func() {
		scanner := bufio.NewScanner(stdoutReader)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()

	writeClient := func(line string) {
		if _, err := io.WriteString(stdinWriter, line+"\n"); err != nil {
			t.Errorf("writing to the bridge: %v", err)
		}
	}

	writeClient(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`)
	initMsg := mustMessage(t, lines)
	if initMsg.Error != nil {
		t.Fatalf("initialize = %+v", initMsg.Error)
	}

	writeClient(`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{}}`)
	newMsg := mustMessage(t, lines)
	var newResult struct {
		SessionID     string          `json:"sessionId"`
		ConfigOptions json.RawMessage `json:"configOptions"`
	}
	if err := json.Unmarshal(newMsg.Result, &newResult); err != nil {
		t.Fatalf("session/new result %s: %v", newMsg.Result, err)
	}
	if newResult.SessionID == "" {
		t.Fatal("no sessionId")
	}
	if !strings.Contains(string(newResult.ConfigOptions), "Stub Model A") {
		t.Fatalf("configOptions = %s, want the stub model", newResult.ConfigOptions)
	}

	writeClient(fmt.Sprintf(`{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":%q,"prompt":[{"type":"text","text":"hello"}]}}`, newResult.SessionID))

	var streamed strings.Builder
	var stopReason string
	deadline := time.After(20 * time.Second)
	for stopReason == "" {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatal("the bridge closed the stream mid-turn")
			}
			var msg clientMessage
			if err := json.Unmarshal([]byte(line), &msg); err != nil {
				t.Fatalf("malformed protocol line %q: %v", line, err)
			}
			if msg.Method == "session/update" {
				var params struct {
					Update struct {
						Content struct {
							Text string `json:"text"`
						} `json:"content"`
					} `json:"update"`
				}
				if err := json.Unmarshal(msg.Params, &params); err != nil {
					t.Fatalf("malformed update %s: %v", msg.Params, err)
				}
				streamed.WriteString(params.Update.Content.Text)
				continue
			}
			if msg.Error != nil {
				t.Fatalf("prompt failed: %d %s", msg.Error.Code, msg.Error.Message)
			}
			var result struct {
				StopReason string `json:"stopReason"`
			}
			if err := json.Unmarshal(msg.Result, &result); err != nil {
				t.Fatalf("malformed prompt result %s: %v", msg.Result, err)
			}
			stopReason = result.StopReason
		case <-deadline:
			t.Fatal("the turn never completed")
		}
	}
	if streamed.String() != "Hello world done" {
		t.Fatalf("streamed %q, want %q", streamed.String(), "Hello world done")
	}
	if stopReason != "end_turn" {
		t.Fatalf("stopReason = %q", stopReason)
	}

	if err := stdinWriter.Close(); err != nil {
		t.Errorf("closing client stdin: %v", err)
	}
	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("run returned %v, want a clean stop at EOF", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after stdin closed")
	}
}

type clientMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func mustMessage(t *testing.T, lines chan string) clientMessage {
	t.Helper()
	select {
	case line, ok := <-lines:
		if !ok {
			t.Fatal("the bridge closed the stream")
		}
		var msg clientMessage
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			t.Fatalf("malformed protocol line %q: %v", line, err)
		}
		return msg
	case <-time.After(10 * time.Second):
		t.Fatal("no response within 10s")
		return clientMessage{}
	}
}

func buildAgyStub(t *testing.T) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "agystubbin")
	cmd := exec.Command("go", "build", "-o", out, "../../testdata/agystubbin")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("building the agy stub: %v: %s", err, output)
	}
	return out
}

// TestBuildConfigRejectsUnknownPermissionMode keeps the posture closed: only deny and
// approve are understood, so a typo cannot silently mean something other than what the
// operator wrote.
func TestBuildConfigRejectsUnknownPermissionMode(t *testing.T) {
	dir := t.TempDir()
	agyBin := filepath.Join(dir, "agy")
	if err := os.WriteFile(agyBin, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	for _, mode := range []string{"yolo", "Approve", ""} {
		opts := defaultOptions()
		opts.agyBin = agyBin
		opts.permissionMode = mode
		_, err := buildConfig(opts, io.Discard)
		if err == nil {
			t.Fatalf("--permission-mode %q was accepted", mode)
		}
		if !strings.Contains(err.Error(), "--permission-mode") {
			t.Fatalf("error = %v, want it to name --permission-mode", err)
		}
	}

	for _, mode := range []string{"deny", "approve"} {
		opts := defaultOptions()
		opts.agyBin = agyBin
		opts.permissionMode = mode
		cfg, err := buildConfig(opts, io.Discard)
		if err != nil {
			t.Fatalf("--permission-mode %s: %v", mode, err)
		}
		if cfg.PermissionMode != mode {
			t.Fatalf("PermissionMode = %q, want %q", cfg.PermissionMode, mode)
		}
	}
}
