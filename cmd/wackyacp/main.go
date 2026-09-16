package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/anmitsu/go-shlex"
	"github.com/colinrgodsey/wackyacp/internal/acp"
	"github.com/colinrgodsey/wackyacp/internal/d112"
	"github.com/colinrgodsey/wackyacp/internal/harness"
	"github.com/colinrgodsey/wackyacp/internal/session"
)

func run() error {
	harnessCmd := flag.String("harness-cmd", "", "Harness command to execute (e.g. agy, npx)")
	harnessArgs := flag.String("harness-args", "", "Arguments for the harness command")
	agentFolder := flag.String("agent-folder", "", "Absolute path to the agent folder")
	flag.Parse()

	// 1. Validate agent-folder: must be absolute, NO CWD-based fallback (D117 Decision)
	if *agentFolder == "" {
		return fmt.Errorf("--agent-folder is required")
	}
	if !filepath.IsAbs(*agentFolder) {
		return fmt.Errorf("--agent-folder must be an absolute path: %q (no CWD fallback allowed)", *agentFolder)
	}

	// 2. Validate harness-cmd: LookPath at startup, fail fast (D117 Decision)
	if *harnessCmd == "" {
		return fmt.Errorf("--harness-cmd is required")
	}
	if _, err := harness.Resolve(*harnessCmd); err != nil {
		return fmt.Errorf("resolving harness: %w", err)
	}

	// 3. Tokenize harness-args if provided
	var args []string
	if *harnessArgs != "" {
		parsed, err := shlex.Split(*harnessArgs, true)
		if err != nil {
			return fmt.Errorf("parsing --harness-args %q: %w", *harnessArgs, err)
		}
		args = parsed
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 4. Lock discipline: acp-session.lock taken BEFORE reading session or spawning harness (D117)
	lock, err := session.AcquireLock(ctx, *agentFolder)
	if err != nil {
		return fmt.Errorf("acquiring session lock: %w", err)
	}
	defer func() {
		_ = lock.Release()
	}()

	// 5. Read existing session file if present
	savedSession, err := session.ReadSession(*agentFolder)
	if err != nil {
		// Log warning on ownership mismatch or corruption; EstablishSession will fall back to new
		fmt.Fprintf(os.Stderr, "wackyacp: warning reading saved session: %v\n", err)
		savedSession = nil
	}

	// 6. Spawn harness subprocess with process-group isolation
	proc, err := harness.Start(ctx, harness.Config{
		Command:     *harnessCmd,
		Args:        args,
		AgentFolder: *agentFolder,
		Stderr:      os.Stderr,
	})
	if err != nil {
		return fmt.Errorf("starting harness: %w", err)
	}
	defer func() {
		_ = proc.Close()
	}()

	// 7. Initialize ACP client and perform handshake
	acpClient := acp.NewClient(proc.Stdin, proc.Stdout)
	if _, err := acpClient.Initialize(ctx); err != nil {
		return fmt.Errorf("acp initialize failed: %w", err)
	}

	// 8. Establish session via capability-driven fallback chain (resume -> load -> new)
	sessionID, err := acpClient.EstablishSession(ctx, *agentFolder, savedSession)
	if err != nil {
		return fmt.Errorf("establishing session: %w", err)
	}

	// 9. Start D112 gRPC server on stdio (dies on stdin EOF / turn end)
	srv := d112.NewServer(acpClient, sessionID, *agentFolder)
	if err := d112.ServeStdio(ctx, srv, os.Stdin, os.Stdout); err != nil {
		return fmt.Errorf("serving d112: %w", err)
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "wackyacp: %v\n", err)
		os.Exit(1)
	}
}
