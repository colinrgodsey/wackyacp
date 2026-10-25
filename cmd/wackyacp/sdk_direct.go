package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/colinrgodsey/wackyacp/internal/claudesdk"
	"github.com/colinrgodsey/wackyacp/internal/d112"
	"github.com/colinrgodsey/wackyacp/internal/session"
)

// Runtime drivers selected by --driver. acp is the shipped default and stays
// that way until the REMOTE_MANIFEST cutover, so claude-sdk must never change
// behaviour for a bridge that does not ask for it.
const (
	driverACP       = "acp"
	driverClaudeSDK = "claude-sdk"
)

type sdkDirectOptions struct {
	agentFolder      string
	systemPromptMode string
	sdkDir           string
	nodeBin          string
	additionalDirs   string
}

// runSDKDirect serves the same D112 gRPC surface as the ACP path, backed by the
// Claude Agent SDK instead of a harness subprocess.
//
// Lock discipline is unchanged: acp-session.lock is still taken before any
// session read, and the SDK worker starts lazily on the first turn so a bridge
// that is only probed for Status costs no Claude process.
func runSDKDirect(o sdkDirectOptions) error {
	mode, err := claudesdk.ParseSystemPromptMode(o.systemPromptMode)
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	lock, err := session.AcquireLock(ctx, o.agentFolder)
	if err != nil {
		return fmt.Errorf("acquiring session lock: %w", err)
	}
	defer func() {
		_ = lock.Release()
	}()

	saved, err := session.ReadSession(o.agentFolder)
	if err != nil {
		fmt.Fprintf(os.Stderr, "wackyacp: warning reading saved session: %v\n", err)
		saved = nil
	}
	savedID := ""
	if saved != nil {
		savedID = saved.SessionID
	}

	drv, err := claudesdk.New(claudesdk.Config{
		AgentFolder:           o.agentFolder,
		SystemPromptMode:      mode,
		SessionID:             savedID,
		NodeBin:               o.nodeBin,
		SDKDir:                o.sdkDir,
		AdditionalDirectories: splitDirs(o.additionalDirs),
		OnSessionEstablished: func(sessionID string) {
			// A bridge that started without a session file must persist the id
			// the first turn created, or the next bridge cannot resume it.
			if sessionID == "" || sessionID == savedID {
				return
			}
			savedID = sessionID
			if _, err := session.WriteSession(o.agentFolder, sessionID); err != nil {
				fmt.Fprintf(os.Stderr, "wackyacp: warning writing session file: %v\n", err)
			}
		},
	})
	if err != nil {
		return err
	}
	defer func() {
		_ = drv.Close()
	}()

	srv := d112.NewServer(drv, savedID, o.agentFolder)
	if err := d112.ServeStdio(ctx, srv, os.Stdin, os.Stdout); err != nil {
		return fmt.Errorf("serving d112: %w", err)
	}
	return nil
}

// splitDirs parses the comma-separated --additional-directories value.
func splitDirs(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
