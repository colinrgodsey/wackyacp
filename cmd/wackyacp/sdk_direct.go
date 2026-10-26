package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/colinrgodsey/wackyacp/internal/claudesdk"
	"github.com/colinrgodsey/wackyacp/internal/d112"
	"github.com/colinrgodsey/wackyacp/internal/harness"
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
	// claudeDir overrides which Claude configuration directory the
	// stale OAuth lock probe inspects. Empty resolves the same way the ACP
	// path resolves it.
	claudeDir string
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

	// OAuth refresh lock parity with the ACP path, where harness.Start reclaims
	// a lock a dead claude died holding before spawning anything. The SDK worker
	// leaves claude children that can die holding the very same lock.
	recoverOAuthRefreshLock(o.agentFolder, o.claudeDir, os.Stderr)
	warnIfClaudeMarkdown(o.agentFolder, os.Stderr)

	drv, err := claudesdk.New(claudesdk.Config{
		AgentFolder:           o.agentFolder,
		BridgeCtx:             ctx,
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

// sdkRuntimeDescriptor names the runtime SDK-direct mode drives. Claude Code is
// the process that owns .oauth_refresh.lock, and passing it as the harness
// descriptor keeps lock-directory resolution shared with the ACP path instead of
// reimplementing that decision here.
const sdkRuntimeDescriptor = "claude"

// recoverOAuthRefreshLock reclaims a stale Claude OAuth refresh lock left behind
// by a claude process that died mid-refresh. An empty claudeDir keeps the same
// candidate set the ACP path probes: CLAUDE_CONFIG_DIR, the user's ~/.claude, and
// the agent folder's own .claude when it exists.
func recoverOAuthRefreshLock(agentFolder, claudeDir string, stderr io.Writer) {
	if stderr == nil {
		stderr = os.Stderr
	}
	if _, err := harness.RecoverStaleOAuthLock(harness.Config{
		Command:     sdkRuntimeDescriptor,
		AgentFolder: agentFolder,
		ClaudeDir:   claudeDir,
		Stderr:      stderr,
	}); err != nil {
		fmt.Fprintln(stderr, "wackyacp: warning: checking oauth refresh lock:", err)
	}
}

// claudeMarkdownName is the file Claude Code loads as project instructions when
// settingSources includes "project", which it must for .claude/skills discovery.
const claudeMarkdownName = "CLAUDE.md"

// warnIfClaudeMarkdown flags the one way SDK-direct mode can silently load two
// sets of instructions: a CLAUDE.md in the agent folder would be layered on top
// of the wackypub-rendered prompt, doubling and partly overriding it. Nothing
// creates such a file today, so this is a tripwire, not a fix.
func warnIfClaudeMarkdown(agentFolder string, stderr io.Writer) {
	if stderr == nil {
		stderr = os.Stderr
	}
	if _, err := os.Stat(filepath.Join(agentFolder, claudeMarkdownName)); err == nil {
		fmt.Fprintln(stderr, "wackyacp: warning:", filepath.Join(agentFolder, claudeMarkdownName),
			"will be loaded by the SDK as project instructions on top of the composed AGENTS.md prompt")
	}
}
