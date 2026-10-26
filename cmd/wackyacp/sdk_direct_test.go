package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/colinrgodsey/wackyacp/internal/harness"
)

// seedLock creates an OAuth refresh lock with a backdated mtime. Passing an
// explicit claudeDir keeps these tests hermetic: the lock probe then looks only
// at that directory instead of also scanning the developer's real ~/.claude.
func seedLock(t *testing.T, dir string, age time.Duration) string {
	t.Helper()
	lock := filepath.Join(dir, harness.OAuthLockName)
	if err := os.MkdirAll(lock, 0o755); err != nil {
		t.Fatalf("creating lock %s: %v", lock, err)
	}
	stamp := time.Now().Add(-age)
	if err := os.Chtimes(lock, stamp, stamp); err != nil {
		t.Fatalf("backdating lock %s: %v", lock, err)
	}
	return lock
}

func TestSDKModeReclaimsStaleOAuthRefreshLock(t *testing.T) {
	claudeDir := t.TempDir()
	lock := seedLock(t, claudeDir, 5*time.Minute)

	var stderr bytes.Buffer
	recoverOAuthRefreshLock(t.TempDir(), claudeDir, &stderr)

	if _, err := os.Stat(lock); !os.IsNotExist(err) {
		t.Errorf("SDK-direct mode left a stale oauth refresh lock in place (stat err=%v, stderr=%q); turns would stall behind it", err, stderr.String())
	}
}

func TestSDKModeLeavesLiveOAuthRefreshLockAlone(t *testing.T) {
	claudeDir := t.TempDir()
	lock := seedLock(t, claudeDir, 0)

	var stderr bytes.Buffer
	recoverOAuthRefreshLock(t.TempDir(), claudeDir, &stderr)

	if _, err := os.Stat(lock); err != nil {
		t.Errorf("a live oauth refresh lock was removed: %v", err)
	}
}

func TestSDKModeWarnsWhenCLAUDEMarkdownWouldDoubleLoad(t *testing.T) {
	agentFolder := t.TempDir()

	var stderr bytes.Buffer
	warnIfClaudeMarkdown(agentFolder, &stderr)
	if stderr.Len() != 0 {
		t.Errorf("an agent folder without CLAUDE.md produced a warning: %q", stderr.String())
	}

	if err := os.WriteFile(filepath.Join(agentFolder, claudeMarkdownName), []byte("project instructions"), 0o644); err != nil {
		t.Fatalf("writing CLAUDE.md: %v", err)
	}
	stderr.Reset()
	warnIfClaudeMarkdown(agentFolder, &stderr)
	if !strings.Contains(stderr.String(), claudeMarkdownName) {
		t.Errorf("expected a warning that the SDK would load CLAUDE.md alongside the composed prompt, got %q", stderr.String())
	}
}
