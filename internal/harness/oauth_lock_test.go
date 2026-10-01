package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRecoverStaleOAuthLock_NoLock(t *testing.T) {
	claudeDir := t.TempDir()
	var stderr bytes.Buffer

	removed, err := RecoverStaleOAuthLockInDir(claudeDir, 10*time.Second, &stderr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if removed {
		t.Fatalf("expected removed=false, got true")
	}
	if stderr.Len() != 0 {
		t.Fatalf("expected empty stderr, got: %s", stderr.String())
	}
}

func TestRecoverStaleOAuthLock_DeadPID(t *testing.T) {
	claudeDir := t.TempDir()
	lockPath := filepath.Join(claudeDir, OAuthLockName)
	if err := os.Mkdir(lockPath, 0755); err != nil {
		t.Fatalf("mkdir lock: %v", err)
	}

	deadPID := 9999999
	ownerData, _ := json.Marshal(ownerRecord{PID: deadPID})
	ownerPath := filepath.Join(claudeDir, OAuthOwnerFileName)
	if err := os.WriteFile(ownerPath, ownerData, 0644); err != nil {
		t.Fatalf("write owner: %v", err)
	}

	var stderr bytes.Buffer
	removed, err := RecoverStaleOAuthLockInDir(claudeDir, 10*time.Second, &stderr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !removed {
		t.Fatalf("expected removed=true, got false")
	}

	// Verify lock directory and owner file were removed
	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatalf("expected lockPath to be deleted, err=%v", err)
	}
	if _, err := os.Stat(ownerPath); !os.IsNotExist(err) {
		t.Fatalf("expected ownerPath to be deleted, err=%v", err)
	}

	// Verify loud log
	logMsg := stderr.String()
	if !strings.Contains(logMsg, "wackyacp: WARNING: removing stale OAuth refresh lock") {
		t.Fatalf("expected loud warning log, got: %s", logMsg)
	}
	if !strings.Contains(logMsg, fmt.Sprintf("PID %d is not running", deadPID)) {
		t.Fatalf("expected log to mention dead PID, got: %s", logMsg)
	}
}

func TestRecoverStaleOAuthLock_LivePID(t *testing.T) {
	claudeDir := t.TempDir()
	lockPath := filepath.Join(claudeDir, OAuthLockName)
	if err := os.Mkdir(lockPath, 0755); err != nil {
		t.Fatalf("mkdir lock: %v", err)
	}

	// Start a background process that stays alive during the test
	cmd := exec.Command("sleep", "10")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	livePID := cmd.Process.Pid
	ownerData, _ := json.Marshal(ownerRecord{PID: livePID})
	ownerPath := filepath.Join(claudeDir, OAuthOwnerFileName)
	if err := os.WriteFile(ownerPath, ownerData, 0644); err != nil {
		t.Fatalf("write owner: %v", err)
	}

	var stderr bytes.Buffer
	removed, err := RecoverStaleOAuthLockInDir(claudeDir, 10*time.Second, &stderr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if removed {
		t.Fatalf("expected removed=false for live process holder, got true")
	}

	// Lock should still exist
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("expected lockPath to remain, err=%v", err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("expected no warning for live holder, got: %s", stderr.String())
	}
}

func TestRecoverStaleOAuthLock_LivePID_ExpiredHeartbeat(t *testing.T) {
	claudeDir := t.TempDir()
	lockPath := filepath.Join(claudeDir, OAuthLockName)
	if err := os.Mkdir(lockPath, 0755); err != nil {
		t.Fatalf("mkdir lock: %v", err)
	}

	// Set mtime to 10 minutes ago
	oldTime := time.Now().Add(-10 * time.Minute)
	if err := os.Chtimes(lockPath, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	// PID is our current process (alive)
	currentPID := os.Getpid()
	ownerData, _ := json.Marshal(ownerRecord{PID: currentPID})
	ownerPath := filepath.Join(claudeDir, OAuthOwnerFileName)
	if err := os.WriteFile(ownerPath, ownerData, 0644); err != nil {
		t.Fatalf("write owner: %v", err)
	}

	var stderr bytes.Buffer
	// Threshold = 100ms
	removed, err := RecoverStaleOAuthLockInDir(claudeDir, 100*time.Millisecond, &stderr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !removed {
		t.Fatalf("expected removed=true due to expired heartbeat, got false")
	}

	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatalf("expected lockPath to be deleted, err=%v", err)
	}
	if !strings.Contains(stderr.String(), "heartbeat expired") {
		t.Fatalf("expected heartbeat expired message, got: %s", stderr.String())
	}
}

func TestRecoverStaleOAuthLock_NoPID_StaleMtime(t *testing.T) {
	claudeDir := t.TempDir()
	lockPath := filepath.Join(claudeDir, OAuthLockName)
	if err := os.Mkdir(lockPath, 0755); err != nil {
		t.Fatalf("mkdir lock: %v", err)
	}

	// Set mtime to 5 minutes ago
	oldTime := time.Now().Add(-5 * time.Minute)
	if err := os.Chtimes(lockPath, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	var stderr bytes.Buffer
	// Threshold = 60s
	removed, err := RecoverStaleOAuthLockInDir(claudeDir, 60*time.Second, &stderr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !removed {
		t.Fatalf("expected removed=true for stale mtime without PID, got false")
	}

	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatalf("expected lockPath to be deleted, err=%v", err)
	}
	logMsg := stderr.String()
	if !strings.Contains(logMsg, "no holder PID found and lock age") {
		t.Fatalf("expected log to mention age exceeding threshold, got: %s", logMsg)
	}
}

func TestRecoverStaleOAuthLock_NoPID_FreshMtime(t *testing.T) {
	claudeDir := t.TempDir()
	lockPath := filepath.Join(claudeDir, OAuthLockName)
	if err := os.Mkdir(lockPath, 0755); err != nil {
		t.Fatalf("mkdir lock: %v", err)
	}

	var stderr bytes.Buffer
	// Threshold = 60s, lock was just created (fresh)
	removed, err := RecoverStaleOAuthLockInDir(claudeDir, 60*time.Second, &stderr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if removed {
		t.Fatalf("expected removed=false for fresh lock without PID, got true")
	}

	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("expected lockPath to remain, err=%v", err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("expected empty stderr, got: %s", stderr.String())
	}
}

func TestRecoverStaleOAuthLock_LockIsFile(t *testing.T) {
	claudeDir := t.TempDir()
	lockPath := filepath.Join(claudeDir, OAuthLockName)
	// Create lock as regular file containing dead PID
	deadPID := 9999998
	if err := os.WriteFile(lockPath, []byte(fmt.Sprintf("%d\n", deadPID)), 0644); err != nil {
		t.Fatalf("write lock file: %v", err)
	}

	var stderr bytes.Buffer
	removed, err := RecoverStaleOAuthLockInDir(claudeDir, 10*time.Second, &stderr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !removed {
		t.Fatalf("expected removed=true, got false")
	}
	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatalf("expected file lockPath to be deleted, err=%v", err)
	}
}

func TestRecoverStaleOAuthLock_CleansLegacyLocks(t *testing.T) {
	claudeDir := t.TempDir()
	lockPath := filepath.Join(claudeDir, OAuthLockName)
	if err := os.Mkdir(lockPath, 0755); err != nil {
		t.Fatalf("mkdir lock: %v", err)
	}

	ownerPath := filepath.Join(claudeDir, OAuthOwnerFileName)
	_ = os.WriteFile(ownerPath, []byte(`{"pid": 9999999}`), 0644)

	legacyLockPath := filepath.Join(claudeDir, OAuthLegacyLockName)
	_ = os.WriteFile(legacyLockPath, []byte("legacy"), 0644)

	var stderr bytes.Buffer
	removed, err := RecoverStaleOAuthLockInDir(claudeDir, 10*time.Second, &stderr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !removed {
		t.Fatalf("expected removed=true, got false")
	}

	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatalf("lockPath still exists")
	}
	if _, err := os.Stat(ownerPath); !os.IsNotExist(err) {
		t.Fatalf("ownerPath still exists")
	}
	if _, err := os.Stat(legacyLockPath); !os.IsNotExist(err) {
		t.Fatalf("legacyLockPath still exists")
	}
}

type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func TestStart_HarnessInvokesRecovery(t *testing.T) {
	claudeDir := t.TempDir()
	lockPath := filepath.Join(claudeDir, OAuthLockName)
	if err := os.Mkdir(lockPath, 0755); err != nil {
		t.Fatalf("mkdir lock: %v", err)
	}

	ownerPath := filepath.Join(claudeDir, OAuthOwnerFileName)
	_ = os.WriteFile(ownerPath, []byte(`{"pid": 9999999}`), 0644)

	var stderr safeBuffer
	cfg := Config{
		Command:   "cat",
		ClaudeDir: claudeDir,
		Stderr:    &stderr,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proc, err := Start(ctx, cfg)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer proc.Close()

	// Verify lock directory was deleted before harness started
	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatalf("expected lockPath to be deleted by Start, err=%v", err)
	}
	if !strings.Contains(stderr.String(), "wackyacp: WARNING: removing stale OAuth refresh lock") {
		t.Fatalf("expected warning log from Start, got: %s", stderr.String())
	}
}

func TestRecoverStaleOAuthLock_IgnoresNonClaudeHarness(t *testing.T) {
	staleDir := t.TempDir()
	_ = os.Mkdir(filepath.Join(staleDir, OAuthLockName), 0755)
	t.Setenv("CLAUDE_CONFIG_DIR", staleDir)

	var stderr bytes.Buffer
	cfg := Config{
		Command:     "echo",
		Args:        []string{"hello"},
		AgentFolder: t.TempDir(),
		Stderr:      &stderr,
	}

	removed, err := RecoverStaleOAuthLock(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if removed {
		t.Fatalf("expected removed=false for non-claude harness, got true")
	}
	if stderr.Len() != 0 {
		t.Fatalf("expected empty stderr, got: %s", stderr.String())
	}
	// Verify lock in staleDir was not touched
	if _, err := os.Stat(filepath.Join(staleDir, OAuthLockName)); err != nil {
		t.Fatalf("expected lock to remain untouched, err=%v", err)
	}
}

func TestRecoverStaleOAuthLock_PIDReuse(t *testing.T) {
	claudeDir := t.TempDir()
	lockPath := filepath.Join(claudeDir, OAuthLockName)
	if err := os.Mkdir(lockPath, 0755); err != nil {
		t.Fatalf("mkdir lock: %v", err)
	}

	// PID is alive (current process), but procStart is completely different (simulating PID recycle)
	ownerData, _ := json.Marshal(ownerRecord{
		PID:       os.Getpid(),
		ProcStart: "99999999999",
	})
	ownerPath := filepath.Join(claudeDir, OAuthOwnerFileName)
	if err := os.WriteFile(ownerPath, ownerData, 0644); err != nil {
		t.Fatalf("write owner: %v", err)
	}

	var stderr bytes.Buffer
	removed, err := RecoverStaleOAuthLockInDir(claudeDir, 10*time.Second, &stderr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// If /proc/<pid>/stat is readable on this OS, it should have detected the recycled start time
	if getProcessStartTime(os.Getpid()) != "" {
		if !removed {
			t.Fatalf("expected removed=true due to PID reuse, got false")
		}
		if !strings.Contains(stderr.String(), "was recycled") {
			t.Fatalf("expected log to mention PID recycled, got: %s", stderr.String())
		}
	}
}

func TestRecoverStaleOAuthLock_RemovalRaceAborts(t *testing.T) {
	claudeDir := t.TempDir()
	lockPath := filepath.Join(claudeDir, OAuthLockName)
	if err := os.Mkdir(lockPath, 0755); err != nil {
		t.Fatalf("mkdir lock: %v", err)
	}

	// Dead PID
	ownerData, _ := json.Marshal(ownerRecord{PID: 9999999})
	ownerPath := filepath.Join(claudeDir, OAuthOwnerFileName)
	_ = os.WriteFile(ownerPath, ownerData, 0644)

	// Set initial mtime to 1 minute ago (stale)
	oldTime := time.Now().Add(-1 * time.Minute)
	_ = os.Chtimes(lockPath, oldTime, oldTime)

	// Case 1: Mtime race - a concurrent process updates heartbeat between staleness decision and unlinking
	preUnlinkHook = func(p string) {
		newTime := time.Now()
		_ = os.Chtimes(p, newTime, newTime)
	}
	defer func() { preUnlinkHook = nil }()

	var stderr bytes.Buffer
	removed, err := RecoverStaleOAuthLockInDir(claudeDir, 10*time.Second, &stderr)
	if err != nil {
		t.Fatalf("unexpected error on mtime race: %v", err)
	}
	if removed {
		t.Fatalf("expected removal to abort on raced mtime, got removed=true")
	}
	// Verify lock directory was preserved
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("expected lockPath to be preserved on race abort, err=%v", err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("expected no removal warning on race abort, got: %s", stderr.String())
	}

	// Case 2: Inode race - a concurrent process replaces the lock directory with a fresh lock
	_ = os.Chtimes(lockPath, oldTime, oldTime)
	preUnlinkHook = func(p string) {
		_ = os.Remove(p)
		_ = os.Mkdir(p, 0755)
	}

	stderr.Reset()
	removed, err = RecoverStaleOAuthLockInDir(claudeDir, 10*time.Second, &stderr)
	if err != nil {
		t.Fatalf("unexpected error on inode race: %v", err)
	}
	if removed {
		t.Fatalf("expected removal to abort on raced inode, got removed=true")
	}
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("expected recreated lockPath to be preserved, err=%v", err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("expected no removal warning on inode race abort, got: %s", stderr.String())
	}
}

func TestRecoverStaleOAuthLock_CredentialsJSONLock_DeadPID(t *testing.T) {
	claudeDir := t.TempDir()
	lockPath := filepath.Join(claudeDir, CredentialsJSONLockName)
	if err := os.Mkdir(lockPath, 0755); err != nil {
		t.Fatalf("mkdir lock: %v", err)
	}

	deadPID := 9999999
	ownerData, _ := json.Marshal(ownerRecord{PID: deadPID})
	ownerPath := filepath.Join(claudeDir, CredentialsJSONLockName+".owner")
	if err := os.WriteFile(ownerPath, ownerData, 0644); err != nil {
		t.Fatalf("write owner: %v", err)
	}

	var stderr bytes.Buffer
	removed, err := RecoverStaleOAuthLockInDir(claudeDir, 10*time.Second, &stderr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !removed {
		t.Fatalf("expected removed=true, got false")
	}

	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatalf("expected lockPath to be deleted, err=%v", err)
	}
	if _, err := os.Stat(ownerPath); !os.IsNotExist(err) {
		t.Fatalf("expected ownerPath to be deleted, err=%v", err)
	}
	if !strings.Contains(stderr.String(), "wackyacp: WARNING: removing stale OAuth refresh lock") {
		t.Fatalf("expected warning log, got: %s", stderr.String())
	}
}

func TestRecoverStaleOAuthLock_CredentialsJSONLock_StaleMtime(t *testing.T) {
	// Replicates anthropics/claude-code#95236 where the lock directory was left behind empty
	claudeDir := t.TempDir()
	lockPath := filepath.Join(claudeDir, CredentialsJSONLockName)
	if err := os.Mkdir(lockPath, 0755); err != nil {
		t.Fatalf("mkdir lock: %v", err)
	}

	oldTime := time.Now().Add(-5 * time.Minute)
	if err := os.Chtimes(lockPath, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	var stderr bytes.Buffer
	removed, err := RecoverStaleOAuthLockInDir(claudeDir, 60*time.Second, &stderr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !removed {
		t.Fatalf("expected removed=true for stale empty lock directory, got false")
	}

	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatalf("expected lockPath to be deleted, err=%v", err)
	}
	if !strings.Contains(stderr.String(), "no holder PID found and lock age") {
		t.Fatalf("expected log to mention age exceeding threshold, got: %s", stderr.String())
	}
}

func TestRecoverStaleOAuthLock_CredentialsJSONLock_LivePID(t *testing.T) {
	claudeDir := t.TempDir()
	lockPath := filepath.Join(claudeDir, CredentialsJSONLockName)
	if err := os.Mkdir(lockPath, 0755); err != nil {
		t.Fatalf("mkdir lock: %v", err)
	}

	cmd := exec.Command("sleep", "10")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	ownerData, _ := json.Marshal(ownerRecord{PID: cmd.Process.Pid})
	ownerPath := filepath.Join(claudeDir, CredentialsJSONLockName+".owner")
	if err := os.WriteFile(ownerPath, ownerData, 0644); err != nil {
		t.Fatalf("write owner: %v", err)
	}

	var stderr bytes.Buffer
	removed, err := RecoverStaleOAuthLockInDir(claudeDir, 10*time.Second, &stderr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if removed {
		t.Fatalf("expected removed=false for live holder, got true")
	}

	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("expected lockPath to remain, err=%v", err)
	}
}

func TestRecoverStaleOAuthLock_LegacyLockOnly(t *testing.T) {
	claudeDir := t.TempDir()
	legacyLockPath := filepath.Join(claudeDir, OAuthLegacyLockName)
	if err := os.Mkdir(legacyLockPath, 0755); err != nil {
		t.Fatalf("mkdir legacy lock: %v", err)
	}

	oldTime := time.Now().Add(-5 * time.Minute)
	_ = os.Chtimes(legacyLockPath, oldTime, oldTime)

	var stderr bytes.Buffer
	removed, err := RecoverStaleOAuthLockInDir(claudeDir, 60*time.Second, &stderr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !removed {
		t.Fatalf("expected removed=true for standalone legacy lock, got false")
	}
	if _, err := os.Stat(legacyLockPath); !os.IsNotExist(err) {
		t.Fatalf("expected legacyLockPath to be deleted, err=%v", err)
	}
}

func TestRecoverStaleOAuthLock_ClaudeDirLock(t *testing.T) {
	parentDir := t.TempDir()
	claudeDir := filepath.Join(parentDir, ".claude")
	if err := os.Mkdir(claudeDir, 0755); err != nil {
		t.Fatalf("mkdir claudeDir: %v", err)
	}
	dirLockPath := claudeDir + ".lock"
	if err := os.Mkdir(dirLockPath, 0755); err != nil {
		t.Fatalf("mkdir dir lock: %v", err)
	}

	oldTime := time.Now().Add(-5 * time.Minute)
	_ = os.Chtimes(dirLockPath, oldTime, oldTime)

	var stderr bytes.Buffer
	removed, err := RecoverStaleOAuthLockInDir(claudeDir, 60*time.Second, &stderr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !removed {
		t.Fatalf("expected removed=true for dir lock, got false")
	}
	if _, err := os.Stat(dirLockPath); !os.IsNotExist(err) {
		t.Fatalf("expected dirLockPath to be deleted, err=%v", err)
	}
}

func TestRecoverStaleOAuthLock_BothPrimaryAndLegacyStale(t *testing.T) {
	claudeDir := t.TempDir()
	primaryLock := filepath.Join(claudeDir, OAuthLockName)
	if err := os.Mkdir(primaryLock, 0755); err != nil {
		t.Fatalf("mkdir primary lock: %v", err)
	}
	credLock := filepath.Join(claudeDir, CredentialsJSONLockName)
	if err := os.Mkdir(credLock, 0755); err != nil {
		t.Fatalf("mkdir cred lock: %v", err)
	}

	oldTime := time.Now().Add(-5 * time.Minute)
	_ = os.Chtimes(primaryLock, oldTime, oldTime)
	_ = os.Chtimes(credLock, oldTime, oldTime)

	var stderr bytes.Buffer
	removed, err := RecoverStaleOAuthLockInDir(claudeDir, 60*time.Second, &stderr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !removed {
		t.Fatalf("expected removed=true, got false")
	}

	if _, err := os.Stat(primaryLock); !os.IsNotExist(err) {
		t.Fatalf("expected primaryLock to be deleted, err=%v", err)
	}
	if _, err := os.Stat(credLock); !os.IsNotExist(err) {
		t.Fatalf("expected credLock to be deleted, err=%v", err)
	}
}

func TestRecoverStaleOAuthLock_AllStaleShapesWorkspace(t *testing.T) {
	parentDir := t.TempDir()
	claudeDir := filepath.Join(parentDir, ".claude")
	if err := os.Mkdir(claudeDir, 0755); err != nil {
		t.Fatalf("mkdir claudeDir: %v", err)
	}

	// Real credentials file: must remain byte-identical at mode 0600
	credFile := filepath.Join(claudeDir, ".credentials.json")
	credContent := []byte(`{"access_token": "secret_oauth_token", "refresh_token": "refresh_xyz"}`)
	if err := os.WriteFile(credFile, credContent, 0600); err != nil {
		t.Fatalf("write credentials.json: %v", err)
	}

	oldTime := time.Now().Add(-10 * time.Minute)

	// 1. dir-shaped .credentials.json.lock (10min old)
	credJSONLock := filepath.Join(claudeDir, ".credentials.json.lock")
	if err := os.Mkdir(credJSONLock, 0755); err != nil {
		t.Fatalf("mkdir credJSONLock: %v", err)
	}
	_ = os.Chtimes(credJSONLock, oldTime, oldTime)

	// 2. .credentials.lock (dir-shaped)
	credLock := filepath.Join(claudeDir, ".credentials.lock")
	if err := os.Mkdir(credLock, 0755); err != nil {
		t.Fatalf("mkdir credLock: %v", err)
	}
	_ = os.Chtimes(credLock, oldTime, oldTime)

	// 3. .oauth_refresh.lock.lock
	oauthLegacyLock := filepath.Join(claudeDir, ".oauth_refresh.lock.lock")
	if err := os.Mkdir(oauthLegacyLock, 0755); err != nil {
		t.Fatalf("mkdir oauthLegacyLock: %v", err)
	}
	_ = os.Chtimes(oauthLegacyLock, oldTime, oldTime)

	// 4. .credentials.json.lock.lock
	credJSONLegacyLock := filepath.Join(claudeDir, ".credentials.json.lock.lock")
	if err := os.Mkdir(credJSONLegacyLock, 0755); err != nil {
		t.Fatalf("mkdir credJSONLegacyLock: %v", err)
	}
	_ = os.Chtimes(credJSONLegacyLock, oldTime, oldTime)

	// 5. file-shaped .oauth_refresh.lock
	oauthFileLock := filepath.Join(claudeDir, ".oauth_refresh.lock")
	if err := os.WriteFile(oauthFileLock, []byte("stale file lock"), 0644); err != nil {
		t.Fatalf("write oauthFileLock: %v", err)
	}
	_ = os.Chtimes(oauthFileLock, oldTime, oldTime)

	// 6. <claudeDir>.lock shadow
	dirShadowLock := claudeDir + ".lock"
	if err := os.Mkdir(dirShadowLock, 0755); err != nil {
		t.Fatalf("mkdir dirShadowLock: %v", err)
	}
	_ = os.Chtimes(dirShadowLock, oldTime, oldTime)

	var stderr bytes.Buffer
	removed, err := RecoverStaleOAuthLockInDir(claudeDir, 60*time.Second, &stderr)
	if err != nil {
		t.Fatalf("unexpected error recovering all stale shapes: %v", err)
	}
	if !removed {
		t.Fatalf("expected removed=true for all stale shapes workspace, got false")
	}

	// Verify all 6 lock paths were removed
	lockPaths := map[string]string{
		"dir-shaped .credentials.json.lock": credJSONLock,
		".credentials.lock":                 credLock,
		".oauth_refresh.lock.lock":          oauthLegacyLock,
		".credentials.json.lock.lock":       credJSONLegacyLock,
		"file-shaped .oauth_refresh.lock":   oauthFileLock,
		"<claudeDir>.lock shadow":           dirShadowLock,
	}
	for name, path := range lockPaths {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("expected %s (%q) to be deleted, err=%v", name, path, err)
		}
	}

	// Verify .credentials.json remains byte-identical and at mode 0600
	afterData, err := os.ReadFile(credFile)
	if err != nil {
		t.Fatalf("read credentials.json after recovery: %v", err)
	}
	if !bytes.Equal(afterData, credContent) {
		t.Fatalf("expected credentials.json to be byte-identical, got %q vs %q", string(afterData), string(credContent))
	}
	fi, err := os.Stat(credFile)
	if err != nil {
		t.Fatalf("stat credentials.json after recovery: %v", err)
	}
	if fi.Mode().Perm() != 0600 {
		t.Fatalf("expected credentials.json mode 0600, got %#o", fi.Mode().Perm())
	}
}

func TestRecoverStaleOAuthLock_LiveHolderLegacyDirectoryLock(t *testing.T) {
	parentDir := t.TempDir()
	claudeDir := filepath.Join(parentDir, ".claude")
	if err := os.Mkdir(claudeDir, 0755); err != nil {
		t.Fatalf("mkdir claudeDir: %v", err)
	}
	dirLockPath := claudeDir + ".lock"
	if err := os.Mkdir(dirLockPath, 0755); err != nil {
		t.Fatalf("mkdir dirLock: %v", err)
	}

	cmd := exec.Command("sleep", "10")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	ownerData, _ := json.Marshal(ownerRecord{PID: cmd.Process.Pid})
	ownerPath := dirLockPath + ".owner"
	if err := os.WriteFile(ownerPath, ownerData, 0644); err != nil {
		t.Fatalf("write owner: %v", err)
	}

	var stderr bytes.Buffer
	removed, err := RecoverStaleOAuthLockInDir(claudeDir, 10*time.Second, &stderr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if removed {
		t.Fatalf("expected removed=false for live holder on legacy directory lock, got true")
	}

	// Verify legacy directory lock and owner file survive intact
	if _, err := os.Stat(dirLockPath); err != nil {
		t.Fatalf("expected dirLockPath to remain intact, err=%v", err)
	}
	if _, err := os.Stat(ownerPath); err != nil {
		t.Fatalf("expected ownerPath to remain intact, err=%v", err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("expected empty stderr, got: %s", stderr.String())
	}
}
