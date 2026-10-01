package harness

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// DefaultOAuthLockStaleThreshold is the default duration after which an OAuth
// refresh lock without an active holder is considered stale. Matches Claude Code's
// 60-second lock stale window.
const DefaultOAuthLockStaleThreshold = 60 * time.Second

const (
	OAuthLockName           = ".oauth_refresh.lock"
	OAuthOwnerFileName      = ".oauth_refresh.lock.owner"
	OAuthLegacyLockName     = ".oauth_refresh.lock.lock"
	CredentialsJSONLockName = ".credentials.json.lock"
	CredentialsLockName     = ".credentials.lock"
)

type lockCandidate struct {
	lockPath   string
	ownerPath  string
	legacyPath string
}

type ownerRecord struct {
	PID       int `json:"pid"`
	ProcStart any `json:"procStart,omitempty"`
}

// RecoverStaleOAuthLock checks candidate Claude configuration directories for stale
// OAuth and credentials refresh locks and removes them if no live holder process exists.
// It returns whether any stale lock was removed and any error encountered.
func RecoverStaleOAuthLock(cfg Config) (bool, error) {
	dirs := claudeCandidateDirs(cfg)
	if len(dirs) == 0 {
		return false, nil
	}

	threshold := cfg.OAuthLockStaleThreshold
	if threshold <= 0 {
		threshold = DefaultOAuthLockStaleThreshold
	}

	stderr := cfg.Stderr
	if stderr == nil {
		stderr = os.Stderr
	}

	anyRemoved := false
	var firstErr error
	for _, dir := range dirs {
		removed, err := RecoverStaleOAuthLockInDir(dir, threshold, stderr)
		if removed {
			anyRemoved = true
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return anyRemoved, firstErr
}

// candidateLocksForDir generates the candidate lock targets in and around a Claude directory.
// Upstream Claude Code (anthropics/claude-code#95236) uses both the primary OAuth lock
// (.oauth_refresh.lock) and legacy lockfile paths on credentials (${credentialsPath}.lock,
// e.g. .credentials.json.lock or <claudeDir>.lock) via proper-lockfile.
func candidateLocksForDir(dir string) []lockCandidate {
	cleanDir := filepath.Clean(dir)
	candidates := []lockCandidate{
		{
			lockPath:   filepath.Join(cleanDir, OAuthLockName),
			ownerPath:  filepath.Join(cleanDir, OAuthOwnerFileName),
			legacyPath: filepath.Join(cleanDir, OAuthLegacyLockName),
		},
		{
			lockPath:   filepath.Join(cleanDir, CredentialsJSONLockName),
			ownerPath:  filepath.Join(cleanDir, CredentialsJSONLockName+".owner"),
			legacyPath: filepath.Join(cleanDir, CredentialsJSONLockName+".lock"),
		},
		{
			lockPath:   filepath.Join(cleanDir, CredentialsLockName),
			ownerPath:  filepath.Join(cleanDir, CredentialsLockName+".owner"),
			legacyPath: filepath.Join(cleanDir, CredentialsLockName+".lock"),
		},
		{
			lockPath:   filepath.Join(cleanDir, OAuthLegacyLockName),
			ownerPath:  filepath.Join(cleanDir, OAuthLegacyLockName+".owner"),
			legacyPath: "",
		},
		{
			lockPath:   filepath.Join(cleanDir, CredentialsJSONLockName+".lock"),
			ownerPath:  filepath.Join(cleanDir, CredentialsJSONLockName+".lock.owner"),
			legacyPath: "",
		},
	}
	if cleanDir != "/" && cleanDir != "." {
		candidates = append(candidates, lockCandidate{
			lockPath:   cleanDir + ".lock",
			ownerPath:  cleanDir + ".lock.owner",
			legacyPath: cleanDir + ".lock.lock",
		})
	}
	return candidates
}

// RecoverStaleOAuthLockInDir inspects a single Claude directory (e.g. ~/.claude)
// for stale OAuth and credentials refresh locks (.oauth_refresh.lock, .credentials.json.lock,
// .credentials.lock, and directory-level locks). If any lock exists and has no live
// holder process (verified via PID liveness, zombie detection, PID reuse checks,
// heartbeat mtime, or fallback mtime age threshold), it removes the stale lock,
// cleans up associated owner/legacy lock files, and logs loudly.
func RecoverStaleOAuthLockInDir(dir string, threshold time.Duration, stderr io.Writer) (bool, error) {
	if dir == "" {
		return false, nil
	}
	if threshold <= 0 {
		threshold = DefaultOAuthLockStaleThreshold
	}
	if stderr == nil {
		stderr = os.Stderr
	}

	candidates := candidateLocksForDir(dir)
	anyRemoved := false
	var firstErr error

	for _, c := range candidates {
		removed, err := recoverSingleLock(c.lockPath, c.ownerPath, c.legacyPath, threshold, stderr)
		if removed {
			anyRemoved = true
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return anyRemoved, firstErr
}

// preUnlinkHook is an optional test hook called immediately before re-statting
// and unlinking a stale lock in recoverSingleLock to allow deterministic testing
// of removal race conditions.
var preUnlinkHook func(lockPath string)

// recoverSingleLock inspects a specific lock path and associated owner/legacy lock files.
// It verifies holder liveness, heartbeats, and age thresholds, ties the staleness decision
// directly to removal via pre-unlinking re-stat, performs atomic removal, and logs loudly.
func recoverSingleLock(lockPath, ownerPath, legacyLockPath string, threshold time.Duration, stderr io.Writer) (bool, error) {
	stat, err := os.Stat(lockPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("statting lock %q: %w", lockPath, err)
	}

	pid, procStart, hasPID := readHolderPID(lockPath, ownerPath, stat.IsDir())

	isStale := false
	var reason string

	if hasPID {
		alive, deadReason := isProcessAlive(pid, procStart)
		if !alive {
			isStale = true
			reason = deadReason
		} else {
			// Process is alive: verify heartbeat is still active
			age := time.Since(stat.ModTime())
			if age > threshold {
				isStale = true
				reason = fmt.Sprintf("holder process PID %d heartbeat expired (lock age %v exceeds threshold %v)", pid, age.Round(time.Millisecond), threshold)
			}
		}
	} else {
		// No PID found in lock or owner record: use filesystem-level liveness probe (mtime age vs threshold)
		age := time.Since(stat.ModTime())
		if age > threshold {
			isStale = true
			reason = fmt.Sprintf("no holder PID found and lock age %v exceeds threshold %v", age.Round(time.Millisecond), threshold)
		}
	}

	if !isStale {
		return false, nil
	}

	if preUnlinkHook != nil {
		preUnlinkHook(lockPath)
	}

	// Tie staleness decision directly to removal: re-stat lockPath immediately
	// before unlinking. Both file identity (same inode) and mtime must match
	// the vetted state, ensuring a fresh lock acquired or updated during analysis
	// is left alone.
	stat2, err := os.Stat(lockPath)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil // Already removed by concurrent process
		}
		return false, fmt.Errorf("re-statting lock %q: %w", lockPath, err)
	}
	if !os.SameFile(stat, stat2) || !stat2.ModTime().Equal(stat.ModTime()) {
		// Lock was replaced or heartbeated concurrently: abort removal
		return false, nil
	}

	// Perform removal: prefer non-recursive removal for directory-shaped locks.
	// Clean known files then remove the empty directory with os.Remove.
	var removeErr error
	if stat.IsDir() {
		for _, name := range []string{"pid", "owner", ".owner"} {
			_ = os.Remove(filepath.Join(lockPath, name))
		}
		if err := os.Remove(lockPath); err != nil && !os.IsNotExist(err) {
			// If unexpected entries prevent rmdir, fall back to RemoveAll
			if err2 := os.RemoveAll(lockPath); err2 != nil && !os.IsNotExist(err2) {
				removeErr = fmt.Errorf("removing stale lock %q: %w", lockPath, err2)
			}
		}
	} else {
		if err := os.Remove(lockPath); err != nil && !os.IsNotExist(err) {
			removeErr = fmt.Errorf("removing stale lock %q: %w", lockPath, err)
		}
	}

	if ownerPath != "" {
		if err := os.Remove(ownerPath); err != nil && !os.IsNotExist(err) {
			if removeErr == nil {
				removeErr = fmt.Errorf("removing lock owner file %q: %w", ownerPath, err)
			}
		}
	}
	if legacyLockPath != "" {
		if err := os.Remove(legacyLockPath); err != nil && !os.IsNotExist(err) {
			_ = os.RemoveAll(legacyLockPath)
		}
	}

	if removeErr != nil {
		fmt.Fprintf(stderr, "wackyacp: ERROR: failed to remove stale OAuth refresh lock %q: %v\n", lockPath, removeErr)
		return false, removeErr
	}

	// Log loudly after unlinking so stderr I/O latency does not widen the TOCTOU race
	fmt.Fprintf(stderr, "wackyacp: WARNING: removing stale OAuth refresh lock %q (%s)\n", lockPath, reason)
	return true, nil
}

func readHolderPID(lockPath, ownerPath string, isDir bool) (int, string, bool) {
	// 1. Check owner file (.oauth_refresh.lock.owner, etc.)
	if ownerPath != "" {
		if data, err := os.ReadFile(ownerPath); err == nil {
			if pid, procStart := parsePIDData(data); pid > 0 {
				return pid, procStart, true
			}
		}
	}

	// Also check altOwnerPath if ownerPath is foo.lock.owner vs foo.owner
	if strings.HasSuffix(lockPath, ".lock") {
		altOwnerPath := strings.TrimSuffix(lockPath, ".lock") + ".owner"
		if altOwnerPath != ownerPath {
			if data, err := os.ReadFile(altOwnerPath); err == nil {
				if pid, procStart := parsePIDData(data); pid > 0 {
					return pid, procStart, true
				}
			}
		}
	}

	// 2. If lockPath is a directory, check for pid or owner files inside it
	if isDir {
		for _, name := range []string{"pid", "owner", ".owner"} {
			subPath := filepath.Join(lockPath, name)
			if data, err := os.ReadFile(subPath); err == nil {
				if pid, procStart := parsePIDData(data); pid > 0 {
					return pid, procStart, true
				}
			}
		}
	} else {
		// 3. If lockPath is a file, check its content directly
		if data, err := os.ReadFile(lockPath); err == nil {
			if pid, procStart := parsePIDData(data); pid > 0 {
				return pid, procStart, true
			}
		}
	}

	return 0, "", false
}

func parsePIDData(data []byte) (int, string) {
	var rec ownerRecord
	if err := json.Unmarshal(data, &rec); err == nil && rec.PID > 0 {
		var procStart string
		if rec.ProcStart != nil {
			procStart = fmt.Sprint(rec.ProcStart)
		}
		return rec.PID, procStart
	}
	trimmed := strings.TrimSpace(string(data))
	if pid, err := strconv.Atoi(trimmed); err == nil && pid > 0 {
		return pid, ""
	}
	return 0, ""
}

func isProcessAlive(pid int, procStart string) (bool, string) {
	if pid <= 0 {
		return false, "invalid PID"
	}
	err := syscall.Kill(pid, 0)
	if errors.Is(err, syscall.ESRCH) {
		return false, fmt.Sprintf("holder process PID %d is not running", pid)
	}
	if err != nil && !errors.Is(err, syscall.EPERM) {
		// Defensive fallback: if unexpected error, treat as alive
		return true, ""
	}

	// Zombie check: an exited process unreaped by its parent returns nil to kill(0),
	// but /proc/<pid>/stat reports state 'Z' (zombie) or 'X' (dead).
	if isZombie(pid) {
		return false, fmt.Sprintf("holder process PID %d is a zombie process", pid)
	}

	// PID reuse check via start time: if the owner record recorded process start time,
	// verify it matches the running process's start time in /proc/<pid>/stat.
	if procStart != "" {
		if currentStart := getProcessStartTime(pid); currentStart != "" && currentStart != procStart {
			return false, fmt.Sprintf("holder process PID %d was recycled (start time %s != %s)", pid, currentStart, procStart)
		}
	}

	return true, ""
}

// isZombie reports whether pid is a zombie (exited but not reaped by parent).
func isZombie(pid int) bool {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	content := string(data)
	lastParen := strings.LastIndex(content, ")")
	if lastParen != -1 && lastParen+1 < len(content) {
		fields := strings.Fields(content[lastParen+1:])
		if len(fields) > 0 {
			// Field 0 after ')' is state: 'Z' is zombie, 'X' is dead
			return fields[0] == "Z" || fields[0] == "X"
		}
	}
	return false
}

// getProcessStartTime retrieves the process start-time signature from /proc/<pid>/stat
// for PID reuse validation (field 22 in /proc/<pid>/stat, index 19 after ')').
func getProcessStartTime(pid int) string {
	if pid <= 0 {
		return ""
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err == nil {
		content := string(data)
		lastParen := strings.LastIndex(content, ")")
		if lastParen != -1 && lastParen+1 < len(content) {
			fields := strings.Fields(content[lastParen+1:])
			if len(fields) > 19 {
				return fields[19]
			}
		}
	}
	return ""
}

func claudeCandidateDirs(cfg Config) []string {
	if cfg.ClaudeDir != "" {
		return []string{cfg.ClaudeDir}
	}

	if !isClaudeTarget(cfg) {
		return nil
	}

	var dirs []string
	seen := make(map[string]bool)
	add := func(dir string) {
		if dir == "" {
			return
		}
		clean := filepath.Clean(dir)
		if !seen[clean] {
			seen[clean] = true
			dirs = append(dirs, clean)
		}
	}

	if env := os.Getenv("CLAUDE_CONFIG_DIR"); env != "" {
		add(env)
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		add(filepath.Join(home, ".claude"))
	}
	if cfg.AgentFolder != "" {
		agentClaude := filepath.Join(cfg.AgentFolder, ".claude")
		if fi, err := os.Stat(agentClaude); err == nil && fi.IsDir() {
			add(agentClaude)
		}
	}
	return dirs
}

// isClaudeTarget checks if the harness invocation is associated with Claude.
// Note: this intentionally uses substring matching (rather than exact binary or folder name
// equality) so that paths like /abs/path/to/claude-agent-acp, arguments like
// -y @agentclientprotocol/claude-agent-acp, and folders like /home/.../workspace/claude all match.
func isClaudeTarget(cfg Config) bool {
	if cfg.ClaudeDir != "" {
		return true
	}
	if strings.Contains(strings.ToLower(cfg.Command), "claude") {
		return true
	}
	for _, arg := range cfg.Args {
		if strings.Contains(strings.ToLower(arg), "claude") {
			return true
		}
	}
	if strings.Contains(strings.ToLower(cfg.AgentFolder), "claude") {
		return true
	}
	return false
}
