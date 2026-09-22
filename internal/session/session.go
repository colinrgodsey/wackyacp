package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const (
	LockFileName    = "acp-session.lock"
	SessionFileName = "acp-session.json"
)

// ErrOwnershipMismatch indicates the session file belongs to a different agent folder.
var ErrOwnershipMismatch = errors.New("session ownership assertion failed")

// SessionData stores persisted session identity and ownership metadata.
type SessionData struct {
	SessionID   string    `json:"sessionId"`
	AgentFolder string    `json:"agent_folder"`
	CreatedAt   time.Time `json:"createdAt"`
}

// Lock holds the flock on acp-session.lock.
type Lock struct {
	file *os.File
	path string
}

// AcquireLock acquires an exclusive flock on acp-session.lock in the given agentFolder.
// It polls with LOCK_NB and responds to context cancellation.
func AcquireLock(ctx context.Context, agentFolder string) (*Lock, error) {
	if err := os.MkdirAll(agentFolder, 0755); err != nil {
		return nil, fmt.Errorf("creating agent folder for lock: %w", err)
	}

	lockPath := filepath.Join(agentFolder, LockFileName)
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("opening lock file %s: %w", lockPath, err)
	}

	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	// contended is set on the first EWOULDBLOCK so the wait-visibility line is printed ONCE
	// instead of every 25ms tick - the caller (and any wrapper that captures stderr as
	// diagnostic context) sees at most one line per acquisition.
	contended := false

	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			if contended {
				fmt.Fprintf(os.Stderr, "acp-session: acquired lock on %s after waiting\n", lockPath)
			}
			return &Lock{file: f, path: lockPath}, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			_ = f.Close()
			return nil, fmt.Errorf("locking %s: %w", lockPath, err)
		}

		if !contended {
			contended = true
			fmt.Fprintf(os.Stderr, "waiting for acp-session.lock on %s (held by another bridge)\n", lockPath)
		}

		select {
		case <-ctx.Done():
			_ = f.Close()
			return nil, fmt.Errorf("acquiring lock on %s: %w", lockPath, ctx.Err())
		case <-ticker.C:
		}
	}
}

// Release releases the file lock and closes the file descriptor.
func (l *Lock) Release() error {
	if l.file == nil {
		return nil
	}
	unlockErr := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	closeErr := l.file.Close()
	l.file = nil
	return errors.Join(unlockErr, closeErr)
}

// ReadSession reads and verifies the acp-session.json file from the agentFolder.
// If the file does not exist, it returns nil, nil.
// If the session's recorded agent_folder does not match agentFolder, it returns ErrOwnershipMismatch.
func ReadSession(agentFolder string) (*SessionData, error) {
	sessionPath := filepath.Join(agentFolder, SessionFileName)
	data, err := os.ReadFile(sessionPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading session file %s: %w", sessionPath, err)
	}

	var session SessionData
	if err := json.Unmarshal(data, &session); err != nil {
		return nil, fmt.Errorf("unmarshaling session file %s: %w", sessionPath, err)
	}

	// Verify session ownership assertion
	if session.AgentFolder != "" && session.AgentFolder != agentFolder {
		return nil, fmt.Errorf("%w: session recorded agent_folder %q, current is %q",
			ErrOwnershipMismatch, session.AgentFolder, agentFolder)
	}

	return &session, nil
}

// WriteSession atomically writes a session file in the agentFolder.
func WriteSession(agentFolder, sessionID string) (*SessionData, error) {
	if err := os.MkdirAll(agentFolder, 0755); err != nil {
		return nil, fmt.Errorf("creating agent folder: %w", err)
	}

	session := &SessionData{
		SessionID:   sessionID,
		AgentFolder: agentFolder,
		CreatedAt:   time.Now().UTC(),
	}

	data, err := json.MarshalIndent(session, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshaling session data: %w", err)
	}

	sessionPath := filepath.Join(agentFolder, SessionFileName)
	tmpPath := fmt.Sprintf("%s.tmp.%d", sessionPath, os.Getpid())

	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return nil, fmt.Errorf("creating temp session file: %w", err)
	}

	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("writing temp session file: %w", err)
	}

	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("syncing temp session file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("closing temp session file: %w", err)
	}

	if err := os.Rename(tmpPath, sessionPath); err != nil {
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("renaming temp session file to %s: %w", sessionPath, err)
	}

	return session, nil
}
