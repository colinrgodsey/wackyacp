package agy

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// StoredSession is the durable binding between an ACP session id and agy's own
// conversation. agy owns the transcript, so the bridge only needs enough state
// to resume it: which conversation, how far the bridge has read in it, and which
// model was selected.
type StoredSession struct {
	ConversationID string `json:"conversationId,omitempty"`
	LastStepIdx    int64  `json:"lastStepIdx"`
	ModelID        string `json:"modelId,omitempty"`
}

// Store persists sessions as a single JSON document in the state directory,
// guarded by an advisory lock so concurrent bridges sharing a state directory
// cannot clobber each other's entries.
type Store struct {
	stateDir string
	path     string
	lockPath string
}

// NewStore returns a Store writing sessions.json inside stateDir.
func NewStore(stateDir string) *Store {
	return &Store{
		stateDir: stateDir,
		path:     filepath.Join(stateDir, "sessions.json"),
		lockPath: filepath.Join(stateDir, "sessions.lock"),
	}
}

// Path reports the file this Store reads and writes.
func (s *Store) Path() string { return s.path }

// Read returns the persisted sessions. A missing file yields an empty map; a
// file that cannot be parsed yields an error so the caller can warn, since
// silently discarding bindings would strand live conversations.
func (s *Store) Read() (map[string]StoredSession, error) {
	unlock, err := s.lock()
	if err != nil {
		return nil, err
	}
	defer unlock()
	return s.readLocked()
}

// Write inserts or replaces one session, atomically replacing the document.
func (s *Store) Write(sessionID string, sess StoredSession) error {
	unlock, err := s.lock()
	if err != nil {
		return err
	}
	defer unlock()

	// A corrupt document is replaced rather than merged: the bindings it held are
	// already unusable, and refusing to write would block every future turn.
	sessions, err := s.readLocked()
	if err != nil {
		sessions = map[string]StoredSession{}
	}
	sessions[sessionID] = sess

	payload, err := json.MarshalIndent(sessions, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding session store: %w", err)
	}

	tmp, err := os.CreateTemp(s.stateDir, "sessions-*.tmp")
	if err != nil {
		return fmt.Errorf("creating temp session store: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("writing temp session store: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("syncing temp session store: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing temp session store: %w", err)
	}
	if err := os.Chmod(tmpName, 0o600); err != nil {
		return fmt.Errorf("chmod-ing temp session store: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("replacing session store: %w", err)
	}
	return nil
}

func (s *Store) readLocked() (map[string]StoredSession, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]StoredSession{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading session store %s: %w", s.path, err)
	}
	if len(data) == 0 {
		return map[string]StoredSession{}, nil
	}
	var sessions map[string]StoredSession
	if err := json.Unmarshal(data, &sessions); err != nil {
		return nil, fmt.Errorf("parsing session store %s: %w", s.path, err)
	}
	if sessions == nil {
		sessions = map[string]StoredSession{}
	}
	return sessions, nil
}

// lock takes the exclusive advisory lock, creating the state directory first.
// The lock is released by closing the lock file descriptor.
func (s *Store) lock() (func(), error) {
	if err := os.MkdirAll(s.stateDir, 0o700); err != nil {
		return nil, fmt.Errorf("creating state dir %s: %w", s.stateDir, err)
	}
	f, err := os.OpenFile(s.lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening session lock %s: %w", s.lockPath, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("acquiring session lock %s: %w", s.lockPath, err)
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}
