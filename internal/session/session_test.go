package session

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSessionReadWrite(t *testing.T) {
	dir := t.TempDir()

	// Initial read should return nil
	s, err := ReadSession(dir)
	if err != nil {
		t.Fatalf("expected nil error on absent session, got: %v", err)
	}
	if s != nil {
		t.Fatalf("expected nil session, got: %+v", s)
	}

	// Write session
	written, err := WriteSession(dir, "test-session-123")
	if err != nil {
		t.Fatalf("WriteSession failed: %v", err)
	}
	if written.SessionID != "test-session-123" {
		t.Errorf("expected sessionId test-session-123, got: %s", written.SessionID)
	}
	if written.AgentFolder != dir {
		t.Errorf("expected agent_folder %s, got: %s", dir, written.AgentFolder)
	}

	// Read back
	read, err := ReadSession(dir)
	if err != nil {
		t.Fatalf("ReadSession failed: %v", err)
	}
	if read.SessionID != "test-session-123" {
		t.Errorf("expected sessionId test-session-123, got: %s", read.SessionID)
	}
}

func TestSessionOwnershipMismatch(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	// Write session in dir1
	_, err := WriteSession(dir1, "session-abc")
	if err != nil {
		t.Fatalf("WriteSession failed: %v", err)
	}

	// Copy session file from dir1 to dir2
	content, err := os.ReadFile(filepath.Join(dir1, SessionFileName))
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir2, SessionFileName), content, 0600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// ReadSession from dir2 should detect that agent_folder in the file is dir1 != dir2
	_, err = ReadSession(dir2)
	if err == nil {
		t.Fatalf("expected ErrOwnershipMismatch, got nil")
	}
	if !errors.Is(err, ErrOwnershipMismatch) {
		t.Fatalf("expected ErrOwnershipMismatch, got: %v", err)
	}
}

func TestLockDiscipline(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	lock1, err := AcquireLock(ctx, dir)
	if err != nil {
		t.Fatalf("AcquireLock 1 failed: %v", err)
	}

	// Second lock attempt with timeout should fail
	cancelCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	_, err = AcquireLock(cancelCtx, dir)
	if err == nil {
		t.Fatalf("expected second lock attempt to fail due to contention")
	}

	// Release first lock
	if err := lock1.Release(); err != nil {
		t.Fatalf("Release failed: %v", err)
	}

	// Now second lock attempt should succeed
	lock2, err := AcquireLock(ctx, dir)
	if err != nil {
		t.Fatalf("AcquireLock 2 failed: %v", err)
	}
	_ = lock2.Release()
}
