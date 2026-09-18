package agy

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestStoreRoundTrip(t *testing.T) {
	store := NewStore(t.TempDir())

	empty, err := store.Read()
	if err != nil {
		t.Fatalf("Read on empty store: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("fresh store = %v, want empty", empty)
	}

	if err := store.Write("s1", StoredSession{ConversationID: "c1", LastStepIdx: 7, ModelID: "Gemini 3.5 Flash (High)"}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := store.Write("s2", StoredSession{LastStepIdx: -1}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	sessions, err := store.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(sessions) != 2 {
		t.Fatalf("sessions = %v, want two entries", sessions)
	}
	got := sessions["s1"]
	if got.ConversationID != "c1" || got.LastStepIdx != 7 || got.ModelID != "Gemini 3.5 Flash (High)" {
		t.Fatalf("s1 = %+v, want the values written", got)
	}
	if sessions["s2"].LastStepIdx != -1 {
		t.Fatalf("s2 = %+v, want lastStepIdx -1 preserved", sessions["s2"])
	}
}

func TestStoreReplacesExistingEntry(t *testing.T) {
	store := NewStore(t.TempDir())
	if err := store.Write("s1", StoredSession{LastStepIdx: 1}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := store.Write("s1", StoredSession{ConversationID: "c9", LastStepIdx: 12}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	sessions, err := store.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(sessions) != 1 || sessions["s1"].ConversationID != "c9" || sessions["s1"].LastStepIdx != 12 {
		t.Fatalf("sessions = %v, want the replacement only", sessions)
	}
}

func TestStoreFilePermissions(t *testing.T) {
	store := NewStore(t.TempDir())
	if err := store.Write("s1", StoredSession{ConversationID: "c1"}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	info, err := os.Stat(store.Path())
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("session store mode = %o, want 600", perm)
	}
}

func TestStoreReadRejectsCorruptFile(t *testing.T) {
	store := NewStore(t.TempDir())
	if err := os.MkdirAll(filepath.Dir(store.Path()), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(store.Path(), []byte("{ not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Read(); err == nil {
		t.Fatal("a corrupt store was read as empty, silently losing every binding")
	}
}

func TestStoreWriteReplacesCorruptFile(t *testing.T) {
	store := NewStore(t.TempDir())
	if err := os.WriteFile(store.Path(), []byte("garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.Write("s1", StoredSession{ConversationID: "c1"}); err != nil {
		t.Fatalf("Write over a corrupt store: %v", err)
	}
	sessions, err := store.Read()
	if err != nil {
		t.Fatalf("Read after recovery write: %v", err)
	}
	if len(sessions) != 1 || sessions["s1"].ConversationID != "c1" {
		t.Fatalf("sessions = %v, want only the new entry", sessions)
	}
}

func TestStoreEmptyFileReadsAsEmpty(t *testing.T) {
	store := NewStore(t.TempDir())
	if err := os.WriteFile(store.Path(), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	sessions, err := store.Read()
	if err != nil {
		t.Fatalf("Read of an empty file: %v", err)
	}
	if len(sessions) != 0 {
		t.Fatalf("sessions = %v, want empty", sessions)
	}
}

func TestStoreConcurrentWrites(t *testing.T) {
	stateDir := t.TempDir()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Each writer uses its own Store, the way two bridge processes would.
			store := NewStore(stateDir)
			if err := store.Write(sessionNameFor(i), StoredSession{ConversationID: "c" + string(rune('a'+i))}); err != nil {
				t.Errorf("concurrent write %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	sessions, err := NewStore(stateDir).Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(sessions) != 8 {
		t.Fatalf("%d of 8 concurrent writes survived: %v", len(sessions), sessions)
	}
}

func sessionNameFor(i int) string {
	return string(rune('a'+i)) + "-session"
}
