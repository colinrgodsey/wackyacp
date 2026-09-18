package agy

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestTranscriptStepsFiltersOrdersAndPages(t *testing.T) {
	dir := t.TempDir()
	newConversationDB(t, dir, "conv1", []row{
		{idx: 1, stepType: stepTypeText, payload: textPayload("one")},
		{idx: 2, stepType: 99, payload: textPayload("ignored type")},
		{idx: 3, stepType: 5, payload: toolPayload("read_file", `{"path":"/x"}`)},
		{idx: 4, stepType: stepTypeText, payload: textPayload("four")},
	})
	tr := NewTranscript(dir)

	steps, err := tr.Steps(context.Background(), "conv1", 0)
	if err != nil {
		t.Fatalf("Steps: %v", err)
	}
	if len(steps) != 3 {
		t.Fatalf("got %d steps, want 3 (non-text/tool rows must be filtered): %+v", len(steps), steps)
	}
	if steps[0].Idx != 1 || steps[1].Idx != 3 || steps[2].Idx != 4 {
		t.Fatalf("unexpected idx sequence: %d %d %d", steps[0].Idx, steps[1].Idx, steps[2].Idx)
	}

	steps, err = tr.Steps(context.Background(), "conv1", 3)
	if err != nil {
		t.Fatalf("Steps after 3: %v", err)
	}
	if len(steps) != 1 || steps[0].Idx != 4 {
		t.Fatalf("afterIdx paging broken: %+v", steps)
	}
}

func TestTranscriptMissingStepsTable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "drifted.db")
	writeEmptyDB(t, path)

	tr := NewTranscript(dir)
	_, err := tr.Steps(context.Background(), "drifted", -1)
	if !errors.Is(err, ErrNoStepsTable) {
		t.Fatalf("err = %v, want ErrNoStepsTable", err)
	}
}

func TestTranscriptMissingConversationDoesNotCreateFile(t *testing.T) {
	dir := t.TempDir()
	tr := NewTranscript(dir)

	if _, err := tr.Steps(context.Background(), "absent", -1); err == nil {
		t.Fatal("Steps on a missing conversation succeeded")
	}
	if _, err := os.Stat(filepath.Join(dir, "absent.db")); !os.IsNotExist(err) {
		t.Fatal("a read-only query created the conversation database")
	}
}

func TestTranscriptSnapshotAndBind(t *testing.T) {
	dir := t.TempDir()
	tr := NewTranscript(dir)

	before, err := tr.Snapshot()
	if err != nil {
		t.Fatalf("empty Snapshot: %v", err)
	}
	if len(before) != 0 {
		t.Fatalf("snapshot of empty dir = %v", before)
	}

	if _, err := tr.OnlyNewSince(before); !errors.Is(err, ErrNoNewConversation) {
		t.Fatalf("err = %v, want ErrNoNewConversation when nothing appeared", err)
	}

	if err := os.WriteFile(filepath.Join(dir, "a.db"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	id, err := tr.OnlyNewSince(before)
	if err != nil {
		t.Fatalf("OnlyNewSince: %v", err)
	}
	if id != "a" {
		t.Fatalf("bound %q, want a (only .db files count)", id)
	}

	if err := os.WriteFile(filepath.Join(dir, "b.db"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.OnlyNewSince(before); !errors.Is(err, ErrAmbiguousConversation) {
		t.Fatalf("err = %v, want ErrAmbiguousConversation when two appeared", err)
	}
}

func TestTranscriptSnapshotMissingDirIsNotFatal(t *testing.T) {
	tr := NewTranscript(filepath.Join(t.TempDir(), "does-not-exist"))
	snapshot, err := tr.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot of missing dir: %v", err)
	}
	if len(snapshot) != 0 {
		t.Fatalf("snapshot = %v, want empty", snapshot)
	}
}

func TestTranscriptConversationPathRejectsTraversal(t *testing.T) {
	tr := NewTranscript(t.TempDir())
	for _, bad := range []string{"", "../evil", "a/b", ".hidden", "/abs"} {
		if _, err := tr.ConversationPath(bad); err == nil {
			t.Fatalf("ConversationPath(%q) accepted a dangerous id", bad)
		}
	}
	path, err := tr.ConversationPath("conv-1")
	if err != nil || filepath.Base(path) != "conv-1.db" {
		t.Fatalf("ConversationPath = %q, %v", path, err)
	}
}

func TestTranscriptConversationExists(t *testing.T) {
	dir := t.TempDir()
	tr := NewTranscript(dir)
	if tr.ConversationExists("x") {
		t.Fatal("conversation exists before it was written")
	}
	newConversationDB(t, dir, "x", nil)
	if !tr.ConversationExists("x") {
		t.Fatal("conversation missing after it was written")
	}
}
