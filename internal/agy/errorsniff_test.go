package agy

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func writeLog(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing %s: %v", name, err)
	}
	return path
}

const glogPrefix = "E0917 08:34:23.910604    84 log.go:398] "

func TestExtractErrorMessage(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
		wantOK  bool
	}{
		{
			name:    "executor anchor",
			content: glogPrefix + "agent executor error: quota exceeded\n",
			want:    "agent executor error: quota exceeded",
			wantOK:  true,
		},
		{
			name:    "bare resource exhausted",
			content: glogPrefix + "call failed: RESOURCE_EXHAUSTED: Quota\n",
			want:    "RESOURCE_EXHAUSTED: Quota",
			wantOK:  true,
		},
		{
			name:    "anchor priority prefers executor",
			content: "model unreachable: a\nagent executor error: b RESOURCE_EXHAUSTED\n",
			want:    "agent executor error: b RESOURCE_EXHAUSTED",
			wantOK:  true,
		},
		{
			name:    "last matching line wins",
			content: "agent executor error: first attempt\nagent executor error: terminal failure\n",
			want:    "agent executor error: terminal failure",
			wantOK:  true,
		},
		{
			name:    "self-wrapped suffix is stripped",
			content: "model unreachable: rpc error.: model unreachable: rpc error.\n",
			want:    "model unreachable: rpc error.",
			wantOK:  true,
		},
		{
			name:    "no anchor",
			content: "I0917 08:34:23.9 log.go:1] all good\n",
			wantOK:  false,
		},
		{name: "empty", content: "", wantOK: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := extractErrorMessage(tc.content)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (got %q)", ok, tc.wantOK, got)
			}
			if ok && got != tc.want {
				t.Fatalf("message = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestExtractErrorMessageCapsAtRuneBoundary(t *testing.T) {
	// A multi-byte rune straddling the 500-byte cap must not be split in half.
	padding := strings.Repeat("a", 499) + "ééé" + " tail"
	got, ok := extractErrorMessage("agent executor error: " + padding)
	if !ok {
		t.Fatal("anchor not found")
	}
	if len(got) > maxErrorByteLen {
		t.Fatalf("message is %d bytes, want at most %d", len(got), maxErrorByteLen)
	}
	if !utf8.ValidString(got) {
		t.Fatalf("message was cut mid-rune: %q", got[len(got)-8:])
	}
}

func TestDetectSwallowedErrorFindsGrownLog(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, "cli-old.log", glogPrefix+"agent executor error: ancient\n")

	pre, err := snapshotLogs(dir)
	if err != nil {
		t.Fatalf("snapshotLogs: %v", err)
	}

	writeLog(t, dir, "cli-new.log", glogPrefix+"agent executor error: 429 quota\n")
	msg, ok := detectSwallowedError(dir, pre, time.Now().Add(-time.Second))
	if !ok {
		t.Fatal("swallowed error not detected in the new log")
	}
	if !strings.Contains(msg, "429 quota") {
		t.Fatalf("message = %q", msg)
	}
}

func TestDetectSwallowedErrorSkipsUnchangedLog(t *testing.T) {
	dir := t.TempDir()
	content := glogPrefix + "agent executor error: from a previous turn\n"
	writeLog(t, dir, "cli-a.log", content)

	pre, err := snapshotLogs(dir)
	if err != nil {
		t.Fatalf("snapshotLogs: %v", err)
	}
	if _, ok := detectSwallowedError(dir, pre, time.Now().Add(-time.Second)); ok {
		t.Fatal("an unchanged log was reported")
	}
}

func TestDetectSwallowedErrorSkipsStaleLog(t *testing.T) {
	dir := t.TempDir()
	path := writeLog(t, dir, "cli-stale.log", glogPrefix+"agent executor error: yesterday\n")
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	// Spawned after the log was last touched, so this log belongs to another turn.
	if _, ok := detectSwallowedError(dir, logSnapshot{}, time.Now()); ok {
		t.Fatal("a log older than the turn was reported")
	}
}

func TestDetectSwallowedErrorIgnoresForeignFiles(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, "helper.log", glogPrefix+"agent executor error: not agy\n")
	writeLog(t, dir, "cli-notlog.txt", glogPrefix+"agent executor error: not agy\n")

	if _, ok := detectSwallowedError(dir, logSnapshot{}, time.Now().Add(-time.Second)); ok {
		t.Fatal("a file outside agy's log naming scheme was scanned")
	}
}

func TestDetectSwallowedErrorScansOnlyAppendedBytes(t *testing.T) {
	dir := t.TempDir()
	first := glogPrefix + "agent executor error: earlier turn\n"
	path := writeLog(t, dir, "cli-grow.log", first)

	pre, err := snapshotLogs(dir)
	if err != nil {
		t.Fatalf("snapshotLogs: %v", err)
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("reopening log: %v", err)
	}
	if _, err := f.WriteString("I0917 08:34:24 log.go:1] turn completed cleanly\n"); err != nil {
		t.Fatalf("appending: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("closing log: %v", err)
	}

	if _, ok := detectSwallowedError(dir, pre, time.Now().Add(-time.Second)); ok {
		t.Fatal("an anchor from before this turn was reported again")
	}
}

func TestReadLogTailBoundsScanWindow(t *testing.T) {
	dir := t.TempDir()
	headAnchor := glogPrefix + "agent executor error: far behind\n"
	tail := strings.Repeat("x", maxLogScanBytes+1024) + "\n"
	path := writeLog(t, dir, "cli-huge.log", headAnchor+tail)

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	window, err := readLogTail(path, 0, info.Size())
	if err != nil {
		t.Fatalf("readLogTail: %v", err)
	}
	if len(window) > maxLogScanBytes {
		t.Fatalf("window is %d bytes, want at most %d", len(window), maxLogScanBytes)
	}
	if strings.Contains(window, "far behind") {
		t.Fatal("scan window included bytes older than the cap")
	}

	// The same cap must still find an anchor that is inside the scan window.
	dir2 := t.TempDir()
	writeLog(t, dir2, "cli-huge2.log", strings.Repeat("x", maxLogScanBytes+1024)+"\n"+glogPrefix+"agent executor error: recent\n")
	if _, ok := detectSwallowedError(dir2, logSnapshot{}, time.Now().Add(-time.Second)); !ok {
		t.Fatal("anchor inside the scan window was missed")
	}
}

func TestSnapshotLogsMissingDir(t *testing.T) {
	pre, err := snapshotLogs(filepath.Join(t.TempDir(), "absent"))
	if err != nil {
		t.Fatalf("snapshotLogs on a missing dir: %v", err)
	}
	if len(pre) != 0 {
		t.Fatalf("snapshot = %v, want empty", pre)
	}
}
