package agy

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

// agy --print can exit 0 with empty stdout and stderr while its backend has
// actually failed (a quota 429 for example), leaving the reason only in its own
// glog-formatted cli-*.log. Without this sniffing the bridge would report a
// successful but empty turn.
var errorAnchors = []string{
	"agent executor error:",
	"model unreachable:",
	"RESOURCE_EXHAUSTED",
}

const (
	logFilePrefix    = "cli-"
	logFileSuffix    = ".log"
	maxLogScanBytes  = 256 * 1024
	maxErrorByteLen  = 500
	logTimeTolerance = time.Second
)

type logSnapshot map[string]int64

// snapshotLogs records each cli-*.log size so a later scan can look only at the
// bytes appended during a turn.
func snapshotLogs(logDir string) (logSnapshot, error) {
	entries, err := os.ReadDir(logDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return logSnapshot{}, nil
		}
		return nil, fmt.Errorf("reading agy log dir %s: %w", logDir, err)
	}
	sizes := logSnapshot{}
	for _, entry := range entries {
		if !isAgyLog(entry.Name()) || entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		sizes[entry.Name()] = info.Size()
	}
	return sizes, nil
}

func isAgyLog(name string) bool {
	return strings.HasPrefix(name, logFilePrefix) && strings.HasSuffix(name, logFileSuffix)
}

type logCandidate struct {
	path   string
	mtime  time.Time
	offset int64
	size   int64
}

// detectSwallowedError scans the bytes appended to agy's cli-*.log files during
// a turn for a known backend failure signature.
//
// The scan is narrowed to logs that grew past their snapshot size and whose
// mtime is not clearly older than the turn's own child process, but the log
// directory is shared by every agy invocation, so an error written by a
// concurrent session can still be attributed to this turn. Only a log directory
// private to this bridge would close that window completely.
func detectSwallowedError(logDir string, pre logSnapshot, spawned time.Time) (string, bool) {
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return "", false
	}

	var candidates []logCandidate
	for _, entry := range entries {
		if !isAgyLog(entry.Name()) || entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		offset := pre[entry.Name()]
		if info.Size() <= offset {
			continue
		}
		if info.ModTime().Add(logTimeTolerance).Before(spawned) {
			continue
		}
		candidates = append(candidates, logCandidate{
			path:   filepath.Join(logDir, entry.Name()),
			mtime:  info.ModTime(),
			offset: offset,
			size:   info.Size(),
		})
	}

	sort.Slice(candidates, func(i, j int) bool { return candidates[i].mtime.After(candidates[j].mtime) })

	for _, candidate := range candidates {
		content, err := readLogTail(candidate.path, candidate.offset, candidate.size)
		if err != nil {
			continue
		}
		if msg, ok := extractErrorMessage(content); ok {
			return msg, true
		}
	}
	return "", false
}

// readLogTail reads the bytes after offset, capped to the newest maxLogScanBytes
// so a debug-logging burst cannot make the bridge allocate without bound.
func readLogTail(path string, offset, size int64) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("opening %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	start := offset
	if floor := size - maxLogScanBytes; floor > start {
		start = floor
	}
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return "", fmt.Errorf("seeking %s: %w", path, err)
	}

	buf := make([]byte, maxLogScanBytes)
	n, err := f.Read(buf)
	if n > 0 {
		return string(buf[:n]), nil
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("reading %s: %w", path, err)
	}
	return "", nil
}

// extractErrorMessage returns the most specific terminal error line found in a
// log excerpt, de-duplicating glog's self-wrapped "msg: msg" suffix.
func extractErrorMessage(content string) (string, bool) {
	for _, anchor := range errorAnchors {
		var line string
		for _, candidate := range strings.Split(content, "\n") {
			if strings.Contains(candidate, anchor) {
				// Keep the last match: retries log the same anchor repeatedly and the
				// terminal failure is the final one.
				line = candidate
			}
		}
		if line == "" {
			continue
		}
		msg := line[strings.Index(line, anchor):]
		if head, _, found := strings.Cut(msg, ".: "); found {
			msg = head + "."
		}
		return truncateBytes(msg, maxErrorByteLen), true
	}
	return "", false
}

// truncateBytes cuts s to at most max bytes without splitting a rune.
func truncateBytes(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}
