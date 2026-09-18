// Command agystubbin stands in for the Google Antigravity CLI (agy) so the
// bridge's real process handling can be tested: argv construction, stderr tail
// capture, exit-status classification, process-group cancellation, and the
// conversation database the bridge polls.
//
// Behaviour is selected with STUB_SCENARIO. Every prompt invocation appends its
// own argv to STUB_ARGV_LOG so a test can assert what the bridge actually passed.
package main

import (
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

const createStepsTable = `CREATE TABLE IF NOT EXISTS steps (
	idx INTEGER PRIMARY KEY,
	step_type INTEGER,
	status INTEGER,
	has_subtrajectory INTEGER,
	metadata BLOB,
	error_details BLOB,
	permissions BLOB,
	task_details BLOB,
	render_info BLOB,
	step_payload BLOB,
	step_format TEXT
)`

func main() {
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "models" {
		listModels()
		return
	}

	if logPath := os.Getenv("STUB_ARGV_LOG"); logPath != "" {
		f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			fmt.Fprintf(os.Stderr, "agystubbin: opening argv log: %v\n", err)
			os.Exit(70)
		}
		quoted := make([]string, len(args))
		for i, arg := range args {
			quoted[i] = strconv.Quote(arg)
		}
		if _, err := fmt.Fprintln(f, strings.Join(quoted, " ")); err != nil {
			fmt.Fprintf(os.Stderr, "agystubbin: writing argv log: %v\n", err)
		}
		if err := f.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "agystubbin: closing argv log: %v\n", err)
		}
	}

	switch os.Getenv("STUB_SCENARIO") {
	case "stream":
		scenarioStream(args)
	case "sleep":
		scenarioSleep()
	case "fail":
		scenarioFail()
	case "swallow":
		scenarioSwallow()
	default:
		os.Exit(0)
	}
}

// listModels mimics `agy models`: one "id<TAB>display name" line per model, plus
// the progress line agy writes to stderr. The bridge must parse stdout and ignore
// that noise entirely.
func listModels() {
	fmt.Fprintln(os.Stderr, "Fetching available models...")
	if os.Getenv("STUB_MODELS_FAIL") == "1" {
		fmt.Fprintln(os.Stderr, "agystubbin: models listing unavailable")
		os.Exit(1)
	}
	for _, name := range strings.Split(os.Getenv("STUB_MODELS"), "\n") {
		if trimmed := strings.TrimSpace(name); trimmed != "" {
			fmt.Println(trimmed)
		}
	}
}

func scenarioStream(args []string) {
	dir := os.Getenv("STUB_CONVERSATIONS_DIR")
	if dir == "" {
		fmt.Fprintln(os.Stderr, "agystubbin: STUB_CONVERSATIONS_DIR is required")
		os.Exit(66)
	}

	// A turn that continues an existing conversation appends to that file; a first
	// turn creates one, which is how the bridge discovers it.
	name := flagValue(args, "--conversation")
	if name == "" {
		name = "stub-conversation"
	}
	path := filepath.Join(dir, name+".db")

	db, err := openDB(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "agystubbin: %v\n", err)
		os.Exit(66)
	}
	defer func() { _ = db.Close() }()

	var nextIdx int64
	if err := db.QueryRow("SELECT COALESCE(MAX(idx), 0) FROM steps").Scan(&nextIdx); err != nil {
		fmt.Fprintf(os.Stderr, "agystubbin: reading steps: %v\n", err)
		os.Exit(66)
	}

	nextIdx++
	if _, err := db.Exec("INSERT INTO steps (idx, step_type, step_payload) VALUES (?, 15, ?)", nextIdx, textPayload("Hello")); err != nil {
		fmt.Fprintf(os.Stderr, "agystubbin: inserting text: %v\n", err)
		os.Exit(66)
	}
	time.Sleep(40 * time.Millisecond)

	if _, err := db.Exec("UPDATE steps SET step_payload = ? WHERE idx = ?", textPayload("Hello world"), nextIdx); err != nil {
		fmt.Fprintf(os.Stderr, "agystubbin: growing text: %v\n", err)
		os.Exit(66)
	}
	nextIdx++
	if _, err := db.Exec("INSERT INTO steps (idx, step_type, step_payload) VALUES (?, 5, ?)", nextIdx, toolPayload("read_file", `{"path":"/x"}`)); err != nil {
		fmt.Fprintf(os.Stderr, "agystubbin: inserting tool: %v\n", err)
		os.Exit(66)
	}
	time.Sleep(40 * time.Millisecond)

	nextIdx++
	if _, err := db.Exec("INSERT INTO steps (idx, step_type, step_payload) VALUES (?, 15, ?)", nextIdx, textPayload(" done")); err != nil {
		fmt.Fprintf(os.Stderr, "agystubbin: inserting tail: %v\n", err)
		os.Exit(66)
	}

	// agy writes progress to its own stdout; the bridge must discard it rather than
	// let it corrupt the protocol stream.
	fmt.Println("STUB-STDOUT-NOISE")
}

// scenarioSleep stands in for a long turn that must be interrupted.
func scenarioSleep() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	if path := os.Getenv("STUB_READY_FILE"); path != "" {
		if err := os.WriteFile(path, []byte("ready\n"), 0o600); err != nil {
			fmt.Fprintf(os.Stderr, "agystubbin: writing ready file: %v\n", err)
		}
	}
	select {
	case received := <-sig:
		if path := os.Getenv("STUB_KILLED_FILE"); path != "" {
			if err := os.WriteFile(path, []byte(received.String()+"\n"), 0o600); err != nil {
				fmt.Fprintf(os.Stderr, "agystubbin: writing killed file: %v\n", err)
			}
		}
		sigNum, ok := received.(syscall.Signal)
		if !ok {
			sigNum = syscall.SIGTERM
		}
		os.Exit(128 + int(sigNum))
	}
}

func scenarioFail() {
	fmt.Fprintln(os.Stderr, "agystubbin: quota exceeded, try again later")
	os.Exit(3)
}

func scenarioSwallow() {
	dir := os.Getenv("STUB_LOG_DIR")
	if dir == "" {
		fmt.Fprintln(os.Stderr, "agystubbin: STUB_LOG_DIR is required")
		os.Exit(66)
	}
	line := "E0917 08:34:23.910604    84 log.go:398] agent executor error: 429 RESOURCE_EXHAUSTED\n"
	f, err := os.OpenFile(filepath.Join(dir, "cli-stub.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		fmt.Fprintf(os.Stderr, "agystubbin: opening log: %v\n", err)
		os.Exit(66)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.WriteString(line); err != nil {
		fmt.Fprintf(os.Stderr, "agystubbin: writing log: %v\n", err)
		os.Exit(66)
	}
}

func flagValue(args []string, flag string) string {
	for i, arg := range args {
		if arg == flag && i+1 < len(args) {
			return args[i+1]
		}
		if value, found := strings.CutPrefix(arg, flag+"="); found {
			return value
		}
	}
	return ""
}

func openDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(10000)")
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", path, err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(createStepsTable); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("creating steps table in %s: %w", path, err)
	}
	return db, nil
}

// Minimal protobuf encoders for agy's step payloads, independent of the bridge's
// decoder so a bug in one cannot mask a bug in the other.

func pbString(field int, value string) []byte {
	out := pbVarint(uint64(field)<<3 | 2)
	out = append(out, pbVarint(uint64(len(value)))...)
	return append(out, value...)
}

func pbVarint(value uint64) []byte {
	var out []byte
	for value >= 0x80 {
		out = append(out, byte(value)|0x80)
		value >>= 7
	}
	return append(out, byte(value))
}

func textPayload(text string) []byte {
	inner := pbString(1, text)
	body := pbVarint(uint64(20)<<3 | 2)
	body = append(body, pbVarint(uint64(len(inner)))...)
	return append(body, inner...)
}

func toolPayload(name, argsJSON string) []byte {
	inner := pbString(2, name)
	inner = append(inner, pbString(3, argsJSON)...)

	call := pbVarint(uint64(4)<<3 | 2)
	call = append(call, pbVarint(uint64(len(inner)))...)
	call = append(call, inner...)

	wrapper := pbVarint(uint64(5)<<3 | 2)
	wrapper = append(wrapper, pbVarint(uint64(len(call)))...)
	return append(wrapper, call...)
}
