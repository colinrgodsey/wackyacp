package agy

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// Minimal protobuf encoders for the subset of agy's step payloads the bridge
// reads. They are deliberately independent of the decoder under test.

// writableDSN gives test writers the same patience the bridge's own reads have,
// so a poller holding a read lock does not fail fixture writes with SQLITE_BUSY.
func writableDSN(path string) string {
	return "file:" + path + "?_pragma=busy_timeout(10000)"
}

func pbVarint(value uint64) []byte {
	var out []byte
	for value >= 0x80 {
		out = append(out, byte(value)|0x80)
		value >>= 7
	}
	return append(out, byte(value))
}

func pbTag(field, wire int) []byte {
	return pbVarint(uint64(field)<<3 | uint64(wire))
}

func pbBytes(field int, payload []byte) []byte {
	out := pbTag(field, 2)
	out = append(out, pbVarint(uint64(len(payload)))...)
	return append(out, payload...)
}

func pbString(field int, value string) []byte {
	return pbBytes(field, []byte(value))
}

func pbInt(field int, value uint64) []byte {
	return append(pbTag(field, 0), pbVarint(value)...)
}

// textPayload builds a step payload carrying assistant text at field 20 -> 1.
func textPayload(text string) []byte {
	return pbBytes(fieldTextField, pbString(fieldTextBody, text))
}

// toolPayload builds a step payload carrying a tool call at field 5 -> 4.
func toolPayload(name, argsJSON string) []byte {
	inner := pbString(fieldToolName, name)
	if argsJSON != "" {
		inner = append(inner, pbString(fieldToolArgs, argsJSON)...)
	}
	return pbBytes(fieldToolWrapper, pbBytes(fieldToolCall, inner))
}

// toolPayloadAltName builds a tool payload that names the tool at field 9 only.
func toolPayloadAltName(name, argsJSON string) []byte {
	inner := pbString(fieldToolNameAlt, name)
	if argsJSON != "" {
		inner = append(inner, pbString(fieldToolArgs, argsJSON)...)
	}
	return pbBytes(fieldToolWrapper, pbBytes(fieldToolCall, inner))
}

const createStepsTable = `CREATE TABLE steps (
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

type row struct {
	idx      int64
	stepType int64
	payload  []byte
}

// newConversationDB writes an agy-shaped conversation database and returns its
// path. rows are inserted in the order given.
func newConversationDB(t *testing.T, dir, name string, rows []row) string {
	t.Helper()
	return writeConversationDB(t, filepath.Join(dir, name+".db"), rows)
}

func writeConversationDB(t *testing.T, path string, rows []row) string {
	t.Helper()
	db, err := sql.Open("sqlite", writableDSN(path))
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(createStepsTable); err != nil {
		t.Fatalf("creating steps table in %s: %v", path, err)
	}
	for _, r := range rows {
		if _, err := db.Exec(
			"INSERT INTO steps (idx, step_type, step_payload) VALUES (?, ?, ?)",
			r.idx, r.stepType, r.payload,
		); err != nil {
			t.Fatalf("inserting step %d into %s: %v", r.idx, path, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("closing %s: %v", path, err)
	}
	return path
}

// writeEmptyDB creates a database with an unrelated table, standing in for an agy
// schema that no longer matches what the bridge reads.
func writeEmptyDB(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", writableDSN(path))
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("CREATE TABLE other (idx INTEGER)"); err != nil {
		t.Fatalf("creating decoy table in %s: %v", path, err)
	}
}

// updateStepPayload replaces a step payload in place, which is how agy grows a
// text row while the model streams.
func updateStepPayload(t *testing.T, path string, idx int64, payload []byte) {
	t.Helper()
	db, err := sql.Open("sqlite", writableDSN(path))
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	defer func() { _ = db.Close() }()
	result, err := db.Exec("UPDATE steps SET step_payload = ? WHERE idx = ?", payload, idx)
	if err != nil {
		t.Fatalf("updating step %d in %s: %v", idx, path, err)
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		t.Fatalf("updating step %d affected %d rows (%v)", idx, n, err)
	}
}

// insertStep appends a step, which is how agy advances to the next row.
func insertStep(t *testing.T, path string, r row) {
	t.Helper()
	db, err := sql.Open("sqlite", writableDSN(path))
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("INSERT INTO steps (idx, step_type, step_payload) VALUES (?, ?, ?)", r.idx, r.stepType, r.payload); err != nil {
		t.Fatalf("inserting step %d into %s: %v", r.idx, path, err)
	}
}
