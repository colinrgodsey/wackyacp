package agy

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	_ "modernc.org/sqlite"
)

// ErrNoStepsTable signals that a conversation database lacks the steps table.
// agy creates that table shortly after the file itself, so during a turn it means
// "not ready yet"; callers report it as a schema change only when it persists.
var ErrNoStepsTable = errors.New("agy conversation has no steps table")

// ErrNoNewConversation means agy has not created its conversation file yet. A
// child creates that file shortly after it starts, so polling before then is
// normal and is not worth a diagnostic.
var ErrNoNewConversation = errors.New("no new agy conversation yet")

// ErrAmbiguousConversation means agy created more than one
// during a turn, so the new conversation cannot be identified by directory diff.
var ErrAmbiguousConversation = errors.New("cannot uniquely identify new agy conversation")

// Step is one row of agy's steps table.
type Step struct {
	Idx      int64
	StepType int64
	Payload  []byte
}

// Transcript reads agy's per-conversation SQLite databases out of a
// conversations directory. Every read is read-only and short-lived so that a
// concurrently writing agy process is never blocked by the bridge.
type Transcript struct {
	dir string
}

// NewTranscript returns a Transcript backed by dir.
func NewTranscript(dir string) *Transcript { return &Transcript{dir: dir} }

// Dir reports the conversations directory this Transcript reads.
func (t *Transcript) Dir() string { return t.dir }

// ConversationPath returns the database file for a conversation id.
func (t *Transcript) ConversationPath(conversationID string) (string, error) {
	if err := validateConversationID(conversationID); err != nil {
		return "", err
	}
	return filepath.Join(t.dir, conversationID+".db"), nil
}

// validateConversationID rejects ids that could escape the conversations
// directory; they are only ever derived from file stems, so anything containing
// a separator or a dot prefix is corrupt input.
func validateConversationID(id string) error {
	if id == "" {
		return errors.New("empty conversation id")
	}
	if strings.ContainsRune(id, os.PathSeparator) || strings.Contains(id, "/") {
		return fmt.Errorf("conversation id %q contains a path separator", id)
	}
	if strings.HasPrefix(id, ".") {
		return fmt.Errorf("conversation id %q is hidden or relative", id)
	}
	return nil
}

// Snapshot records the conversation ids present right now, so a later diff can
// tell which ones a turn created.
func (t *Transcript) Snapshot() (map[string]bool, error) {
	entries, err := os.ReadDir(t.dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]bool{}, nil
		}
		return nil, fmt.Errorf("reading conversations dir %s: %w", t.dir, err)
	}
	ids := make(map[string]bool, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".db" {
			continue
		}
		ids[strings.TrimSuffix(entry.Name(), ".db")] = true
	}
	return ids, nil
}

// OnlyNewSince returns the single conversation id present now but absent from
// before. It reports ErrNoNewConversation when nothing appeared yet and
// ErrAmbiguousConversation when several did,
// because binding the wrong conversation would stream another session's output.
func (t *Transcript) OnlyNewSince(before map[string]bool) (string, error) {
	now, err := t.Snapshot()
	if err != nil {
		return "", err
	}
	var created []string
	for id := range now {
		if !before[id] {
			created = append(created, id)
		}
	}
	switch len(created) {
	case 1:
		return created[0], nil
	case 0:
		return "", fmt.Errorf("%w: %s", ErrNoNewConversation, t.dir)
	default:
		return "", fmt.Errorf("%w: %d new conversations in %s", ErrAmbiguousConversation, len(created), t.dir)
	}
}

// ConversationExists reports whether the conversation database is on disk.
func (t *Transcript) ConversationExists(conversationID string) bool {
	path, err := t.ConversationPath(conversationID)
	if err != nil {
		return false
	}
	_, err = os.Stat(path)
	return err == nil
}

// stepsQuery selects text and tool rows above a step index. It is built once
// because the tool step types are a fixed set.
var stepsQuery = buildStepsQuery()

func buildStepsQuery() string {
	types := make([]int, 0, len(toolStepTypes))
	for t := range toolStepTypes {
		types = append(types, int(t))
	}
	sort.Ints(types)
	placeholders := make([]string, len(types))
	for i, t := range types {
		placeholders[i] = strconv.Itoa(t)
	}
	return fmt.Sprintf(
		"SELECT idx, step_type, step_payload FROM steps WHERE idx > ? AND (step_type = %d OR step_type IN (%s)) ORDER BY idx",
		stepTypeText, strings.Join(placeholders, ","),
	)
}

// Steps returns the text and tool rows of a conversation with idx greater than
// afterIdx, ordered by idx. A database without the steps table yields
// ErrNoStepsTable; a busy database yields an error the caller can ignore, since
// the row set is re-read on the next poll.
func (t *Transcript) Steps(ctx context.Context, conversationID string, afterIdx int64) ([]Step, error) {
	path, err := t.ConversationPath(conversationID)
	if err != nil {
		return nil, err
	}
	// The busy timeout belongs in the DSN: a locked database has to be waited on
	// from the first statement of the connection, because agy commits steps while
	// the bridge is reading them.
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro&_pragma=busy_timeout(250)")
	if err != nil {
		return nil, fmt.Errorf("opening conversation db %s: %w", path, err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("opening conversation db %s: %w", path, err)
	}

	var table string
	err = db.QueryRowContext(ctx,
		"SELECT name FROM sqlite_master WHERE type='table' AND name='steps' LIMIT 1").Scan(&table)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrNoStepsTable, path)
	}
	if err != nil {
		return nil, fmt.Errorf("inspecting %s: %w", path, err)
	}

	rows, err := db.QueryContext(ctx, stepsQuery, afterIdx)
	if err != nil {
		return nil, fmt.Errorf("querying steps in %s: %w", path, err)
	}
	defer func() { _ = rows.Close() }()

	var steps []Step
	for rows.Next() {
		var (
			s        Step
			payload  []byte
			stepType sql.NullInt64
		)
		if err := rows.Scan(&s.Idx, &stepType, &payload); err != nil {
			return nil, fmt.Errorf("scanning step row in %s: %w", path, err)
		}
		s.StepType = stepType.Int64
		s.Payload = payload
		steps = append(steps, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading steps in %s: %w", path, err)
	}
	return steps, nil
}
