package agy

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ModelProvider discovers the models agy offers and renders them as the single
// "model" config option advertised to ACP clients. Discovery is memoized for the
// process lifetime, with the last good list cached on disk.
type ModelProvider struct {
	agyBin     string
	cachePath  string
	logf       func(format string, args ...any)
	fetch      func(ctx context.Context, agyBin string) ([]string, error)
	mu         sync.Mutex
	resolved   []string
	discovered bool
}

// NewModelProvider returns a provider that shells out to agyBin to list models.
func NewModelProvider(agyBin, stateDir string, logf func(format string, args ...any)) *ModelProvider {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &ModelProvider{
		agyBin:    agyBin,
		cachePath: filepath.Join(stateDir, "models_cache.json"),
		logf:      logf,
		fetch:     fetchModels,
	}
}

// Models returns the available model entries, each "id<TAB>display name". When agy cannot be queried
// the cached list is used; when there is no cache either, the list is empty and
// clients simply see no model choice. Inventing model names would advertise
// selections agy may reject, so an empty list is preferred over a guess.
func (m *ModelProvider) Models(ctx context.Context) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.discovered {
		return m.resolved
	}
	m.discovered = true

	models, err := m.fetch(ctx, m.agyBin)
	if err != nil {
		if cached := m.loadCache(); len(cached) > 0 {
			m.logf("agy models failed (%v); using %d cached model(s)", err, len(cached))
			m.resolved = cached
			return m.resolved
		}
		m.logf("agy models failed (%v) and no cache exists; advertising no model choice", err)
		return nil
	}
	if len(models) == 0 {
		if cached := m.loadCache(); len(cached) > 0 {
			m.logf("agy models returned nothing; using %d cached model(s)", len(cached))
			m.resolved = cached
			return m.resolved
		}
		return nil
	}
	m.resolved = models
	m.saveCache(models)
	return m.resolved
}

// ConfigOptions renders the ACP configOptions array. current may be empty, in
// which case the first model is presented as selected. agy's `models` output is
// "id<TAB>display name" per line: the machine id is advertised as the option value
// because it survives renames, and the display name is the label.
func (m *ModelProvider) ConfigOptions(ctx context.Context, current string) json.RawMessage {
	models := m.Models(ctx)
	if len(models) == 0 {
		return json.RawMessage("[]")
	}
	current = m.CanonicalID(current)
	if current == "" {
		current, _ = splitModelEntry(models[0])
	}
	options := make([]map[string]string, 0, len(models))
	for _, entry := range models {
		id, name := splitModelEntry(entry)
		options = append(options, map[string]string{"value": id, "name": name})
	}
	payload, err := json.Marshal([]map[string]any{{
		"id":           "model",
		"name":         "Model",
		"category":     "model",
		"type":         "select",
		"currentValue": current,
		"options":      options,
	}})
	if err != nil {
		m.logf("encoding config options: %v", err)
		return json.RawMessage("[]")
	}
	return payload
}

// CanonicalID maps anything a client or a persisted session handed back onto the
// stable machine id from `agy models`, which is what sessions store, what ACP
// advertises as an option value, and what goes after --model. A display name is
// accepted by agy too, but it changes across releases while the id does not, so
// both forms are normalised here. The composite "id<TAB>name" line that `agy
// models` prints is accepted by neither, and an unknown value is passed through so
// a deliberately chosen custom model is never dropped.
func (m *ModelProvider) CanonicalID(value string) string {
	value = modelID(value)
	if value == "" {
		return ""
	}
	for _, entry := range m.Models(context.Background()) {
		id, name := splitModelEntry(entry)
		if value == id || value == name {
			return id
		}
	}
	return value
}

// modelID returns the machine id from an entry that may carry a display name
// ("id<TAB>name" or a bare id). Clients and older persisted sessions may hand
// back a full display string; every path that feeds --model normalizes through
// this so agy never sees the display part.
func modelID(entry string) string {
	id, _ := splitModelEntry(entry)
	return id
}

// splitModelEntry splits one `agy models` output line into its machine id and
// display name. Entries without a tab (hand-written caches, unexpected output)
// fall back to using the same string for both.
func splitModelEntry(entry string) (id, name string) {
	entry = strings.TrimSpace(entry)
	if id, name, ok := strings.Cut(entry, "	"); ok {
		return strings.TrimSpace(id), strings.TrimSpace(name)
	}
	return entry, entry
}

func (m *ModelProvider) loadCache() []string {
	data, err := os.ReadFile(m.cachePath)
	if err != nil {
		return nil
	}
	var models []string
	if err := json.Unmarshal(data, &models); err != nil {
		return nil
	}
	return models
}

func (m *ModelProvider) saveCache(models []string) {
	data, err := json.Marshal(models)
	if err != nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(m.cachePath), 0o700); err != nil {
		return
	}
	tmp := m.cachePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return
	}
	if err := os.Rename(tmp, m.cachePath); err != nil {
		m.logf("writing model cache %s: %v", m.cachePath, err)
	}
}

// modelsFetchTimeout bounds the model listing. A provider that cannot answer must
// degrade to the cached or empty list; it must not hold up the session that asked.
const modelsFetchTimeout = 15 * time.Second

// fetchModels runs `agy models` and reads one model entry per line.
func fetchModels(parent context.Context, agyBin string) ([]string, error) {
	ctx, cancel := context.WithTimeout(parent, modelsFetchTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, agyBin, "models")
	cmd.WaitDelay = 2 * time.Second
	cmd.Stdin = nil
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("running %s models: %w", agyBin, err)
	}
	var models []string
	for _, line := range strings.Split(string(output), "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			models = append(models, trimmed)
		}
	}
	return models, nil
}
