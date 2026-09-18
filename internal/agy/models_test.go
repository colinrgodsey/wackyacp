package agy

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type fetchStub struct {
	calls int
	got   []string
	err   error
}

func (f *fetchStub) fetch(context.Context, string) ([]string, error) {
	f.calls++
	return f.got, f.err
}

func newTestProvider(t *testing.T, fetch func(context.Context, string) ([]string, error)) (*ModelProvider, string) {
	t.Helper()
	stateDir := t.TempDir()
	provider := NewModelProvider("/nonexistent/agy", stateDir, func(string, ...any) {})
	if fetch != nil {
		provider.fetch = fetch
	}
	return provider, stateDir
}

func TestModelProviderListsAndCaches(t *testing.T) {
	stub := &fetchStub{got: []string{"gemini-3-8-flash-high\tGemini 3.8 Flash (High)", "gemini-3-1-pro-low\tGemini 3.1 Pro (Low)"}}
	provider, stateDir := newTestProvider(t, stub.fetch)

	models := provider.Models(context.Background())
	if len(models) != 2 || models[0] != "gemini-3-8-flash-high\tGemini 3.8 Flash (High)" {
		t.Fatalf("models = %v", models)
	}
	if stub.calls != 1 {
		t.Fatalf("fetch called %d times, want 1", stub.calls)
	}

	cached, err := os.ReadFile(filepath.Join(stateDir, "models_cache.json"))
	if err != nil {
		t.Fatalf("cache not written: %v", err)
	}
	if !json.Valid(cached) {
		t.Fatalf("cache is not JSON: %s", cached)
	}

	// Memoized for the process lifetime.
	provider.Models(context.Background())
	if stub.calls != 1 {
		t.Fatalf("fetch called %d times after memoization, want 1", stub.calls)
	}
}

func TestModelProviderFallsBackToCache(t *testing.T) {
	provider, stateDir := newTestProvider(t, func(context.Context, string) ([]string, error) {
		return nil, errors.New("agy: command not found")
	})
	cache, err := json.Marshal([]string{"Cached Model"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, "models_cache.json"), cache, 0o600); err != nil {
		t.Fatal(err)
	}

	models := provider.Models(context.Background())
	if len(models) != 1 || models[0] != "Cached Model" {
		t.Fatalf("models = %v, want the cached list", models)
	}
}

func TestModelProviderWithoutCacheAdvertisesNothing(t *testing.T) {
	provider, _ := newTestProvider(t, func(context.Context, string) ([]string, error) {
		return nil, errors.New("agy models exited 1")
	})

	if models := provider.Models(context.Background()); models != nil {
		t.Fatalf("models = %v, want none rather than invented names", models)
	}
	options := provider.ConfigOptions(context.Background(), "")
	if string(options) != "[]" {
		t.Fatalf("configOptions = %s, want []", options)
	}
}

func TestModelProviderEmptyListingUsesCache(t *testing.T) {
	provider, stateDir := newTestProvider(t, func(context.Context, string) ([]string, error) {
		return []string{}, nil
	})
	cache, err := json.Marshal([]string{"Cached Model"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, "models_cache.json"), cache, 0o600); err != nil {
		t.Fatal(err)
	}
	if models := provider.Models(context.Background()); len(models) != 1 {
		t.Fatalf("models = %v, want the cached list", models)
	}
}

func TestModelProviderConfigOptionsShape(t *testing.T) {
	stub := &fetchStub{got: []string{"A", "B"}}
	provider, _ := newTestProvider(t, stub.fetch)

	raw := provider.ConfigOptions(context.Background(), "B")
	var options []struct {
		ID           string `json:"id"`
		Name         string `json:"name"`
		Category     string `json:"category"`
		Type         string `json:"type"`
		CurrentValue string `json:"currentValue"`
		Options      []struct {
			Value string `json:"value"`
			Name  string `json:"name"`
		} `json:"options"`
	}
	if err := json.Unmarshal(raw, &options); err != nil {
		t.Fatalf("configOptions is not valid JSON (%s): %v", raw, err)
	}
	if len(options) != 1 {
		t.Fatalf("configOptions = %s, want exactly the model selector", raw)
	}
	opt := options[0]
	if opt.ID != "model" || opt.Name != "Model" || opt.Category != "model" || opt.Type != "select" {
		t.Fatalf("model selector = %+v", opt)
	}
	if opt.CurrentValue != "B" {
		t.Fatalf("currentValue = %q, want B", opt.CurrentValue)
	}
	if len(opt.Options) != 2 || opt.Options[1].Value != "B" || opt.Options[1].Name != "B" {
		t.Fatalf("options = %+v", opt.Options)
	}
}

func TestModelProviderDefaultsCurrentValue(t *testing.T) {
	stub := &fetchStub{got: []string{"A", "B"}}
	provider, _ := newTestProvider(t, stub.fetch)

	raw := provider.ConfigOptions(context.Background(), "")
	if !json.Valid(raw) {
		t.Fatalf("invalid JSON: %s", raw)
	}
	var options []map[string]any
	if err := json.Unmarshal(raw, &options); err != nil {
		t.Fatal(err)
	}
	if got := options[0]["currentValue"]; got != "A" {
		t.Fatalf("currentValue = %v, want the first model", got)
	}
}

// realModelEntries mirrors the shape of `agy models` output: a machine id, a tab,
// and a display name. agy rejects the display name after --model.
var realModelEntries = []string{
	"gemini-3-1-pro-high\tGemini 3.1 Pro (High)",
	"claude-sonnet-4-6\tClaude Sonnet 4.6 (Thinking)",
}

func TestSplitModelEntrySeparatesIDAndDisplay(t *testing.T) {
	tests := []struct {
		name     string
		entry    string
		wantID   string
		wantName string
	}{
		{"tabbed", "gemini-3-1-pro-high\tGemini 3.1 Pro (High)", "gemini-3-1-pro-high", "Gemini 3.1 Pro (High)"},
		{"padded", "  id-a \t  Display A  ", "id-a", "Display A"},
		{"bare id is both", "custom-model", "custom-model", "custom-model"},
		{"extra tabs stay in the name", "id-b\tTwo\tTabs", "id-b", "Two\tTabs"},
		{"empty", "", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id, name := splitModelEntry(tc.entry)
			if id != tc.wantID || name != tc.wantName {
				t.Fatalf("splitModelEntry(%q) = (%q, %q), want (%q, %q)", tc.entry, id, name, tc.wantID, tc.wantName)
			}
		})
	}
}

// TestCanonicalIDTurnsAnythingIntoAMachineID covers every value that can come back
// from a client or an older session: a full entry, an id, a display name, and an
// unknown id that must survive untouched.
func TestCanonicalIDTurnsAnythingIntoAMachineID(t *testing.T) {
	provider, _ := newTestProvider(t, (&fetchStub{got: realModelEntries}).fetch)

	tests := []struct{ in, want string }{
		{"gemini-3-1-pro-high\tGemini 3.1 Pro (High)", "gemini-3-1-pro-high"},
		{"gemini-3-1-pro-high", "gemini-3-1-pro-high"},
		{"Claude Sonnet 4.6 (Thinking)", "claude-sonnet-4-6"},
		{"a-model-added-tomorrow", "a-model-added-tomorrow"},
		{"", ""},
	}
	for _, tc := range tests {
		if got := provider.CanonicalID(tc.in); got != tc.want {
			t.Fatalf("CanonicalID(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestConfigOptionsAdvertisesMachineIDs(t *testing.T) {
	provider, _ := newTestProvider(t, (&fetchStub{got: realModelEntries}).fetch)

	type option struct {
		Value string `json:"value"`
		Name  string `json:"name"`
	}
	var rendered []struct {
		ID           string   `json:"id"`
		Type         string   `json:"type"`
		CurrentValue string   `json:"currentValue"`
		Options      []option `json:"options"`
	}

	raw := provider.ConfigOptions(context.Background(), "")
	if err := json.Unmarshal(raw, &rendered); err != nil {
		t.Fatalf("configOptions = %s: %v", raw, err)
	}
	if len(rendered) != 1 || rendered[0].ID != "model" || rendered[0].Type != "select" {
		t.Fatalf("configOptions = %s", raw)
	}
	if got := rendered[0].CurrentValue; got != "gemini-3-1-pro-high" {
		t.Fatalf("currentValue = %q, want the first machine id", got)
	}

	want := []option{
		{"gemini-3-1-pro-high", "Gemini 3.1 Pro (High)"},
		{"claude-sonnet-4-6", "Claude Sonnet 4.6 (Thinking)"},
	}
	if len(rendered[0].Options) != len(want) {
		t.Fatalf("options = %v, want %v", rendered[0].Options, want)
	}
	for i, o := range rendered[0].Options {
		if o != want[i] {
			t.Fatalf("option %d = %v, want %v", i, o, want[i])
		}
	}

	// A display name handed back as the current selection renders as its id.
	raw = provider.ConfigOptions(context.Background(), "Claude Sonnet 4.6 (Thinking)")
	if err := json.Unmarshal(raw, &rendered); err != nil {
		t.Fatalf("configOptions = %s: %v", raw, err)
	}
	if got := rendered[0].CurrentValue; got != "claude-sonnet-4-6" {
		t.Fatalf("currentValue = %q, want the resolved id", got)
	}
}

// TestConfigOptionsAreEmptyWhenNothingIsKnown keeps the promise that the bridge
// never advertises model names it invented.
func TestConfigOptionsAreEmptyWhenNothingIsKnown(t *testing.T) {
	provider, _ := newTestProvider(t, (&fetchStub{err: errors.New("agy models unavailable")}).fetch)

	if raw := provider.ConfigOptions(context.Background(), ""); string(raw) != "[]" {
		t.Fatalf("configOptions = %s, want []", raw)
	}
	if got := provider.CanonicalID("Some Display Name"); got != "Some Display Name" {
		t.Fatalf("CanonicalID = %q, want it passed through when no list exists", got)
	}
}
