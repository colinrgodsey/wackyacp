package claudesdk

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/colinrgodsey/wackypub/pkg/agent"
)

// toolResponseFrame names the frame that answers a worker tool_request. It must
// match the constant in worker/claude-sdk-worker.mjs.
const toolResponseFrame = "tool_response"

// handleToolRequest answers an in-process MCP scratchpad call from the worker.
//
// Scratchpad storage lives in Go, not in the worker: wackypub owns entry IDs,
// size caps, binary detection and the search and diff semantics, and
// re-implementing them in JavaScript would fork the truth. The worker only ever
// sees the rendered result text.
func (d *Driver) handleToolRequest(ev *workerEvent) {
	text, err := d.scratchpadTool(ev.Name, ev.Args)
	resp := workerToolResponse{Type: toolResponseFrame, RequestID: ev.RequestID, OK: err == nil, Text: text}
	if err != nil {
		resp.Error = err.Error()
	}
	d.mu.Lock()
	enc := d.enc
	d.mu.Unlock()
	if enc == nil {
		return
	}
	if err := enc.Encode(resp); err != nil {
		fmt.Fprintf(os.Stderr, "wackyacp claudesdk: writing tool response: %v\n", err)
	}
}

// scratchpadTool maps the six MCP tool names onto wackypub's own scratchpad API.
func (d *Driver) scratchpadTool(name string, args map[string]any) (string, error) {
	wsDir := filepath.Dir(d.cfg.AgentFolder)
	agentID := filepath.Base(d.cfg.AgentFolder)

	switch name {
	case "create_scratchpad":
		text := stringArg(args, "text")
		if text == "" {
			return "", fmt.Errorf("create_scratchpad needs non-empty text")
		}
		entry, err := agent.CreateScratchpad(d.cfg.AgentFolder, text, stringOr(stringArg(args, "createdBy"), "claude-sdk"))
		if err != nil {
			return "", err
		}
		return mustJSON(entry), nil

	case "get_scratchpad":
		id := stringArg(args, "id")
		if id == "" {
			return "", fmt.Errorf("get_scratchpad needs an entry id")
		}
		text, err := agent.GetScratchpad(d.cfg.AgentFolder, id, intArg(args, "skipLines"), intArg(args, "numLines"))
		if err != nil {
			return "", err
		}
		return text, nil

	case "list_scratchpads":
		items, count, capacity, err := agent.ListScratchpads(d.cfg.AgentFolder)
		if err != nil {
			return "", err
		}
		return mustJSON(map[string]any{
			"entries":   items,
			"count":     count,
			"capacity":  capacity,
			"agent_dir": d.cfg.AgentFolder,
		}), nil

	case "search_scratchpad":
		id := stringArg(args, "id")
		query := stringArg(args, "query")
		if id == "" || query == "" {
			return "", fmt.Errorf("search_scratchpad needs an entry id and a query")
		}
		maxResults := 20
		if p := intArg(args, "maxResults"); p != nil {
			maxResults = *p
		}
		result, err := agent.SearchScratchpad(d.cfg.AgentFolder, id, query, boolPtrArg(args, "caseSensitive"), boolArg(args, "regex"), maxResults)
		if err != nil {
			return "", err
		}
		return mustJSON(result), nil

	case "diff_scratchpad":
		before := stringArg(args, "beforeId")
		after := stringArg(args, "afterId")
		if before == "" || after == "" {
			return "", fmt.Errorf("diff_scratchpad needs beforeId and afterId")
		}
		diff, err := agent.DiffScratchpadEntries(wsDir, agentID, before, after)
		if err != nil {
			return "", err
		}
		return diff, nil

	case "delete_scratchpad":
		id := stringArg(args, "id")
		if id == "" {
			return "", fmt.Errorf("delete_scratchpad needs an entry id")
		}
		if err := agent.DeleteScratchpad(d.cfg.AgentFolder, id); err != nil {
			return "", err
		}
		return "deleted " + id, nil

	default:
		return "", fmt.Errorf("unknown scratchpad tool %q", name)
	}
}

func stringArg(args map[string]any, key string) string {
	if v, ok := args[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func stringOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func intArg(args map[string]any, key string) *int {
	v, ok := args[key]
	if !ok || v == nil {
		return nil
	}
	switch n := v.(type) {
	case float64:
		i := int(n)
		return &i
	case int:
		return &n
	default:
		return nil
	}
}

func boolArg(args map[string]any, key string) bool {
	v, _ := args[key].(bool)
	return v
}

func mustJSON(v any) string {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf("{\"error\":%q}", err.Error())
	}
	return string(data)
}

// boolPtrArg distinguishes absent from false. wackypub defaults case_sensitive
// to true when the pointer is nil, so collapsing absent to false would change
// search behaviour.
func boolPtrArg(args map[string]any, key string) *bool {
	v, ok := args[key].(bool)
	if !ok {
		return nil
	}
	return &v
}
