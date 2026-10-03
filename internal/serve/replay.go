package serve

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/colinrgodsey/wackyacp/internal/acp"
	"google.golang.org/genai"

	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
)

// Replay streams the persisted session history as ACP session/update
// notifications, oldest turn first, mirroring the reference ACP server's
// load-session replay. Fidelity comes from ReadSession's content_json field:
// the 1:1 genai.Content serialization carries thought flags and function
// calls, so a front-end re-opening the project sees thinking and tool
// activity, not just prose. Turns without content_json (ancient sessions)
// are skipped and logged rather than guessed at.
//
// Function calls and their responses live in different turns (the call in
// the model turn, the response in the following user turn), so replay pairs
// them by call id when present and by name (FIFO) otherwise; an unmatched
// call is left in pending status rather than fabricated into a result.
func (b *Backend) Replay(ctx context.Context, emit func(Update) error) error {
	resp, err := b.svc.ReadSession(ctx, &agentv1.ReadSessionRequest{
		AgentId:      b.agentID,
		WorkspaceDir: b.workspaceDir,
	})
	if err != nil {
		return fmt.Errorf("reading session: %w", err)
	}

	openByID := map[string]string{}     // call id -> toolCallId
	openByName := map[string][]string{} // call name -> queue of toolCallIds

	for _, t := range resp.GetTurns() {
		if t.GetContentJson() == "" {
			continue
		}
		var content genai.Content
		if err := json.Unmarshal([]byte(t.GetContentJson()), &content); err != nil {
			b.logf("wackyacp serve: replay: skipping turn %d: unparsable content_json: %v", t.GetSeq(), err)
			continue
		}
		isUser := content.Role == "user"
		for i, p := range content.Parts {
			if p == nil {
				continue
			}
			switch {
			case p.Text != "":
				switch {
				case p.Thought:
					if err := emit(ChunkUpdate(acp.UpdateKindAgentThoughtChunk, p.Text)); err != nil {
						return err
					}
				case isUser:
					if err := emit(ChunkUpdate(acp.UpdateKindUserMessageChunk, p.Text)); err != nil {
						return err
					}
				default:
					if err := emit(ChunkUpdate(acp.UpdateKindAgentMessageChunk, p.Text)); err != nil {
						return err
					}
				}
			case p.FunctionCall != nil:
				id := p.FunctionCall.ID
				if id == "" {
					id = fmt.Sprintf("replay-%d-%d", t.GetSeq(), i)
				}
				openByID[id] = id
				openByName[p.FunctionCall.Name] = append(openByName[p.FunctionCall.Name], id)
				if err := emit(ToolCallUpdatePayload(acp.UpdateKindToolCall, id, p.FunctionCall.Name, acp.ToolStatusPending, p.FunctionCall.Args, nil)); err != nil {
					return err
				}
			case p.FunctionResponse != nil:
				id := ""
				if p.FunctionResponse.ID != "" {
					id = openByID[p.FunctionResponse.ID]
					delete(openByID, p.FunctionResponse.ID)
				} else if q := openByName[p.FunctionResponse.Name]; len(q) > 0 {
					id = q[0]
					openByName[p.FunctionResponse.Name] = q[1:]
				}
				if id == "" {
					// Response for a call that predates this replay window: no
					// tool call to attach it to, so surface it as text.
					if txt, ok := responseText(p.FunctionResponse.Response); ok {
						if err := emit(ChunkUpdate(acp.UpdateKindAgentMessageChunk, txt)); err != nil {
							return err
						}
					}
					continue
				}
				txt, ok := responseText(p.FunctionResponse.Response)
				var result *string
				if ok {
					result = &txt
				}
				if err := emit(ToolCallUpdatePayload(acp.UpdateKindToolCallUpdate, id, p.FunctionResponse.Name, acp.ToolStatusCompleted, nil, result)); err != nil {
					return err
				}
			case p.InlineData != nil:
				// Media parts have no ACP replay slot of their own in v1; a
				// label keeps the history honest without shipping base64.
				mime := ""
				if p.InlineData != nil {
					mime = p.InlineData.MIMEType
				}
				kind := acp.UpdateKindAgentMessageChunk
				if isUser {
					kind = acp.UpdateKindUserMessageChunk
				}
				if err := emit(ChunkUpdate(kind, fmt.Sprintf("[inline data: %s]", mime))); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// responseText renders a genai FunctionResponse payload as display text:
// strings pass through, everything else is JSON-encoded.
func responseText(v any) (string, bool) {
	if v == nil {
		return "", false
	}
	if s, ok := v.(string); ok {
		return s, true
	}
	data, err := json.Marshal(v)
	if err != nil {
		return "", false
	}
	return string(data), true
}
