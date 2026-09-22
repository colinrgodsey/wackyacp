package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type rpcMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type acpShim struct {
	script     string
	reader     *bufio.Reader
	writerMu   sync.Mutex
	writer     io.Writer
	reqCounter atomic.Int64
	respChans  sync.Map // map[any]chan *rpcMessage
}

func (s *acpShim) send(msg *rpcMessage) error {
	msg.JSONRPC = "2.0"
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	s.writerMu.Lock()
	defer s.writerMu.Unlock()
	_, err = fmt.Fprintf(s.writer, "%s\n", data)
	return err
}

func (s *acpShim) sendResult(id any, result any) error {
	return s.send(&rpcMessage{
		ID:     id,
		Result: result,
	})
}

func (s *acpShim) sendError(id any, code int, message string) error {
	return s.send(&rpcMessage{
		ID: id,
		Error: &rpcError{
			Code:    code,
			Message: message,
		},
	})
}

func (s *acpShim) sendNotification(method string, params any) error {
	var rawParams json.RawMessage
	if params != nil {
		data, err := json.Marshal(params)
		if err != nil {
			return err
		}
		rawParams = data
	}
	return s.send(&rpcMessage{
		Method: method,
		Params: rawParams,
	})
}

func (s *acpShim) callClient(method string, params any) (*rpcMessage, error) {
	id := s.reqCounter.Add(1)
	respCh := make(chan *rpcMessage, 1)
	s.respChans.Store(id, respCh)
	defer s.respChans.Delete(id)

	var rawParams json.RawMessage
	if params != nil {
		data, err := json.Marshal(params)
		if err != nil {
			return nil, err
		}
		rawParams = data
	}

	if err := s.send(&rpcMessage{
		ID:     id,
		Method: method,
		Params: rawParams,
	}); err != nil {
		return nil, err
	}

	select {
	case resp := <-respCh:
		return resp, nil
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("timeout waiting for client response to %s", method)
	}
}

func (s *acpShim) handleMessage(msg *rpcMessage) error {
	// If it's a response to a call we initiated
	if msg.ID != nil && msg.Method == "" {
		if chVal, ok := s.respChans.Load(msg.ID); ok {
			ch := chVal.(chan *rpcMessage)
			ch <- msg
			return nil
		}
		// Try int conversion if float64 from JSON unmarshaling
		if fId, ok := msg.ID.(float64); ok {
			if chVal, ok := s.respChans.Load(int64(fId)); ok {
				ch := chVal.(chan *rpcMessage)
				ch <- msg
				return nil
			}
		}
		return nil
	}

	// Incoming request / notification from client
	switch msg.Method {
	case "initialize":
		resumeSupported := true
		loadSupported := true
		if s.script == "no-resume-no-load" {
			resumeSupported = false
			loadSupported = false
		}
		return s.sendResult(msg.ID, map[string]any{
			"protocolVersion": 1,
			"agentCapabilities": map[string]any{
				"loadSession": loadSupported,
				"sessionCapabilities": map[string]any{
					"resume": resumeSupported,
				},
				"promptCapabilities": map[string]any{
					"embeddedContext": false,
				},
			},
			"agentInfo": map[string]any{
				"name":    "acpshimbin",
				"version": "1.0.0",
			},
		})

	case "session/resume":
		if s.script == "fail-resume" || s.script == "fail-load" {
			return s.sendError(msg.ID, -32000, "session resume not available")
		}
		var params struct {
			SessionID string         `json:"sessionId"`
			Cwd       string         `json:"cwd"`
			Meta      map[string]any `json:"_meta"`
		}
		_ = json.Unmarshal(msg.Params, &params)
		result := map[string]any{
			"modes": map[string]any{
				"currentModeId": "code",
			},
		}
		if params.Meta != nil {
			result["_meta"] = params.Meta
		}
		return s.sendResult(msg.ID, result)

	case "session/load":
		if s.script == "fail-load" {
			return s.sendError(msg.ID, -32001, "session load failed")
		}
		var params struct {
			SessionID string         `json:"sessionId"`
			Cwd       string         `json:"cwd"`
			Meta      map[string]any `json:"_meta"`
		}
		_ = json.Unmarshal(msg.Params, &params)
		result := map[string]any{
			"modes": map[string]any{
				"currentModeId": "code",
			},
		}
		if params.Meta != nil {
			result["_meta"] = params.Meta
		}
		return s.sendResult(msg.ID, result)

	case "session/new":
		var params struct {
			Cwd  string         `json:"cwd"`
			Meta map[string]any `json:"_meta"`
		}
		_ = json.Unmarshal(msg.Params, &params)
		result := map[string]any{
			"sessionId": "shim-session-" + s.script,
		}
		if params.Meta != nil {
			result["_meta"] = params.Meta
		}
		return s.sendResult(msg.ID, result)

	case "session/prompt":
		var params struct {
			SessionID string `json:"sessionId"`
			Prompt    []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"prompt"`
		}
		_ = json.Unmarshal(msg.Params, &params)

		promptText := "default"
		for _, part := range params.Prompt {
			if part.Type == "text" && part.Text != "" {
				promptText = part.Text
				break
			}
		}

		switch s.script {
		case "hang-on-permission":
			// Send request_permission to client
			resp, err := s.callClient("session/request_permission", map[string]any{
				"sessionId": params.SessionID,
				"toolCall": map[string]any{
					"toolCallId": "call-1",
					"title":      "Run bash command: rm -rf /",
				},
				"options": []map[string]any{
					{
						"optionId": "deny-opt",
						"name":     "Deny",
						"kind":     "reject_once",
					},
					{
						"optionId": "allow-opt",
						"name":     "Allow",
						"kind":     "allow_once",
					},
				},
			})
			if err != nil {
				return s.sendError(msg.ID, -32603, "permission call failed: "+err.Error())
			}

			// Verify client response
			outcomeStr := fmt.Sprintf("%v", resp.Result)
			_ = s.sendNotification("session/update", map[string]any{
				"sessionId": params.SessionID,
				"update": map[string]any{
					"sessionUpdate": "agent_message_chunk",
					"content": map[string]any{
						"type": "text",
						"text": "permission outcome: " + outcomeStr,
					},
				},
			})
			return s.sendResult(msg.ID, map[string]any{
				"stopReason": "end_turn",
			})

		case "emit-usage":
			// Send text chunk
			_ = s.sendNotification("session/update", map[string]any{
				"sessionId": params.SessionID,
				"update": map[string]any{
					"sessionUpdate": "agent_message_chunk",
					"content": map[string]any{
						"type": "text",
						"text": "usage chunk 1",
					},
				},
			})

			// Send usage notification
			_ = s.sendNotification("session/update", map[string]any{
				"sessionId": params.SessionID,
				"update": map[string]any{
					"sessionUpdate": "usage_update",
					"used":          150,
					"size":          200000,
					"cost": map[string]any{
						"amount":   0.005,
						"currency": "USD",
					},
				},
			})

			return s.sendResult(msg.ID, map[string]any{
				"stopReason": "end_turn",
				"usage": map[string]any{
					"inputTokens":  100,
					"outputTokens": 50,
					"totalTokens":  150,
				},
			})

		case "hold-turn":
			// Hold the turn (and therefore the shim process, and therefore wackyacp's
			// acp-session.lock flock) for ~1s so a concurrently-started second bridge has
			// time to observe lock contention.
			time.Sleep(1 * time.Second)
			_ = s.sendNotification("session/update", map[string]any{
				"sessionId": params.SessionID,
				"update": map[string]any{
					"sessionUpdate": "agent_message_chunk",
					"content": map[string]any{
						"type": "text",
						"text": "held: " + promptText,
					},
				},
			})
			return s.sendResult(msg.ID, map[string]any{
				"stopReason": "end_turn",
			})

		case "resume-ok":
			_ = s.sendNotification("session/update", map[string]any{
				"sessionId": params.SessionID,
				"update": map[string]any{
					"sessionUpdate": "agent_message_chunk",
					"content": map[string]any{
						"type": "text",
						"text": "resumed: " + promptText,
					},
				},
			})
			return s.sendResult(msg.ID, map[string]any{
				"stopReason": "end_turn",
			})

		case "fail-resume":
			_ = s.sendNotification("session/update", map[string]any{
				"sessionId": params.SessionID,
				"update": map[string]any{
					"sessionUpdate": "agent_message_chunk",
					"content": map[string]any{
						"type": "text",
						"text": "loaded: " + promptText,
					},
				},
			})
			return s.sendResult(msg.ID, map[string]any{
				"stopReason": "end_turn",
			})

		case "fail-load":
			_ = s.sendNotification("session/update", map[string]any{
				"sessionId": params.SessionID,
				"update": map[string]any{
					"sessionUpdate": "agent_message_chunk",
					"content": map[string]any{
						"type": "text",
						"text": "fresh: " + promptText,
					},
				},
			})
			return s.sendResult(msg.ID, map[string]any{
				"stopReason": "end_turn",
			})

		default:
			_ = s.sendNotification("session/update", map[string]any{
				"sessionId": params.SessionID,
				"update": map[string]any{
					"sessionUpdate": "agent_message_chunk",
					"content": map[string]any{
						"type": "text",
						"text": "echo from shim: " + promptText,
					},
				},
			})
			return s.sendResult(msg.ID, map[string]any{
				"stopReason": "end_turn",
			})
		}

	case "session/cancel":
		return nil

	default:
		if msg.ID != nil {
			return s.sendError(msg.ID, -32601, "method not found: "+msg.Method)
		}
		return nil
	}
}

func (s *acpShim) run() error {
	for {
		line, err := s.reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				if s.script == "ignore-eof" {
					done := make(chan struct{})
					go func() {
						time.Sleep(1 * time.Hour)
						close(done)
					}()
					<-done
				}
				return nil
			}
			return err
		}
		if len(line) == 0 {
			continue
		}

		var msg rpcMessage
		if err := json.Unmarshal(line, &msg); err != nil {
			continue
		}

		go func(m rpcMessage) {
			if err := s.handleMessage(&m); err != nil {
				fmt.Fprintf(os.Stderr, "acpshimbin handle error: %v\n", err)
			}
		}(msg)
	}
}

func main() {
	script := flag.String("script", "normal", "Scripted failure or execution mode")
	flag.Parse()

	shim := &acpShim{
		script: *script,
		reader: bufio.NewReader(os.Stdin),
		writer: os.Stdout,
	}

	if err := shim.run(); err != nil {
		fmt.Fprintf(os.Stderr, "acpshimbin error: %v\n", err)
		os.Exit(1)
	}
}
