package serve

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
)

// SessionID returns the stable ACP session id for an agent folder:
// "wackyacp:" plus the first 16 hex characters of the SHA-256 of the
// folder's absolute slash-separated path. The id is a function of the folder
// alone - never of session contents - so attaching to a folder always yields
// the same id, across serve processes and across restarts. Renaming the
// folder changes the id; that is deliberate (a different folder is a
// different session).
func SessionID(agentFolder string) string {
	sum := sha256.Sum256([]byte(filepath.ToSlash(filepath.Clean(agentFolder))))
	return "wackyacp:" + hex.EncodeToString(sum[:])[:16]
}

// SessionIDForAgent returns the stable ACP session id for remote mode, where
// the conversation is identified by the remote agent id instead of a local
// folder: "wackyacp:" plus the FULL 32-hex SHA-256 of "agent:<id>". As with
// SessionID the id is a pure function of the identity - it never depends on
// session contents - so re-attaching to the same remote agent always yields
// the same id, across serve processes and restarts.
//
// The full-32-hex body is load-bearing, not cosmetic: a folder-derived id is
// 16 hex chars and the hash input is the bare path, so a folder literally
// named "agent:agent1" would collide with a shorter agent-derived body. Full
// length keeps the two derivations disjoint by construction.
func SessionIDForAgent(agentID string) string {
	sum := sha256.Sum256([]byte("agent:" + agentID))
	return "wackyacp:" + hex.EncodeToString(sum[:])
}
