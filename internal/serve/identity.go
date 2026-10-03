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
