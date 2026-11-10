package protocol

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// AuthCanonical builds Executor WS 1.1 HMAC canonical string.
func AuthCanonical(code, credentialID, instanceKey, timestamp, nonce string) string {
	return strings.Join([]string{
		"AUTH",
		code,
		credentialID,
		instanceKey,
		timestamp,
		nonce,
	}, "\n")
}

func SignHMAC(secret, canonical string) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(canonical))
	return hex.EncodeToString(m.Sum(nil))
}
