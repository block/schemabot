package localruntime

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
)

// The control exchange proves possession of the private credential without
// transmitting it. A stale endpoint therefore cannot harvest the API token.
func signature(token, message string) string {
	mac := hmac.New(sha256.New, []byte(token))
	mac.Write([]byte(message))
	return hex.EncodeToString(mac.Sum(nil))
}

func requestMessage(req *http.Request) string {
	return "request\n" + req.Method + "\n" + req.URL.Path + "\n" + req.Header.Get("X-Runtime-Generation") + "\n" + req.Header.Get("X-Runtime-Nonce")
}

func validSignature(token, message, provided string) bool {
	return hmac.Equal([]byte(signature(token, message)), []byte(provided))
}

func writeControl(w http.ResponseWriter, req *http.Request, token string, value any) error {
	var data []byte
	if value != nil {
		var err error
		data, err = json.Marshal(value)
		if err != nil {
			return err
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Runtime-Signature", signature(token, "response\n"+req.Header.Get("X-Runtime-Nonce")+"\n"+string(data)))
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("write runtime control response: %w", err)
	}
	return nil
}
