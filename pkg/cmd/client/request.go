package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"golang.org/x/net/html"
)

// httpClient is the shared HTTP client for all CLI requests.
// Uses a 30s timeout to avoid hanging indefinitely on network stalls.
var httpClient = &http.Client{Timeout: 30 * time.Second}

// APIError represents an error response from the API.
type APIError struct {
	Status    int    // HTTP status code (e.g., 404, 500)
	ErrorCode string // Error code from API response (e.g., "not_found", "storage_error")
	Message   string
}

func (e *APIError) Error() string {
	return e.Message
}

// IsNotFound reports whether the error is a 404 from the API.
func IsNotFound(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound
}

// doGetInto sends a GET request and unmarshals the JSON response into result.
// Returns an *APIError for non-200 responses (use IsNotFound to check for 404).
func doGetInto(endpoint, path string, result any) error {
	return doGetIntoCtx(context.Background(), endpoint, path, result)
}

// doGetIntoCtx is like doGetInto but accepts a context for timeout/cancellation control.
func doGetIntoCtx(ctx context.Context, endpoint, path string, result any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+path, nil)
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return FormatConnectionError(endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return parseAPIError(resp.StatusCode, respBody)
	}

	if err := json.Unmarshal(respBody, result); err != nil {
		return checkNonJSONResponse(resp, respBody, err)
	}
	return nil
}

// doSendBody sends a request with a JSON body and checks for success.
// Used for POST/DELETE operations that don't need to parse the response.
func doSendBody(endpoint, method, path string, body any) error {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal request body: %w", err)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, endpoint+path, bytes.NewReader(jsonBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return FormatConnectionError(endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return parseAPIError(resp.StatusCode, respBody)
	}
	return nil
}

// doPostInto sends a JSON POST to endpoint+path and unmarshals the JSON response into result.
// Returns an *APIError for non-200 responses (use IsNotFound to check for 404).
func doPostInto(endpoint, path string, body any, result any) error {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal request body: %w", err)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint+path, bytes.NewReader(jsonBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return FormatConnectionError(endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return parseAPIError(resp.StatusCode, respBody)
	}

	if err := json.Unmarshal(respBody, result); err != nil {
		return checkNonJSONResponse(resp, respBody, err)
	}
	return nil
}

// checkNonJSONResponse provides a clear error when the server returns non-JSON
// (e.g., an HTML auth page from a proxy). Falls back to the original parse error.
func checkNonJSONResponse(resp *http.Response, body []byte, parseErr error) error {
	ct := resp.Header.Get("Content-Type")
	if strings.Contains(ct, "html") || (len(body) > 0 && body[0] == '<') {
		// Extract visible text from HTML for context (strip tags).
		text := stripHTMLTags(string(body))
		text = strings.Join(strings.Fields(text), " ") // collapse whitespace
		if len(text) > 200 {
			text = text[:200] + "..."
		}
		if text != "" {
			return fmt.Errorf("unexpected response from server (received HTML, expected JSON):\n  %s", text)
		}
		return fmt.Errorf("unexpected response from server (received HTML, expected JSON)")
	}
	return fmt.Errorf("parse response: %w", parseErr)
}

// stripHTMLTags extracts visible text from HTML using the x/net/html tokenizer.
// Skips <style> and <script> content.
func stripHTMLTags(s string) string {
	tokenizer := html.NewTokenizer(strings.NewReader(s))
	var b strings.Builder
	skip := false
	for {
		tt := tokenizer.Next()
		switch tt {
		case html.ErrorToken:
			return strings.TrimSpace(b.String())
		case html.StartTagToken:
			tn, _ := tokenizer.TagName()
			tag := string(tn)
			if tag == "style" || tag == "script" {
				skip = true
			}
		case html.EndTagToken:
			tn, _ := tokenizer.TagName()
			tag := string(tn)
			if tag == "style" || tag == "script" {
				skip = false
			}
		case html.TextToken:
			if !skip {
				b.Write(tokenizer.Text())
			}
		}
	}
}

// parseAPIError builds an APIError from a non-200 HTTP response, extracting
// the error_code if present in the JSON body.
func parseAPIError(statusCode int, body []byte) *APIError {
	apiErr := &APIError{
		Status:  statusCode,
		Message: FormatAPIError(statusCode, body),
	}
	var resp struct {
		ErrorCode string `json:"error_code"`
	}
	if json.Unmarshal(body, &resp) == nil && resp.ErrorCode != "" {
		apiErr.ErrorCode = resp.ErrorCode
	}
	return apiErr
}

// ConnectionError represents a client-side failure to reach the server
// (connection refused, DNS resolution, timeout). Distinct from APIError,
// which means the server responded with a non-200 status.
type ConnectionError struct {
	Endpoint string
	Err      error
}

func (e *ConnectionError) Error() string {
	return e.message()
}

func (e *ConnectionError) Unwrap() error {
	return e.Err
}

func (e *ConnectionError) message() string {
	msg := e.Err.Error()
	if strings.Contains(msg, "connection refused") {
		return fmt.Sprintf("cannot connect to %s (is the server running?)", e.Endpoint)
	}
	if strings.Contains(msg, "no such host") {
		return fmt.Sprintf("cannot resolve host: %s", e.Endpoint)
	}
	if strings.Contains(msg, "timeout") {
		return fmt.Sprintf("connection timeout: %s", e.Endpoint)
	}
	return fmt.Sprintf("connection failed: %s", e.Endpoint)
}

// FormatConnectionError returns a ConnectionError wrapping the underlying cause.
func FormatConnectionError(endpoint string, err error) error {
	return &ConnectionError{Endpoint: endpoint, Err: err}
}

// FormatAPIError returns a user-friendly error message from an API response.
func FormatAPIError(statusCode int, body []byte) string {
	var resp map[string]any
	if err := json.Unmarshal(body, &resp); err == nil {
		if msg, ok := resp["error"].(string); ok && msg != "" {
			return msg
		}
		if msg, ok := resp["message"].(string); ok && msg != "" {
			return msg
		}
		if msg, ok := resp["error_message"].(string); ok && msg != "" {
			return msg
		}
	}

	bodyStr := string(body)
	if len(bodyStr) > 100 {
		bodyStr = bodyStr[:100] + "..."
	}
	if bodyStr == "" {
		return fmt.Sprintf("HTTP %d", statusCode)
	}
	return fmt.Sprintf("HTTP %d: %s", statusCode, bodyStr)
}
