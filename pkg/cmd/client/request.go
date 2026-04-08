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
)

// httpClient is the shared HTTP client for all CLI requests.
// Uses a 30s timeout to avoid hanging indefinitely on network stalls.
var httpClient = &http.Client{Timeout: 30 * time.Second}

// APIError represents an error response from the API with an HTTP status code.
type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	return e.Message
}

// IsNotFound reports whether the error is a 404 from the API.
func IsNotFound(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound
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
		return &APIError{
			StatusCode: resp.StatusCode,
			Message:    FormatAPIError(resp.StatusCode, respBody),
		}
	}

	if err := json.Unmarshal(respBody, result); err != nil {
		return fmt.Errorf("parse response: %w", err)
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
		return &APIError{
			StatusCode: resp.StatusCode,
			Message:    FormatAPIError(resp.StatusCode, respBody),
		}
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
		return &APIError{
			StatusCode: resp.StatusCode,
			Message:    FormatAPIError(resp.StatusCode, respBody),
		}
	}

	if err := json.Unmarshal(respBody, result); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}
	return nil
}

// FormatConnectionError returns a user-friendly error for connection failures.
func FormatConnectionError(endpoint string, err error) error {
	if strings.Contains(err.Error(), "connection refused") {
		return fmt.Errorf("cannot connect to %s (is the server running?)", endpoint)
	}
	if strings.Contains(err.Error(), "no such host") {
		return fmt.Errorf("cannot resolve host: %s", endpoint)
	}
	if strings.Contains(err.Error(), "timeout") {
		return fmt.Errorf("connection timeout: %s", endpoint)
	}
	return fmt.Errorf("connection failed: %s", endpoint)
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
