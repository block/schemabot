package applynotifications

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQSPublish(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
	}{{"success", http.StatusOK}, {"queue unavailable", http.StatusServiceUnavailable}} {
		t.Run(tc.name, func(t *testing.T) {
			var attempts atomic.Int32
			bodies := make(chan map[string]string, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts.Add(1)
				var body map[string]string
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("decode SQS request: %v", err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				select {
				case bodies <- body:
				default:
				}
				w.Header().Set("Content-Type", "application/x-amz-json-1.0")
				w.WriteHeader(tc.status)
				_, err := w.Write([]byte(`{}`))
				assert.NoError(t, err)
			}))
			t.Cleanup(server.Close)
			t.Setenv("AWS_REGION", "us-east-1")
			t.Setenv("AWS_ACCESS_KEY_ID", "test")
			t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
			t.Setenv("AWS_SESSION_TOKEN", "")
			t.Setenv("AWS_PROFILE", "")
			t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "config"))
			t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials"))
			t.Setenv("AWS_ENDPOINT_URL_SQS", server.URL)
			queueURL := server.URL + "/123/completions"
			publisher, err := NewSQS(t.Context(), queueURL)
			require.NoError(t, err)
			completedAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
			err = publisher.Publish(t.Context(), Event{ApplyIdentifier: "apply-123", Database: "shop", DatabaseType: "mysql", Environment: "staging", CompletedAt: &completedAt})
			if tc.status == http.StatusOK {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "apply-123")
			}
			assert.Equal(t, int32(1), attempts.Load(), "SDK retries must be disabled even for retryable service failures")
			select {
			case body := <-bodies:
				assert.Equal(t, queueURL, body["QueueUrl"])
				assert.JSONEq(t, `{"apply_identifier":"apply-123","database":"shop","database_type":"mysql","environment":"staging","completed_at":"2026-01-02T03:04:05Z"}`, body["MessageBody"])
			default:
				require.FailNow(t, "SQS received no request")
			}
		})
	}
}
