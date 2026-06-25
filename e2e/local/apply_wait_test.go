//go:build e2e

package local

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/e2e/testutil"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/state"
)

func extractApplyID(t *testing.T, output string) string {
	t.Helper()
	for line := range strings.SplitSeq(strings.TrimSpace(output), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var result struct {
			ApplyID string `json:"apply_id"`
		}
		if err := json.Unmarshal([]byte(line), &result); err == nil && result.ApplyID != "" {
			return result.ApplyID
		}
	}
	require.Failf(t, "apply_id not found", "could not find apply_id in JSON output: %s", output)
	return ""
}

func fetchApplyState(endpoint, applyID string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), testutil.ProgressTimeout)
	defer cancel()
	result, err := client.GetProgressCtx(ctx, endpoint, applyID)
	if err != nil {
		return "", err
	}
	return state.NormalizeState(result.State), nil
}

func waitForApplyState(t *testing.T, endpoint, applyID, expectedState string, timeout time.Duration) {
	t.Helper()
	expected := strings.ToLower(expectedState)
	var lastState, lastError string
	start := time.Now()
	testutil.Poll(t, timeout, 300*time.Millisecond,
		func() bool {
			ctx, cancel := context.WithTimeout(t.Context(), testutil.ProgressTimeout)
			result, err := client.GetProgressCtx(ctx, endpoint, applyID)
			cancel()
			if err != nil {
				t.Logf("waitForApplyState: %s poll error: %v (elapsed=%s)", applyID, err, time.Since(start))
				return false
			}
			newState := state.NormalizeState(result.State)
			if newState != lastState {
				t.Logf("waitForApplyState: %s state=%s (elapsed=%s)", applyID, newState, time.Since(start))
			}
			lastState = newState
			lastError = result.ErrorMessage
			return lastState == expected
		},
		func() string {
			return fmt.Sprintf("timeout waiting for apply %s state %q after %s, last API state: %q, error: %q\n%s",
				applyID, expectedState, time.Since(start), lastState, lastError,
				applyTimeoutDiagnostics(applyID))
		},
	)
}

// applyTimeoutDiagnostics returns a best-effort dump of the SchemaBot storage
// state for applyID plus the operator's overall backlog, for inclusion in a
// wait-timeout failure message. It answers the triage question when an apply
// never leaves its starting state: whether a driver ever claimed the apply or
// its operations (lease_owner, updated_at), and whether the operator pool is
// busy with other in-flight applies. It never fails the test — any query error
// is rendered into the returned text, since the caller is already on the
// timeout path.
func applyTimeoutDiagnostics(applyID string) string {
	dsn := os.Getenv("E2E_MYSQL_DSN")
	if dsn == "" {
		return "diagnostics: E2E_MYSQL_DSN not set"
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Sprintf("diagnostics: open schemabot db: %v", err)
	}
	defer utils.CloseAndLog(db)

	ctx, cancel := context.WithTimeout(context.Background(), testutil.ProgressTimeout)
	defer cancel()

	var b strings.Builder
	b.WriteString("--- apply timeout diagnostics ---\n")

	var (
		applyDBID                        int64
		applyState, leaseOwner, applyErr string
		attempt                          int
		updatedAt                        string
	)
	err = db.QueryRowContext(ctx,
		"SELECT id, state, attempt, lease_owner, updated_at, COALESCE(error_message, '') "+
			"FROM applies WHERE apply_identifier = ?", applyID).
		Scan(&applyDBID, &applyState, &attempt, &leaseOwner, &updatedAt, &applyErr)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		fmt.Fprintf(&b, "applies: no row for %s\n", applyID)
		return b.String()
	case err != nil:
		fmt.Fprintf(&b, "applies: query error: %v\n", err)
		return b.String()
	}
	fmt.Fprintf(&b, "applies: id=%d state=%q attempt=%d lease_owner=%q updated_at=%s error=%q\n",
		applyDBID, applyState, attempt, leaseOwner, updatedAt, applyErr)

	opRows, err := db.QueryContext(ctx,
		"SELECT id, deployment, state, lease_owner, updated_at FROM apply_operations "+
			"WHERE apply_id = ? ORDER BY id", applyDBID)
	if err != nil {
		fmt.Fprintf(&b, "apply_operations: query error: %v\n", err)
	} else {
		func() {
			defer utils.CloseAndLog(opRows)
			for opRows.Next() {
				var (
					opID                              int64
					deployment, opState, opLeaseOwner string
					opUpdatedAt                       string
				)
				if err := opRows.Scan(&opID, &deployment, &opState, &opLeaseOwner, &opUpdatedAt); err != nil {
					fmt.Fprintf(&b, "apply_operations: scan error: %v\n", err)
					return
				}
				fmt.Fprintf(&b, "  operation id=%d deployment=%q state=%q lease_owner=%q updated_at=%s\n",
					opID, deployment, opState, opLeaseOwner, opUpdatedAt)
			}
			if err := opRows.Err(); err != nil {
				fmt.Fprintf(&b, "apply_operations: row iteration error: %v\n", err)
			}
		}()
	}

	// Operator backlog: every apply grouped by state, so triage can see how
	// many sit in non-terminal states (e.g. pending, running) competing for
	// drivers when this one is stuck. A large non-terminal count points at
	// driver-pool starvation.
	backlogRows, err := db.QueryContext(ctx,
		"SELECT state, COUNT(*) FROM applies GROUP BY state ORDER BY state")
	if err != nil {
		fmt.Fprintf(&b, "applies backlog: query error: %v\n", err)
	} else {
		func() {
			defer utils.CloseAndLog(backlogRows)
			b.WriteString("applies by state:")
			for backlogRows.Next() {
				var s string
				var n int
				if err := backlogRows.Scan(&s, &n); err != nil {
					fmt.Fprintf(&b, " scan error: %v", err)
					return
				}
				fmt.Fprintf(&b, " %s=%d", s, n)
			}
			if err := backlogRows.Err(); err != nil {
				fmt.Fprintf(&b, " row iteration error: %v", err)
			}
			b.WriteString("\n")
		}()
	}

	logRows, err := db.QueryContext(ctx,
		"SELECT level, event_type, COALESCE(old_state, ''), COALESCE(new_state, ''), message, created_at "+
			"FROM apply_logs WHERE apply_id = ? ORDER BY id DESC LIMIT 20", applyDBID)
	if err != nil {
		fmt.Fprintf(&b, "apply_logs: query error: %v\n", err)
	} else {
		func() {
			defer utils.CloseAndLog(logRows)
			b.WriteString("recent apply_logs (newest first):\n")
			for logRows.Next() {
				var level, eventType, oldState, newState, message, createdAt string
				if err := logRows.Scan(&level, &eventType, &oldState, &newState, &message, &createdAt); err != nil {
					fmt.Fprintf(&b, "  scan error: %v\n", err)
					return
				}
				fmt.Fprintf(&b, "  [%s] %s %s->%s %s: %s\n",
					createdAt, level, oldState, newState, eventType, message)
			}
			if err := logRows.Err(); err != nil {
				fmt.Fprintf(&b, "  row iteration error: %v\n", err)
			}
		}()
	}

	return b.String()
}

func waitForApplyAnyState(t *testing.T, endpoint, applyID string, expectedStates []string, timeout time.Duration) string {
	t.Helper()
	return testutil.WaitForAnyState(t, endpoint, applyID, expectedStates, timeout)
}
