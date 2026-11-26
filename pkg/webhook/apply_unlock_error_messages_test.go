package webhook

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/api"
)

// An apply whose plan cannot reach a remote deployment names the deployment and
// target the operator configured and never the transport error, which carries
// hosts and gRPC text.
func TestApplyRemoteUnavailableMessageHidesTheTransportError(t *testing.T) {
	transport := status.Error(codes.Unavailable, "connection error: dial tcp 10.1.2.3:9090: connect: connection refused")

	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "deployment and target",
			err:  &api.RemoteDeploymentUnavailableError{Deployment: "pie", Target: "orders-staging", Err: transport},
			want: "SchemaBot could not reach the remote deployment `pie` for target `orders-staging`, so the apply did not start. Run the command again once that deployment is healthy; if it fails again, the SchemaBot server logs have the cause.",
		},
		{
			name: "deployment only",
			err:  &api.RemoteDeploymentUnavailableError{Deployment: "pie", Err: transport},
			want: "SchemaBot could not reach the remote deployment `pie`, so the apply did not start. Run the command again once that deployment is healthy; if it fails again, the SchemaBot server logs have the cause.",
		},
		{
			name: "wrapped typed error",
			err:  fmt.Errorf("plan orders/staging: %w", &api.RemoteDeploymentUnavailableError{Deployment: "pie", Target: "orders-staging", Err: transport}),
			want: "SchemaBot could not reach the remote deployment `pie` for target `orders-staging`, so the apply did not start. Run the command again once that deployment is healthy; if it fails again, the SchemaBot server logs have the cause.",
		},
		{
			name: "gRPC unavailable without a deployment",
			err:  transport,
			want: "SchemaBot could not reach the remote schema change service, so the apply did not start. Run the command again once the service is healthy; if it fails again, the SchemaBot server logs have the cause.",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := applyRemoteUnavailableMessage(tc.err)

			assert.Equal(t, tc.want, got)
			assert.NotContains(t, got, "dial tcp")
			assert.NotContains(t, got, "Raw error")
		})
	}
}

// An apply that lost the lock to another owner is told so, and one whose lock
// acquire failed for any other reason gets the fixed failure line.
func TestApplyLockAcquireMessagesNameTheDatabase(t *testing.T) {
	assert.Equal(t,
		"The lock for `orders` changed while this command ran, so the apply did not start. Run the command again to see who holds it now.",
		applyLockHeldAtAcquireMessage("orders"))
	assert.Equal(t,
		"SchemaBot could not take the lock for `orders`, so the apply did not start. Run the command again; if it fails again, the SchemaBot server logs have the cause.",
		applyLockAcquireFailedMessage("orders"))
}

// An accepted apply whose stored check state could not be updated keeps the two
// refusals SchemaBot decides on its own, each with the fix it calls for, and
// shows every other failure as a fixed line that names neither the cause nor a
// promise the code does not keep.
func TestApplyCheckUpdateFailedMessage(t *testing.T) {
	storageFailure := errors.New("upsert stored check state: dial tcp 10.1.2.3:3306: connection refused")

	tests := []struct {
		name         string
		err          error
		wantContains []string
		wantAbsent   []string
	}{
		{
			name:         "storage failure",
			err:          fmt.Errorf("apply start: %w", storageFailure),
			wantContains: []string{"`orders`", "`staging`", "was accepted", "could not update the required status check", "The apply itself is not affected", "Once the apply finishes, run plan again to refresh the check."},
			wantAbsent:   []string{"connection refused", "dial tcp", "drift", "namespace placement", "server logs", "does not show it yet"},
		},
		{
			name:         "deployment drift block",
			err:          fmt.Errorf("apply start refused for repo r pr 1: %w", errApplyStartDriftBlocked),
			wantContains: []string{"`orders`", "`staging`", "was accepted", "deployment drift", "Reconcile the deployments", "run plan again"},
			wantAbsent:   []string{"apply_id", "repo r", "server logs"},
		},
		{
			name:         "namespace placement block",
			err:          fmt.Errorf("apply start refused for repo r pr 1: %w", errApplyStartPlacementBlocked),
			wantContains: []string{"`orders`", "`staging`", "was accepted", "namespace placement", "Fix the namespace placement", "run plan again"},
			wantAbsent:   []string{"apply_id", "repo r", "drift", "server logs"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := applyCheckUpdateFailedMessage("orders", "staging", tc.err)

			for _, want := range tc.wantContains {
				assert.Contains(t, got, want)
			}
			for _, absent := range tc.wantAbsent {
				assert.NotContains(t, got, absent)
			}
			assert.NotContains(t, got, "&", "the text must survive HTML escaping unchanged")
		})
	}
}
