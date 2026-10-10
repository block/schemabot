package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
)

// A dispatch refused because another change already holds the database is the
// one rejection an operator can act on themselves, so the message stored on the
// apply — and rendered into the pull request — names the work, the state
// holding it, the pull request that owns it, and what ends the hold.
func TestRemoteApplyRejectionComposesTheConflictAnOperatorCanActOn(t *testing.T) {
	resp := &ternv1.ApplyResponse{
		Accepted:     false,
		ErrorMessage: `schema change already in progress for database "payments" (plan plan-9f2)`,
		Conflict: &ternv1.ApplyConflict{
			Table:         "balance_accounts",
			BlockingState: state.Apply.Stopped,
			Repository:    "acme/payments",
			PullRequest:   4821,
			Caller:        "webhook:acme/payments#4821",
		},
	}

	assert.Equal(t,
		"Table balance_accounts is held by a schema change (Stopped) on acme/payments#4821; "+
			"it holds the database until it is started or cancelled.",
		remoteApplyRejectionMessage(resp, "", "remote apply was not accepted"))
}

// The structured conflict outranks the remote's own prose. That text is the
// engine's, and passing it through would put whatever it caught — including the
// engine's own identifiers, which resolve nowhere an operator can reach — onto
// a public pull request.
func TestRemoteApplyRejectionPrefersTheConflictOverRemoteProse(t *testing.T) {
	resp := &ternv1.ApplyResponse{
		ErrorMessage: "dial tcp 10.4.2.11:3306: connect: connection refused (apply-49ea5a453e9a4f18)",
		Conflict: &ternv1.ApplyConflict{
			Table:         "balance_accounts",
			BlockingState: state.Apply.Running,
			Caller:        "cli:dana@laptop",
		},
	}

	message := remoteApplyRejectionMessage(resp, "", "remote apply was not accepted")
	assert.Equal(t,
		"Table balance_accounts is held by a schema change (Running) started by cli:dana@laptop; "+
			"it releases the database when it finishes, unless it parks for cutover first.",
		message)
	assert.NotContains(t, message, "10.4.2.11")
	assert.NotContains(t, message, "apply-49ea5a453e9a4f18")
}

// A rejection that is not a conflict keeps the data plane's own message, which
// is the line it chose to return; a rejection that carried no message at all
// falls back to the caller's, so an apply never fails without a reason.
func TestRemoteApplyRejectionFallsBackWhenThereIsNoConflict(t *testing.T) {
	assert.Equal(t, "unsafe change requires an explicit opt-in",
		remoteApplyRejectionMessage(
			&ternv1.ApplyResponse{ErrorMessage: "unsafe change requires an explicit opt-in"},
			"", "remote apply was not accepted"))

	assert.Equal(t, "remote apply was not accepted",
		remoteApplyRejectionMessage(&ternv1.ApplyResponse{}, "", "remote apply was not accepted"))

	assert.Equal(t, "remote apply was not accepted",
		remoteApplyRejectionMessage(nil, "", "remote apply was not accepted"))
}

// The handle an operator needs is this plane's identifier for the holding
// change, never the engine's. The engine's crosses the wire so the control plane
// can look its own up, and a control plane that finds no match says nothing
// rather than offering an identifier its own commands refuse.
func TestRemoteApplyRejectionNamesOnlyAnIdentifierThatResolvesHere(t *testing.T) {
	resp := &ternv1.ApplyResponse{
		Conflict: &ternv1.ApplyConflict{
			Table:            "xfers",
			BlockingState:    state.Apply.Running,
			Repository:       "acme/payments",
			PullRequest:      12,
			HolderExternalId: "apply-49ea5a453e9a4f18",
		},
	}

	resolved := remoteApplyRejectionMessage(resp, "apply-a1b2c3d4e5f67890", "remote apply was not accepted")
	assert.Contains(t, resolved, "The holding apply is apply-a1b2c3d4e5f67890.")
	assert.NotContains(t, resolved, "apply-49ea5a453e9a4f18")

	unresolved := remoteApplyRejectionMessage(resp, "", "remote apply was not accepted")
	assert.NotContains(t, unresolved, "apply-49ea5a453e9a4f18")
	assert.NotContains(t, unresolved, "The holding apply is")
}

// A change started outside a pull request has no link to follow, so the handle
// is the only thing an operator can act on. Without it the refusal names a
// person and leaves them to find the schema change themselves.
func TestRemoteApplyRejectionCarriesTheHandleWhenNoPullRequestOwnsTheHolder(t *testing.T) {
	resp := &ternv1.ApplyResponse{
		Conflict: &ternv1.ApplyConflict{
			Table:            "xfers",
			BlockingState:    state.Apply.Running,
			Caller:           "cli:dana@laptop",
			HolderExternalId: "apply-49ea5a453e9a4f18",
		},
	}

	assert.Equal(t,
		"Table xfers is held by a schema change (Running) started by cli:dana@laptop; "+
			"it releases the database when it finishes, unless it parks for cutover first. "+
			"The holding apply is apply-a1b2c3d4e5f67890.",
		remoteApplyRejectionMessage(resp, "apply-a1b2c3d4e5f67890", "remote apply was not accepted"))
}

// The subject narrows exactly as far as the data plane could prove: a sharded
// change holds one primary and says so, a multi-table atomic change names no
// table, and a state whose next move is not certain promises nothing.
func TestApplyConflictMessageNarrowsToWhatWasProven(t *testing.T) {
	for _, tc := range []struct {
		name          string
		conflict      *ternv1.ApplyConflict
		holderApplyID string
		want          string
	}{
		{
			name: "a sharded change holds one primary, so the message names it",
			conflict: &ternv1.ApplyConflict{
				Table: "xfers", Shard: "-40", BlockingState: state.Apply.WaitingForCutover,
				Repository: "acme/payments", PullRequest: 12,
			},
			want: "Table xfers shard -40 is held by a schema change (Waiting for cutover) on acme/payments#12; " +
				"it holds the database until it is cut over or cancelled.",
		},
		{
			name: "a multi-table atomic change names no table, so the shard carries the subject",
			conflict: &ternv1.ApplyConflict{
				Shard: "-40", BlockingState: state.Apply.FailedRetryable,
			},
			want: "Shard -40 of this database is held by a schema change (Retrying); " +
				"it holds the database until it is retried or cancelled.",
		},
		{
			name: "neither table nor shard leaves the database itself as the subject",
			conflict: &ternv1.ApplyConflict{
				BlockingState: state.Apply.RevertWindow, Repository: "acme/payments", PullRequest: 12,
			},
			want: "This database is held by a schema change (Revert window) on acme/payments#12; " +
				"it holds the database until it is reverted or skip-reverted.",
		},
		{
			name: "a state whose next move is not certain is named without promising an outcome",
			conflict: &ternv1.ApplyConflict{
				Table: "xfers", BlockingState: state.Apply.CuttingOver, Repository: "acme/payments", PullRequest: 12,
			},
			want: "Table xfers is held by a schema change (Cutting over) on acme/payments#12.",
		},
		{
			name: "a pull request outranks the caller string it was derived from",
			conflict: &ternv1.ApplyConflict{
				Table: "xfers", BlockingState: state.Apply.Stopped,
				Repository: "acme/payments", PullRequest: 12, Caller: "webhook:acme/payments#12",
			},
			want: "Table xfers is held by a schema change (Stopped) on acme/payments#12; " +
				"it holds the database until it is started or cancelled.",
		},
		{
			name: "a change this control plane dispatched is named by the handle its own commands take",
			conflict: &ternv1.ApplyConflict{
				Table: "xfers", BlockingState: state.Apply.Stopped,
				Repository: "acme/payments", PullRequest: 12,
			},
			holderApplyID: "apply-a1b2c3d4e5f67890",
			want: "Table xfers is held by a schema change (Stopped) on acme/payments#12; " +
				"it holds the database until it is started or cancelled. " +
				"The holding apply is apply-a1b2c3d4e5f67890.",
		},
		{
			name: "a state that promises nothing still carries the handle",
			conflict: &ternv1.ApplyConflict{
				Table: "xfers", BlockingState: state.Apply.CuttingOver,
			},
			holderApplyID: "apply-a1b2c3d4e5f67890",
			want: "Table xfers is held by a schema change (Cutting over). " +
				"The holding apply is apply-a1b2c3d4e5f67890.",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, applyConflictMessage(tc.conflict, tc.holderApplyID))
		})
	}

	assert.Empty(t, applyConflictMessage(nil, "apply-a1b2c3d4e5f67890"))
}
