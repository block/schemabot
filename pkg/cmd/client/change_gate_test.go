package client

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

// Apply, start and cutover send the override reason, trimmed, when one is
// given and leave the field out of the body when it is not.
func TestChangeGateCallsSendOverrideOnlyWhenSet(t *testing.T) {
	calls := map[string]struct {
		path string
		call func(url, override string) error
	}{
		"apply": {"/api/apply", func(url, override string) error {
			_, err := CallApplyAPIWithOverride(url, "plan-1", "production", "", "", true, override, nil)
			return err
		}},
		"start": {"/api/start", func(url, override string) error {
			_, err := CallStartAPIWithOverride(url, "production", "apply-1", override)
			return err
		}},
		"cutover": {"/api/cutover", func(url, override string) error {
			_, err := CallCutoverAPIWithOverride(url, "production", "apply-1", override)
			return err
		}},
	}
	for name, tc := range calls {
		t.Run(name, func(t *testing.T) {
			url, seen := freezeServer(t, http.StatusOK, map[string]any{"accepted": true})

			require.NoError(t, tc.call(url, "  SEV-1234 fix the index "))
			assert.Equal(t, tc.path, seen.path)
			assert.Equal(t, "SEV-1234 fix the index", seen.body["override"])

			require.NoError(t, tc.call(url, ""))
			assert.NotContains(t, seen.body, "override")

			require.NoError(t, tc.call(url, "   "))
			assert.NotContains(t, seen.body, "override", "whitespace is no reason")
		})
	}
}

// The wrappers that predate --override ask for none.
func TestChangeGateCallsWithoutOverrideSendNone(t *testing.T) {
	url, seen := freezeServer(t, http.StatusOK, map[string]any{"accepted": true})

	_, err := CallApplyAPIForTarget(url, "plan-1", "production", "", "", true, nil)
	require.NoError(t, err)
	assert.NotContains(t, seen.body, "override")
	_, err = CallStartAPI(url, "production", "apply-1")
	require.NoError(t, err)
	assert.NotContains(t, seen.body, "override")
	_, err = CallCutoverAPI(url, "production", "apply-1")
	require.NoError(t, err)
	assert.NotContains(t, seen.body, "override")
}

// A change_gate_refused answer reaches the caller with its blockers and the
// override request, through the one decode of the error body.
func TestChangeGateRefusalDecodesBlockersAndOverrideRequest(t *testing.T) {
	ends := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	url, _ := freezeServer(t, http.StatusConflict, apitypes.ErrorResponse{
		Error:     "production is frozen",
		ErrorCode: apitypes.ErrCodeChangeGateRefused,
		Blockers: []apitypes.ChangeGateBlocker{
			{Gate: "freeze", ID: "freeze-abc", Reason: "SEV-1234", EndsAt: &ends},
		},
		OverrideRequest: &apitypes.FreezeOverrideRequest{ID: "ovr-123"},
	})

	_, err := CallStartAPIWithOverride(url, "production", "apply-1", "SEV-1234 fix the index")

	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusConflict, apiErr.Status)
	assert.Equal(t, apitypes.ErrCodeChangeGateRefused, apiErr.ErrorCode)
	require.Len(t, apiErr.Blockers, 1)
	assert.Equal(t, "freeze-abc", apiErr.Blockers[0].ID)
	assert.Equal(t, "SEV-1234", apiErr.Blockers[0].Reason)
	assert.Equal(t, ends, *apiErr.Blockers[0].EndsAt)
	require.NotNil(t, apiErr.OverrideRequest)
	assert.Equal(t, "ovr-123", apiErr.OverrideRequest.ID)
}

// A refusal with no recorded request leaves OverrideRequest nil.
func TestChangeGateRefusalWithoutOverrideRequest(t *testing.T) {
	url, _ := freezeServer(t, http.StatusConflict, apitypes.ErrorResponse{
		Error:     "production is frozen",
		ErrorCode: apitypes.ErrCodeChangeGateRefused,
		Blockers:  []apitypes.ChangeGateBlocker{{Gate: "freeze", ID: "freeze-abc"}},
	})

	_, err := CallCutoverAPI(url, "production", "apply-1")

	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Nil(t, apiErr.OverrideRequest)
	assert.Len(t, apiErr.Blockers, 1)
}

// A lost response to an apply, start or cutover is still reported with the
// status command, whatever the request carried.
func TestChangeGateCallsLostResponseIsShownByStatus(t *testing.T) {
	for _, path := range []string{"/api/apply", "/api/start", "/api/cutover"} {
		assert.Equal(t, shownBy("status"), effectOf(http.MethodPost, path), path)
	}
}
