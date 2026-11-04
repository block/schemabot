package api

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// A schema file comparison runs on the environment's primary deployment
// only, the one its plans run on, and hands that deployment all four
// versions of the file. Its verdict passes through; a comparison that fails
// proves nothing, so it returns same=false with an error that names the
// deployment. Each comparison is bounded by the Service's deadline, and a
// caller's shorter deadline is kept.
func TestServiceCompareSchemaFileChange(t *testing.T) {
	compare := func(ctx context.Context, svc *Service) (bool, string, error) {
		return svc.CompareSchemaFileChange(ctx, "testapp", "production",
			"approved base", "approved", "head base", "head")
	}

	t.Run("only the primary deployment receives all four versions", func(t *testing.T) {
		eu := &mockTernClient{compareResp: &ternv1.CompareSchemaFileChangeResponse{Reason: "the head declares column note differently from the approved commit"}}
		us := &mockTernClient{compareResp: &ternv1.CompareSchemaFileChangeResponse{Same: true}}
		same, reason, err := compare(t.Context(), twoDeploymentService(t, eu, us))
		require.NoError(t, err)
		assert.False(t, same)
		assert.Equal(t, "the head declares column note differently from the approved commit", reason)

		require.Len(t, eu.compareReqs, 1)
		req := eu.compareReqs[0]
		assert.Equal(t, "testapp", req.GetDatabase())
		assert.Equal(t, storage.DatabaseTypeMySQL, req.GetType())
		assert.Equal(t, "production", req.GetEnvironment())
		assert.Equal(t, "testapp", req.GetTarget())
		assert.Equal(t, [4]string{"approved base", "approved", "head base", "head"},
			[4]string{req.GetApprovedBase(), req.GetApproved(), req.GetHeadBase(), req.GetHead()})
		assert.Empty(t, us.compareReqs, "a deployment other than the primary is never asked")
	})

	t.Run("a same verdict passes through", func(t *testing.T) {
		eu := &mockTernClient{compareResp: &ternv1.CompareSchemaFileChangeResponse{Same: true}}
		same, reason, err := compare(t.Context(), twoDeploymentService(t, eu, &mockTernClient{}))
		require.NoError(t, err)
		assert.True(t, same)
		assert.Empty(t, reason)
	})

	t.Run("a comparison that fails proves nothing", func(t *testing.T) {
		eu := &mockTernClient{compareErr: errors.New("dial tcp: connection refused")}
		same, reason, err := compare(t.Context(), twoDeploymentService(t, eu, &mockTernClient{}))
		require.ErrorContains(t, err, "connection refused")
		assert.ErrorContains(t, err, `on deployment "eu"`)
		assert.False(t, same)
		assert.Empty(t, reason)
	})

	t.Run("a database with no target is never compared", func(t *testing.T) {
		eu := &mockTernClient{compareResp: &ternv1.CompareSchemaFileChangeResponse{Same: true}}
		same, _, err := twoDeploymentService(t, eu, &mockTernClient{}).CompareSchemaFileChange(t.Context(),
			"unknown", "production", "approved base", "approved", "head base", "head")
		require.ErrorContains(t, err, `resolve target for database "unknown"`)
		assert.False(t, same)
		assert.Empty(t, eu.compareReqs)
	})

	t.Run("the comparison is bounded by the Service's deadline", func(t *testing.T) {
		eu := &mockTernClient{compareResp: &ternv1.CompareSchemaFileChangeResponse{Same: true}}
		before := time.Now()
		_, _, err := compare(t.Context(), twoDeploymentService(t, eu, &mockTernClient{}))
		require.NoError(t, err)
		require.Len(t, eu.compareDeadlines, 1)
		deadline := eu.compareDeadlines[0]
		assert.False(t, deadline.Before(before.Add(schemaFileComparisonTimeout)), "deadline %s is earlier than the comparison timeout", deadline)
		assert.False(t, deadline.After(time.Now().Add(schemaFileComparisonTimeout)), "deadline %s is later than the comparison timeout", deadline)
	})

	t.Run("a caller's shorter deadline is kept", func(t *testing.T) {
		eu := &mockTernClient{compareResp: &ternv1.CompareSchemaFileChangeResponse{Same: true}}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		callerDeadline, _ := ctx.Deadline()
		_, _, err := compare(ctx, twoDeploymentService(t, eu, &mockTernClient{}))
		require.NoError(t, err)
		require.Len(t, eu.compareDeadlines, 1)
		assert.Equal(t, callerDeadline, eu.compareDeadlines[0])
	})
}
