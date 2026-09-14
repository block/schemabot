//go:build integration

package postgres

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/targetauth"
	"github.com/block/schemabot/pkg/testutil"
)

// targetAuthCase describes one way a PostgreSQL target can refuse a
// connection for a reason the operator has to fix in the target's
// credentials, and the classification the engine must attach to it.
type targetAuthCase struct {
	name   string
	mutate func(u *url.URL)
	want   targetauth.Classification
}

func targetAuthCases() []targetAuthCase {
	return []targetAuthCase{
		{
			name:   "wrong password",
			mutate: func(u *url.URL) { u.User = url.UserPassword(u.User.Username(), "wrong") },
			want:   targetauth.AuthInvalidCredentials,
		},
		{
			name:   "missing database",
			mutate: func(u *url.URL) { u.Path = "/target_auth_missing_db" },
			want:   targetauth.AuthNoDatabase,
		},
	}
}

// dsnWith rewrites one part of a URL-form PostgreSQL DSN.
func dsnWith(t *testing.T, dsn string, mutate func(u *url.URL)) string {
	t.Helper()
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	mutate(u)
	return u.String()
}

func assertTargetAuthClassification(t *testing.T, err error, want targetauth.Classification) {
	t.Helper()
	require.Error(t, err)
	classification, ok := targetauth.ClassificationOf(err)
	assert.True(t, ok, "error should carry a target auth classification: %v", err)
	assert.Equal(t, want, classification)
}

// Planning against a PostgreSQL target that rejects the configured
// credentials or names a database that does not exist fails with an error
// that still carries its authentication classification after the engine
// wraps it, so a caller can tell a credential problem apart from a transient
// connection failure without parsing the driver's message.
func TestEnginePlanClassifiesTargetAuthFailure(t *testing.T) {
	dsn, _ := testutil.StartPostgres(t, "plan_target_auth_test")
	for _, tc := range targetAuthCases() {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New().Plan(t.Context(), &engine.PlanRequest{
				Database: "plan_target_auth_test",
				SchemaFiles: schema.SchemaFiles{
					"public": {Files: map[string]string{
						"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY)",
					}},
				},
				Credentials: &engine.Credentials{DSN: dsnWith(t, dsn, tc.mutate)},
			})
			assertTargetAuthClassification(t, err, tc.want)
		})
	}
}

// A schema pull from a PostgreSQL target that rejects the configured
// credentials or names a database that does not exist fails with an error
// that still carries its authentication classification after the engine
// wraps it.
func TestEnginePullSchemaClassifiesTargetAuthFailure(t *testing.T) {
	dsn, _ := testutil.StartPostgres(t, "pull_target_auth_test")
	for _, tc := range targetAuthCases() {
		t.Run(tc.name, func(t *testing.T) {
			eng := NewForTarget(0, 0, "pull_target_auth_test", &engine.Credentials{DSN: dsnWith(t, dsn, tc.mutate)})
			_, err := eng.PullSchema(t.Context(), &ternv1.PullSchemaRequest{Namespace: "public"})
			assertTargetAuthClassification(t, err, tc.want)
		})
	}
}
