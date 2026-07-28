package api

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/auth"
	"github.com/block/schemabot/pkg/storage"
)

// scopedWriteConfig returns a valid server config with one scoped-write grant:
// the payments-team operator group may mutate the payments database, and the
// instance-wide operator environment policy allows staging only, while
// schema-admins hold the deployment-wide write groups.
func scopedWriteConfig() *ServerConfig {
	return &ServerConfig{
		Auth: AuthConfig{
			Type: "forward_auth",
			ForwardAuth: ForwardAuthSettings{
				TrustedProxyCIDRs:    []string{"127.0.0.1/32"},
				WriteGroups:          []string{"schema-admins"},
				OperatorEnvironments: []string{"staging"},
			},
		},
		Databases: map[string]DatabaseConfig{
			"payments": {
				Type: "mysql",
				Environments: map[string]EnvironmentConfig{
					"staging":    {DSN: "root@tcp(localhost)/payments"},
					"production": {DSN: "root@tcp(localhost)/payments_prod"},
				},
				OperatorGroups: []string{"payments-team"},
			},
			"orders": {
				Type: "mysql",
				Environments: map[string]EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost)/orders"},
				},
			},
		},
	}
}

func TestServerConfig_ValidateOperatorScoping(t *testing.T) {
	t.Run("valid scoped grant passes", func(t *testing.T) {
		require.NoError(t, scopedWriteConfig().Validate())
	})

	t.Run("neither grants nor environments set is a no-op", func(t *testing.T) {
		cfg := scopedWriteConfig()
		db := cfg.Databases["payments"]
		db.OperatorGroups = nil
		cfg.Databases["payments"] = db
		cfg.Auth.ForwardAuth.OperatorEnvironments = nil
		require.NoError(t, cfg.Validate())
	})

	t.Run("grants without the environment policy are rejected", func(t *testing.T) {
		cfg := scopedWriteConfig()
		cfg.Auth.ForwardAuth.OperatorEnvironments = nil
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "databases.payments.operator_groups requires at least one auth.forward_auth.operator_environments entry")
	})

	t.Run("environment policy without any grant is rejected", func(t *testing.T) {
		cfg := scopedWriteConfig()
		db := cfg.Databases["payments"]
		db.OperatorGroups = nil
		cfg.Databases["payments"] = db
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "auth.forward_auth.operator_environments requires at least one databases.*.operator_groups grant")
	})

	t.Run("whitespace-only entries count as empty", func(t *testing.T) {
		cfg := scopedWriteConfig()
		cfg.Auth.ForwardAuth.OperatorEnvironments = []string{"  "}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "operator_groups requires at least one auth.forward_auth.operator_environments entry")
	})

	t.Run("requires forward_auth", func(t *testing.T) {
		cfg := scopedWriteConfig()
		cfg.Auth.Type = "none"
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "requires auth type forward_auth")
	})

	t.Run("operator environment must exist on some database", func(t *testing.T) {
		cfg := scopedWriteConfig()
		cfg.Auth.ForwardAuth.OperatorEnvironments = []string{"staging", "sandbox"}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"sandbox" is not a configured environment on any database`)
	})

	t.Run("duplicate operator groups are rejected", func(t *testing.T) {
		cfg := scopedWriteConfig()
		db := cfg.Databases["payments"]
		db.OperatorGroups = []string{"payments-team", "payments-team"}
		cfg.Databases["payments"] = db
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate")
	})

	t.Run("duplicate operator environments are rejected", func(t *testing.T) {
		cfg := scopedWriteConfig()
		cfg.Auth.ForwardAuth.OperatorEnvironments = []string{"staging", "staging"}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate")
	})
}

func TestServerConfig_OperatorGroupUnion(t *testing.T) {
	cfg := scopedWriteConfig()
	orders := cfg.Databases["orders"]
	orders.OperatorGroups = []string{"orders-team", "payments-team"}
	cfg.Databases["orders"] = orders

	assert.Equal(t, []string{"orders-team", "payments-team"}, cfg.OperatorGroupUnion(),
		"union is deduplicated and sorted")

	assert.Empty(t, (&ServerConfig{}).OperatorGroupUnion(), "no databases means no operator groups")
}

func TestAuthorizeDirectWrite(t *testing.T) {
	cfg := scopedWriteConfig()
	admin := &auth.User{Subject: "alice", Groups: []string{"schema-admins"}}
	operator := &auth.User{Subject: "bob", Groups: []string{"payments-team"}}

	t.Run("disabled lane passes through", func(t *testing.T) {
		unscoped := scopedWriteConfig()
		db := unscoped.Databases["payments"]
		db.OperatorGroups = nil
		unscoped.Databases["payments"] = db
		unscoped.Auth.ForwardAuth.OperatorEnvironments = nil

		result := unscoped.AuthorizeDirectWrite(nil, "payments", "production")
		assert.True(t, result.Allowed)
		assert.Equal(t, DirectWriteReasonScopedLaneDisabled, result.Reason)
	})

	t.Run("missing identity is denied", func(t *testing.T) {
		result := cfg.AuthorizeDirectWrite(nil, "payments", "staging")
		assert.False(t, result.Allowed)
		assert.Equal(t, DirectWriteReasonMissingIdentity, result.Reason)
	})

	t.Run("admin is allowed on any database and environment", func(t *testing.T) {
		result := cfg.AuthorizeDirectWrite(admin, "orders", "staging")
		assert.True(t, result.Allowed)
		assert.Equal(t, DirectWriteReasonAdminAllow, result.Reason)
		assert.Equal(t, "schema-admins", result.MatchedPrincipal)
	})

	t.Run("operator is allowed on their database in a granted environment", func(t *testing.T) {
		result := cfg.AuthorizeDirectWrite(operator, "payments", "staging")
		assert.True(t, result.Allowed)
		assert.Equal(t, DirectWriteReasonScopedAllow, result.Reason)
		assert.Equal(t, "payments-team", result.MatchedPrincipal)
	})

	t.Run("operator group slug matches an org-qualified caller group", func(t *testing.T) {
		qualified := &auth.User{Subject: "bob", Groups: []string{"example-org/payments-team"}}
		result := cfg.AuthorizeDirectWrite(qualified, "payments", "staging")
		assert.True(t, result.Allowed)
		assert.Equal(t, "payments-team", result.MatchedPrincipal,
			"the configured name is attributed, not the caller's raw group")
	})

	t.Run("operator is denied outside their granted environments", func(t *testing.T) {
		result := cfg.AuthorizeDirectWrite(operator, "payments", "production")
		assert.False(t, result.Allowed)
		assert.Equal(t, DirectWriteReasonEnvironmentNotAllowed, result.Reason)
	})

	t.Run("operator is denied on a database without their grant", func(t *testing.T) {
		result := cfg.AuthorizeDirectWrite(operator, "orders", "staging")
		assert.False(t, result.Allowed)
		assert.Equal(t, DirectWriteReasonNotDatabaseOperator, result.Reason)
	})

	t.Run("unknown database is denied", func(t *testing.T) {
		result := cfg.AuthorizeDirectWrite(operator, "ghost", "staging")
		assert.False(t, result.Allowed)
		assert.Equal(t, DirectWriteReasonMissingDatabaseConfig, result.Reason)
	})
}

func TestAuthorizeDirectDatabaseWrite(t *testing.T) {
	cfg := scopedWriteConfig()
	operator := &auth.User{Subject: "bob", Groups: []string{"payments-team"}}

	t.Run("operator grant covers the database regardless of environment", func(t *testing.T) {
		result := cfg.AuthorizeDirectDatabaseWrite(operator, "payments")
		assert.True(t, result.Allowed)
		assert.Equal(t, DirectWriteReasonScopedAllow, result.Reason)
		assert.Equal(t, "payments-team", result.MatchedPrincipal)
	})

	t.Run("non-operator is denied", func(t *testing.T) {
		result := cfg.AuthorizeDirectDatabaseWrite(operator, "orders")
		assert.False(t, result.Allowed)
		assert.Equal(t, DirectWriteReasonNotDatabaseOperator, result.Reason)
	})

	t.Run("unknown database is denied", func(t *testing.T) {
		result := cfg.AuthorizeDirectDatabaseWrite(operator, "ghost")
		assert.False(t, result.Allowed)
		assert.Equal(t, DirectWriteReasonMissingDatabaseConfig, result.Reason)
	})
}

func TestAuthorizeDirectAdminWrite(t *testing.T) {
	cfg := scopedWriteConfig()

	t.Run("admin is allowed", func(t *testing.T) {
		result := cfg.AuthorizeDirectAdminWrite(&auth.User{Subject: "alice", Groups: []string{"schema-admins"}})
		assert.True(t, result.Allowed)
		assert.Equal(t, DirectWriteReasonAdminAllow, result.Reason)
	})

	t.Run("scoped operator is denied on database-less operations", func(t *testing.T) {
		result := cfg.AuthorizeDirectAdminWrite(&auth.User{Subject: "bob", Groups: []string{"payments-team"}})
		assert.False(t, result.Allowed)
		assert.Equal(t, DirectWriteReasonNotAdmin, result.Reason)
	})

	t.Run("disabled lane passes through", func(t *testing.T) {
		result := (&ServerConfig{}).AuthorizeDirectAdminWrite(nil)
		assert.True(t, result.Allowed)
		assert.Equal(t, DirectWriteReasonScopedLaneDisabled, result.Reason)
	})
}

// scopedDenialRequest sends the request to the handler as the given caller and
// returns the response, for asserting on handler-level scoped-write denials.
func scopedDenialRequest(t *testing.T, handler http.HandlerFunc, user *auth.User, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	ctx := auth.WithUser(t.Context(), user)
	req := httptest.NewRequestWithContext(ctx, method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler(rec, req)
	return rec
}

// A scoped operator's mutating requests are denied at the handler once the
// target database resolves outside their grant. The 403 names the principals
// that would grant access so a blocked caller knows who to ask.
func TestHandlersEnforceScopedWriteDenials(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	operator := &auth.User{Subject: "bob", Groups: []string{"payments-team"}}

	t.Run("plan denies an environment outside the grant", func(t *testing.T) {
		svc := New(&mockStorage{}, scopedWriteConfig(), nil, logger)
		rec := scopedDenialRequest(t, svc.handlePlan, operator, http.MethodPost, "/api/plan",
			`{"database":"payments","environment":"production","type":"mysql","schema_files":{"default":{"files":{"t.sql":"CREATE TABLE t (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)"}}}}`)

		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Contains(t, rec.Body.String(), "staging", "the denial lists the environments the grant allows")
		assert.Contains(t, rec.Body.String(), "schema-admins", "the denial names the groups that could act instead")
	})

	t.Run("apply resolves the database from the stored plan", func(t *testing.T) {
		st := &mockStorageWithPlanLookup{plans: &mockPlanLookupStore{plan: &storage.Plan{
			PlanIdentifier: "plan-1",
			Database:       "payments",
			Environment:    "production",
		}}}
		svc := New(st, scopedWriteConfig(), nil, logger)
		rec := scopedDenialRequest(t, svc.handleApply, operator, http.MethodPost, "/api/apply",
			`{"plan_id":"plan-1","environment":"production"}`)

		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Contains(t, rec.Body.String(), "payments", "the denial names the database resolved from the plan")
	})

	t.Run("control operation resolves the database from the stored apply", func(t *testing.T) {
		st := &mockStorageWithApplyStores{applies: &staticApplyStore{apply: &storage.Apply{
			ApplyIdentifier: "apply-1",
			Database:        "orders",
			Environment:     "staging",
		}}}
		svc := New(st, scopedWriteConfig(), nil, logger)
		rec := scopedDenialRequest(t, svc.handleStop, operator, http.MethodPost, "/api/stop",
			`{"apply_id":"apply-1","environment":"staging"}`)

		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Contains(t, rec.Body.String(), "orders", "the denial names the database resolved from the apply")
	})

	t.Run("lock acquire denies a database outside the grant", func(t *testing.T) {
		svc := New(&mockStorage{}, scopedWriteConfig(), nil, logger)
		rec := scopedDenialRequest(t, svc.handleLockAcquire, operator, http.MethodPost, "/api/locks/acquire",
			`{"database":"orders","database_type":"mysql","owner":"bob"}`)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("settings mutation stays admin-only for scoped operators", func(t *testing.T) {
		svc := New(&mockStorage{}, scopedWriteConfig(), nil, logger)
		rec := scopedDenialRequest(t, svc.handleSettingsSet, operator, http.MethodPost, "/api/settings",
			`{"key":"pause_applies","value":"true"}`)

		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Contains(t, rec.Body.String(), "schema-admins")
	})
}
