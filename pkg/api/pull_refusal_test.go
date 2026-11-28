package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// Every 400 from POST /api/pull carries error_code invalid_request, so a client
// can tell a request it must correct from a pull that failed on the server by
// matching the code alone, whichever refusal it hit. The message each refusal
// carries is part of the contract too: it tells the caller what to change.
func TestPullSchemaHandlerAnswersEveryRefusalAsInvalidRequest(t *testing.T) {
	appScoped := func(t *testing.T) *Service {
		return newAppScopedPullService(t, map[string]string{"orders": "commerce"}, &mockTernClient{})
	}
	typed := func(databaseType string, client *mockTernClient) func(t *testing.T) *Service {
		return func(t *testing.T) *Service {
			return newTypeVocabularyService(t, &mockStorageWithApplyStores{
				plans:   &staticPlanStore{},
				applies: &staticApplyStore{},
			}, client, "orders", databaseType, "primary")
		}
	}
	tests := []struct {
		name      string
		service   func(t *testing.T) *Service
		body      string
		wantError string
	}{
		{
			name:      "unknown request field",
			service:   appScoped,
			body:      `{"database":"orders","environment":"production","env":"production"}`,
			wantError: `invalid request body: json: unknown field "env"`,
		},
		{
			name:      "database and app both set",
			service:   appScoped,
			body:      `{"database":"orders","app":"commerce","environment":"production"}`,
			wantError: "database and app both select the database; set exactly one",
		},
		{
			name:      "neither database nor app set",
			service:   appScoped,
			body:      `{"environment":"production"}`,
			wantError: "database or app is required",
		},
		{
			name:      "no environment",
			service:   appScoped,
			body:      `{"database":"orders"}`,
			wantError: "environment is required",
		},
		{
			name:      "unknown catalog detail",
			service:   appScoped,
			body:      `{"database":"orders","environment":"production","catalog_detail":"full"}`,
			wantError: `catalog_detail "full" must be basic or detailed`,
		},
		{
			name:      "app no database declares",
			service:   appScoped,
			body:      `{"app":"nosuch","environment":"production"}`,
			wantError: `app "nosuch" has no configured databases`,
		},
		{
			name:      "type disagrees with server config",
			service:   typed("cockroach", &mockTernClient{}),
			body:      `{"database":"orders","environment":"production","type":"mysql"}`,
			wantError: `database "orders" type "mysql" does not match server config type "cockroach"`,
		},
		{
			name:      "environment not configured",
			service:   typed(storage.DatabaseTypeMySQL, &mockTernClient{}),
			body:      `{"database":"orders","environment":"sandbox"}`,
			wantError: `resolve target for orders/sandbox: database "orders" environment "sandbox" is not configured on this server`,
		},
		{
			name: "namespace no targets entry selects",
			service: func(t *testing.T) *Service {
				return namespaceSelectionService(t, &mockTernClient{isRemote: true}, &capturingPlanStore{})
			},
			body:      `{"database":"orders","environment":"production","namespaces":["ns_l"]}`,
			wantError: `database "orders" environment "production" has no targets entry selecting namespaces [ns_l]; the selectable namespaces are [ns_0, ns_1, ns_2]`,
		},
		{
			name:      "namespace that is a schema directory placeholder",
			service:   typed(storage.DatabaseTypeMySQL, &mockTernClient{}),
			body:      `{"database":"orders","environment":"production","namespaces":["{env}"]}`,
			wantError: `pull schema failed: pull namespace "{env}" must be a concrete live namespace; resolve {env} or $ENV before calling pull`,
		},
		{
			name:      "lint on a dialect without a schema audit",
			service:   typed("cockroach", &mockTernClient{}),
			body:      `{"database":"orders","environment":"production","lint":true}`,
			wantError: "schema linting on pull is not supported for cockroach databases",
		},
		{
			name: "lint on a pulled table that is not a CREATE TABLE",
			service: typed(storage.DatabaseTypeMySQL, &mockTernClient{
				pullSchemaResp: &ternv1.PullSchemaResponse{
					Database:    "orders",
					Type:        storage.DatabaseTypeMySQL,
					Environment: "production",
					Namespaces: map[string]*ternv1.PulledNamespace{
						"orders": {Tables: map[string]string{"users": "DROP TABLE `users`;\n"}},
					},
					TableCount: 1,
				},
			}),
			body:      `{"database":"orders","environment":"production","lint":true}`,
			wantError: `lint pulled schema for database "orders" namespace "orders": table "users" is a DROP TABLE statement, expected CREATE TABLE`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			tt.service(t).ConfigureRoutes(mux)

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/pull", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			var resp apitypes.ErrorResponse
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
			assert.Equal(t, apitypes.ErrCodeInvalidRequest, resp.ErrorCode)
			assert.Equal(t, tt.wantError, resp.Error)
		})
	}
}
