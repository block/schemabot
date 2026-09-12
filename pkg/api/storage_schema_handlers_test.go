package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/auth"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/tern"
)

// fakeStorageSchemaService answers the storage schema RPCs for one instance's
// storage, capturing what it was asked so a test can assert the request the
// control plane forwarded rather than only the response it rendered.
type fakeStorageSchemaService struct {
	planReq  *ternv1.StorageSchemaPlanRequest
	planResp *ternv1.StorageSchemaPlanResponse
	planErr  error

	applyReq  *ternv1.StorageSchemaApplyRequest
	applyResp *ternv1.StorageSchemaApplyResponse
	applyErr  error
}

func (f *fakeStorageSchemaService) StorageSchemaPlan(_ context.Context, req *ternv1.StorageSchemaPlanRequest) (*ternv1.StorageSchemaPlanResponse, error) {
	f.planReq = req
	return f.planResp, f.planErr
}

func (f *fakeStorageSchemaService) StorageSchemaApply(_ context.Context, req *ternv1.StorageSchemaApplyRequest) (*ternv1.StorageSchemaApplyResponse, error) {
	f.applyReq = req
	return f.applyResp, f.applyErr
}

// storageSchemaTernClient is a tern client that also answers the storage schema
// RPCs, the way a GRPCClient to a data plane does.
type storageSchemaTernClient struct {
	*mockTernClient
	*fakeStorageSchemaService
}

var (
	_ tern.StorageSchemaService = (*fakeStorageSchemaService)(nil)
	_ tern.Client               = (*storageSchemaTernClient)(nil)
	_ tern.StorageSchemaService = (*storageSchemaTernClient)(nil)
)

func storageSchemaReportMessage(database string, outstanding ...string) *ternv1.StorageSchemaReport {
	report := &ternv1.StorageSchemaReport{
		Dialect:  string(schema.DialectMySQL),
		Database: database,
		Version:  "v0.1.0",
	}
	for _, table := range outstanding {
		report.Outstanding = append(report.Outstanding, &ternv1.StorageSchemaStatement{
			Table:     table,
			Operation: storageSchemaOpAlterTable,
			Ddl:       "ALTER TABLE `" + table + "` ADD COLUMN `superseded_by` varchar(255) NOT NULL DEFAULT ''",
		})
	}
	return report
}

// storageSchemaRoutingConfig configures one data plane endpoint, so a request
// naming it routes rather than being refused as unconfigured.
func storageSchemaRoutingConfig() *ServerConfig {
	return &ServerConfig{
		TernDeployments: TernConfig{
			"west": TernEndpoints{"production": "west.example:9090"},
		},
	}
}

func newStorageSchemaService(t *testing.T, cfg *ServerConfig) *Service {
	t.Helper()
	return New(nil, cfg, nil, slog.New(slog.DiscardHandler))
}

func storageSchemaPlanRequestFor(t *testing.T, svc *Service, body string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/storage/schema/plan", strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func storageSchemaApplyRequest(t *testing.T, svc *Service, body string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/storage/schema/apply", strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func decodePlanResponse(t *testing.T, rec *httptest.ResponseRecorder) apitypes.StorageSchemaPlanResponse {
	t.Helper()
	var response apitypes.StorageSchemaPlanResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response), "body: %s", rec.Body.String())
	require.NotNil(t, response.Report)
	return response
}

// A request that names no deployment reads the storage of the server it was
// made to, and the report says which database that was.
func TestHandleStorageSchemaPlan_ReadsThisServersStorage(t *testing.T) {
	svc := newStorageSchemaService(t, &ServerConfig{})
	local := &fakeStorageSchemaService{
		planResp: &ternv1.StorageSchemaPlanResponse{Report: storageSchemaReportMessage("schemabot_storage", "applies")},
	}
	svc.SetStorageSchemaService(local)

	rec := storageSchemaPlanRequestFor(t, svc, "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	report := decodePlanResponse(t, rec).Report
	assert.Empty(t, report.Deployment, "this server's own storage is not a deployment")
	assert.Empty(t, report.Environment)
	assert.Equal(t, "schemabot_storage", report.Database)
	assert.Equal(t, string(schema.DialectMySQL), report.Dialect)
	assert.False(t, report.Converged)
	require.Len(t, report.Outstanding, 1)
	assert.Equal(t, "applies", report.Outstanding[0].Table)
	assert.Contains(t, report.Outstanding[0].DDL, "ADD COLUMN")
	assert.False(t, local.planReq.GetAllowDestructive(), "the default must not opt into destructive statements")
}

// A converged storage database reports converged, which is the answer a
// pre-deploy check is looking for.
func TestHandleStorageSchemaPlan_ReportsConverged(t *testing.T) {
	svc := newStorageSchemaService(t, &ServerConfig{})
	svc.SetStorageSchemaService(&fakeStorageSchemaService{
		planResp: &ternv1.StorageSchemaPlanResponse{Report: storageSchemaReportMessage("schemabot_storage")},
	})

	rec := storageSchemaPlanRequestFor(t, svc, "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	report := decodePlanResponse(t, rec).Report
	assert.True(t, report.Converged)
	assert.Empty(t, report.Outstanding)
}

// allow_destructive reaches the instance that answers, so the report says what
// an apply with the same flag would do rather than what the default would.
func TestHandleStorageSchemaPlan_ForwardsAllowDestructive(t *testing.T) {
	svc := newStorageSchemaService(t, &ServerConfig{})
	local := &fakeStorageSchemaService{
		planResp: &ternv1.StorageSchemaPlanResponse{Report: storageSchemaReportMessage("schemabot_storage")},
	}
	svc.SetStorageSchemaService(local)

	rec := storageSchemaPlanRequestFor(t, svc, `{"allow_destructive":true}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.True(t, local.planReq.GetAllowDestructive())
}

// A server with no storage schema service refuses rather than guessing at a
// storage DSN: a guess reads the wrong database and reports it as the right
// one.
func TestHandleStorageSchemaPlan_RefusesWithoutLocalService(t *testing.T) {
	svc := newStorageSchemaService(t, &ServerConfig{})

	rec := storageSchemaPlanRequestFor(t, svc, "")
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "does not expose its own storage schema")
	assert.Contains(t, rec.Body.String(), "name a deployment")
}

// A named deployment is read through its own endpoint, and the report is
// stamped with the deployment the operator asked about — not only with whatever
// database answered.
func TestHandleStorageSchemaPlan_ReadsDataPlaneStorage(t *testing.T) {
	svc := newStorageSchemaService(t, storageSchemaRoutingConfig())
	svc.SetStorageSchemaService(&fakeStorageSchemaService{
		planResp: &ternv1.StorageSchemaPlanResponse{Report: storageSchemaReportMessage("control_plane_storage")},
	})
	remote := &fakeStorageSchemaService{
		planResp: &ternv1.StorageSchemaPlanResponse{Report: storageSchemaReportMessage("west_storage", "apply_operations")},
	}
	svc.RegisterTernClient("west", "production", &storageSchemaTernClient{
		mockTernClient:           &mockTernClient{isRemote: true},
		fakeStorageSchemaService: remote,
	})

	rec := storageSchemaPlanRequestFor(t, svc, `{"deployment":"west","environment":"production"}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	report := decodePlanResponse(t, rec).Report
	assert.Equal(t, "west", report.Deployment)
	assert.Equal(t, "production", report.Environment)
	assert.Equal(t, "west_storage", report.Database, "the data plane's storage, not the control plane's")
	require.Len(t, report.Outstanding, 1)
	assert.Equal(t, "apply_operations", report.Outstanding[0].Table)
	assert.NotNil(t, remote.planReq, "the data plane must be the one asked")
}

// A deployment with no configured endpoint is an error naming what was looked
// under. It never falls back to the storage of the server that took the
// request: reporting a converged control plane to an operator asking about an
// unreachable data plane is a wrong answer that reads like a right one.
func TestHandleStorageSchemaPlan_UnconfiguredDeploymentDoesNotFallBack(t *testing.T) {
	svc := newStorageSchemaService(t, storageSchemaRoutingConfig())
	local := &fakeStorageSchemaService{
		planResp: &ternv1.StorageSchemaPlanResponse{Report: storageSchemaReportMessage("control_plane_storage")},
	}
	svc.SetStorageSchemaService(local)

	rec := storageSchemaPlanRequestFor(t, svc, `{"deployment":"east","environment":"production"}`)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "no data plane configured for deployment")
	assert.Contains(t, body, "east")
	assert.Contains(t, body, "production")
	assert.Contains(t, body, "tern_deployments.east.production")
	assert.Nil(t, local.planReq, "the control plane's own storage must not answer for a data plane")
}

// A deployment whose endpoint is configured but whose client resolves
// in-process is a routing ambiguity, not a local read. Saying so beats
// reporting this server's storage under the deployment's name.
func TestHandleStorageSchemaPlan_RefusesNonRoutableDeployment(t *testing.T) {
	svc := newStorageSchemaService(t, storageSchemaRoutingConfig())
	svc.SetStorageSchemaService(&fakeStorageSchemaService{
		planResp: &ternv1.StorageSchemaPlanResponse{Report: storageSchemaReportMessage("control_plane_storage")},
	})
	svc.RegisterTernClient("west", "production", &mockTernClient{})

	rec := storageSchemaPlanRequestFor(t, svc, `{"deployment":"west","environment":"production"}`)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "in-process client")
	assert.Contains(t, rec.Body.String(), "west")
}

// Naming half a target is refused with the half that is missing, on both
// halves: a deployment with no environment has no single endpoint, and an
// environment with no deployment selects nothing.
func TestHandleStorageSchemaPlan_RefusesHalfNamedTarget(t *testing.T) {
	svc := newStorageSchemaService(t, storageSchemaRoutingConfig())
	svc.SetStorageSchemaService(&fakeStorageSchemaService{
		planResp: &ternv1.StorageSchemaPlanResponse{Report: storageSchemaReportMessage("control_plane_storage")},
	})

	deploymentOnly := storageSchemaPlanRequestFor(t, svc, `{"deployment":"west"}`)
	require.Equal(t, http.StatusBadRequest, deploymentOnly.Code)
	assert.Contains(t, deploymentOnly.Body.String(), "needs an environment")

	environmentOnly := storageSchemaPlanRequestFor(t, svc, `{"environment":"production"}`)
	require.Equal(t, http.StatusBadRequest, environmentOnly.Code)
	assert.Contains(t, environmentOnly.Body.String(), "without a deployment")
}

// A convergence returns both halves — what was outstanding and what is left —
// and attributes the run to the caller, so the target's own logs name a person.
func TestHandleStorageSchemaApply_ConvergesThisServersStorage(t *testing.T) {
	svc := newStorageSchemaService(t, &ServerConfig{})
	local := &fakeStorageSchemaService{
		applyResp: &ternv1.StorageSchemaApplyResponse{
			Planned:   storageSchemaReportMessage("schemabot_storage", "applies"),
			Remaining: storageSchemaReportMessage("schemabot_storage"),
		},
	}
	svc.SetStorageSchemaService(local)

	rec := storageSchemaApplyRequest(t, svc, `{"caller":"cli:operator@workstation"}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var response apitypes.StorageSchemaApplyResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.NotNil(t, response.Planned)
	require.NotNil(t, response.Remaining)
	assert.Len(t, response.Planned.Outstanding, 1)
	assert.True(t, response.Remaining.Converged)
	assert.Equal(t, "cli:operator@workstation", local.applyReq.GetCaller())
	assert.False(t, local.applyReq.GetAllowDestructive())
}

// An empty body converges the storage of the server the request was made to,
// with destructive statements refused: every field is optional and the defaults
// are the safe ones.
func TestHandleStorageSchemaApply_EmptyBodyUsesSafeDefaults(t *testing.T) {
	svc := newStorageSchemaService(t, &ServerConfig{})
	local := &fakeStorageSchemaService{
		applyResp: &ternv1.StorageSchemaApplyResponse{
			Planned:   storageSchemaReportMessage("schemabot_storage"),
			Remaining: storageSchemaReportMessage("schemabot_storage"),
		},
	}
	svc.SetStorageSchemaService(local)

	rec := storageSchemaApplyRequest(t, svc, "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.NotNil(t, local.applyReq)
	assert.False(t, local.applyReq.GetAllowDestructive())
}

// A misspelled field is refused rather than ignored: a dropped "deployment"
// would converge the wrong storage database.
func TestHandleStorageSchemaApply_RefusesUnknownField(t *testing.T) {
	svc := newStorageSchemaService(t, &ServerConfig{})
	local := &fakeStorageSchemaService{}
	svc.SetStorageSchemaService(local)

	rec := storageSchemaApplyRequest(t, svc, `{"deploymnet":"west"}`)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Nil(t, local.applyReq, "nothing may converge on a request that was not understood")
}

// An incomplete answer from the target is an error, not a convergence: without
// both halves there is no way to tell a run that finished from one that left
// statements behind.
func TestHandleStorageSchemaApply_RefusesIncompleteResult(t *testing.T) {
	svc := newStorageSchemaService(t, &ServerConfig{})
	svc.SetStorageSchemaService(&fakeStorageSchemaService{
		applyResp: &ternv1.StorageSchemaApplyResponse{
			Planned: storageSchemaReportMessage("schemabot_storage", "applies"),
		},
	})

	rec := storageSchemaApplyRequest(t, svc, `{}`)
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "incomplete result")
}

// A failure is answered with the status that says whose problem it is. A
// caller who sent an unreadable schema file gets 400 and the reason, because
// they wrote the input and can correct it; anything else is 500 naming only
// where to look, because the cause is the answering instance's own — a DSN, a
// host, a driver error — and none of it goes back over this route.
//
// Both spellings of the caller's fault are recognized, since the same handler
// serves an adapter in this process and one reached over gRPC: in-process the
// sentinel arrives intact, and over the wire it has already become an
// InvalidArgument status.
func TestStorageSchemaRoutes_FailureStatusSaysWhoseProblemItIs(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		wantCode    int
		wantBody    string
		wantNotBody string
	}{
		{
			name:     "in-process caller error",
			err:      fmt.Errorf("%w: read the supplied storage schema: applies.sql is not DDL", tern.ErrInvalidStorageSchemaRequest),
			wantCode: http.StatusBadRequest,
			wantBody: "applies.sql is not DDL",
		},
		{
			name:     "caller error over gRPC",
			err:      status.Error(codes.InvalidArgument, "read the supplied storage schema: applies.sql is not DDL"),
			wantCode: http.StatusBadRequest,
			wantBody: "applies.sql is not DDL",
		},
		{
			name:        "the answering instance's own failure",
			err:         errors.New("dial postgres://schemabot:hunter2@db.example:5432/schemabot: connection refused"),
			wantCode:    http.StatusInternalServerError,
			wantBody:    "see the answering deployment's logs",
			wantNotBody: "db.example",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc := newStorageSchemaService(t, &ServerConfig{})
			svc.SetStorageSchemaService(&fakeStorageSchemaService{planErr: tc.err, applyErr: tc.err})

			for route, rec := range map[string]*httptest.ResponseRecorder{
				"plan":  storageSchemaPlanRequestFor(t, svc, `{}`),
				"apply": storageSchemaApplyRequest(t, svc, `{}`),
			} {
				assert.Equal(t, tc.wantCode, rec.Code, "%s: %s", route, rec.Body.String())
				assert.Contains(t, rec.Body.String(), tc.wantBody, route)
				if tc.wantNotBody != "" {
					assert.NotContains(t, rec.Body.String(), tc.wantNotBody, route)
				}
			}
		})
	}
}

// Both routes are admin-only, and the read-only diff is no exception. A scoped
// database operator is denied on it even though it is a GET, because the tier
// rule admits it as a write — the storage database is SchemaBot's own
// bookkeeping, not any team's database, so there is nothing for a per-database
// grant to scope to.
func TestStorageSchemaRoutes_DenyScopedOperator(t *testing.T) {
	svc := newStorageSchemaService(t, scopedWriteConfig())
	local := &fakeStorageSchemaService{
		planResp: &ternv1.StorageSchemaPlanResponse{Report: storageSchemaReportMessage("schemabot_storage")},
		applyResp: &ternv1.StorageSchemaApplyResponse{
			Planned:   storageSchemaReportMessage("schemabot_storage"),
			Remaining: storageSchemaReportMessage("schemabot_storage"),
		},
	}
	svc.SetStorageSchemaService(local)

	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	operator := auth.WithUser(t.Context(), &auth.User{Subject: "bob", Groups: []string{"payments-team"}})

	for _, route := range []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPost, "/api/storage/schema/plan", `{}`},
		{http.MethodPost, "/api/storage/schema/apply", `{}`},
	} {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			req := httptest.NewRequestWithContext(operator, route.method, route.path, strings.NewReader(route.body))
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			assert.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), "write group")
		})
	}
	assert.Nil(t, local.planReq, "a denied request must not read the storage database")
	assert.Nil(t, local.applyReq, "a denied request must not converge the storage database")
}

// An admin is allowed on both routes under the same configuration that denies
// the scoped operator, so the denial above is the grant working rather than the
// routes being closed to everyone.
func TestStorageSchemaRoutes_AllowAdmin(t *testing.T) {
	svc := newStorageSchemaService(t, scopedWriteConfig())
	svc.SetStorageSchemaService(&fakeStorageSchemaService{
		planResp: &ternv1.StorageSchemaPlanResponse{Report: storageSchemaReportMessage("schemabot_storage")},
	})

	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	admin := auth.WithUser(t.Context(), &auth.User{Subject: "alice", Groups: []string{"schema-admins"}})
	req := httptest.NewRequestWithContext(admin, http.MethodPost, "/api/storage/schema/plan", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.True(t, decodePlanResponse(t, rec).Report.Converged)
}

// A half-supplied desired schema is refused. Files with no source produce a
// report that cannot say what it was compared against; a source with no files
// would label this server's own embedded schema with another release's name.
func TestValidateStorageSchemaPlanRequest(t *testing.T) {
	require.NoError(t, validateStorageSchemaPlanRequest(apitypes.StorageSchemaPlanRequest{}))
	require.NoError(t, validateStorageSchemaPlanRequest(apitypes.StorageSchemaPlanRequest{
		SchemaFiles:  validStorageSchemaFiles(),
		SchemaSource: "the schema files of release v1.4.0",
	}))

	err := validateStorageSchemaPlanRequest(apitypes.StorageSchemaPlanRequest{SchemaFiles: validStorageSchemaFiles()})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "without schema_source")

	err = validateStorageSchemaPlanRequest(apitypes.StorageSchemaPlanRequest{SchemaSource: "the schema files of release v1.4.0"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "without schema_files")
}
