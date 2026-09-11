package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	diffReq  *ternv1.StorageSchemaDiffRequest
	diffResp *ternv1.StorageSchemaDiffResponse
	diffErr  error

	applyReq  *ternv1.StorageSchemaApplyRequest
	applyResp *ternv1.StorageSchemaApplyResponse
	applyErr  error
}

func (f *fakeStorageSchemaService) StorageSchemaDiff(_ context.Context, req *ternv1.StorageSchemaDiffRequest) (*ternv1.StorageSchemaDiffResponse, error) {
	f.diffReq = req
	return f.diffResp, f.diffErr
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

func storageSchemaDiffRequest(t *testing.T, svc *Service, body string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/storage/schema/diff", strings.NewReader(body))
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

func decodeDiffResponse(t *testing.T, rec *httptest.ResponseRecorder) apitypes.StorageSchemaDiffResponse {
	t.Helper()
	var response apitypes.StorageSchemaDiffResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response), "body: %s", rec.Body.String())
	require.NotNil(t, response.Report)
	return response
}

// A request that names no deployment reads the storage of the server it was
// made to, and the report says which database that was.
func TestHandleStorageSchemaDiff_ReadsThisServersStorage(t *testing.T) {
	svc := newStorageSchemaService(t, &ServerConfig{})
	local := &fakeStorageSchemaService{
		diffResp: &ternv1.StorageSchemaDiffResponse{Report: storageSchemaReportMessage("schemabot_storage", "applies")},
	}
	svc.SetStorageSchemaService(local)

	rec := storageSchemaDiffRequest(t, svc, "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	report := decodeDiffResponse(t, rec).Report
	assert.Empty(t, report.Deployment, "this server's own storage is not a deployment")
	assert.Empty(t, report.Environment)
	assert.Equal(t, "schemabot_storage", report.Database)
	assert.Equal(t, string(schema.DialectMySQL), report.Dialect)
	assert.False(t, report.Converged)
	require.Len(t, report.Outstanding, 1)
	assert.Equal(t, "applies", report.Outstanding[0].Table)
	assert.Contains(t, report.Outstanding[0].DDL, "ADD COLUMN")
	assert.False(t, local.diffReq.GetAllowDestructive(), "the default must not opt into destructive statements")
}

// A converged storage database reports converged, which is the answer a
// pre-deploy check is looking for.
func TestHandleStorageSchemaDiff_ReportsConverged(t *testing.T) {
	svc := newStorageSchemaService(t, &ServerConfig{})
	svc.SetStorageSchemaService(&fakeStorageSchemaService{
		diffResp: &ternv1.StorageSchemaDiffResponse{Report: storageSchemaReportMessage("schemabot_storage")},
	})

	rec := storageSchemaDiffRequest(t, svc, "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	report := decodeDiffResponse(t, rec).Report
	assert.True(t, report.Converged)
	assert.Empty(t, report.Outstanding)
}

// allow_destructive reaches the instance that answers, so the report says what
// an apply with the same flag would do rather than what the default would.
func TestHandleStorageSchemaDiff_ForwardsAllowDestructive(t *testing.T) {
	svc := newStorageSchemaService(t, &ServerConfig{})
	local := &fakeStorageSchemaService{
		diffResp: &ternv1.StorageSchemaDiffResponse{Report: storageSchemaReportMessage("schemabot_storage")},
	}
	svc.SetStorageSchemaService(local)

	rec := storageSchemaDiffRequest(t, svc, `{"allow_destructive":true}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.True(t, local.diffReq.GetAllowDestructive())
}

// A server with no storage schema service refuses rather than guessing at a
// storage DSN: a guess reads the wrong database and reports it as the right
// one.
func TestHandleStorageSchemaDiff_RefusesWithoutLocalService(t *testing.T) {
	svc := newStorageSchemaService(t, &ServerConfig{})

	rec := storageSchemaDiffRequest(t, svc, "")
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "does not expose its own storage schema")
	assert.Contains(t, rec.Body.String(), "name a deployment")
}

// A named deployment is read through its own endpoint, and the report is
// stamped with the deployment the operator asked about — not only with whatever
// database answered.
func TestHandleStorageSchemaDiff_ReadsDataPlaneStorage(t *testing.T) {
	svc := newStorageSchemaService(t, storageSchemaRoutingConfig())
	svc.SetStorageSchemaService(&fakeStorageSchemaService{
		diffResp: &ternv1.StorageSchemaDiffResponse{Report: storageSchemaReportMessage("control_plane_storage")},
	})
	remote := &fakeStorageSchemaService{
		diffResp: &ternv1.StorageSchemaDiffResponse{Report: storageSchemaReportMessage("west_storage", "apply_operations")},
	}
	svc.RegisterTernClient("west", "production", &storageSchemaTernClient{
		mockTernClient:           &mockTernClient{isRemote: true},
		fakeStorageSchemaService: remote,
	})

	rec := storageSchemaDiffRequest(t, svc, `{"deployment":"west","environment":"production"}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	report := decodeDiffResponse(t, rec).Report
	assert.Equal(t, "west", report.Deployment)
	assert.Equal(t, "production", report.Environment)
	assert.Equal(t, "west_storage", report.Database, "the data plane's storage, not the control plane's")
	require.Len(t, report.Outstanding, 1)
	assert.Equal(t, "apply_operations", report.Outstanding[0].Table)
	assert.NotNil(t, remote.diffReq, "the data plane must be the one asked")
}

// A deployment with no configured endpoint is an error naming what was looked
// under. It never falls back to the storage of the server that took the
// request: reporting a converged control plane to an operator asking about an
// unreachable data plane is a wrong answer that reads like a right one.
func TestHandleStorageSchemaDiff_UnconfiguredDeploymentDoesNotFallBack(t *testing.T) {
	svc := newStorageSchemaService(t, storageSchemaRoutingConfig())
	local := &fakeStorageSchemaService{
		diffResp: &ternv1.StorageSchemaDiffResponse{Report: storageSchemaReportMessage("control_plane_storage")},
	}
	svc.SetStorageSchemaService(local)

	rec := storageSchemaDiffRequest(t, svc, `{"deployment":"east","environment":"production"}`)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "no data plane configured for deployment")
	assert.Contains(t, body, "east")
	assert.Contains(t, body, "production")
	assert.Contains(t, body, "tern_deployments.east.production")
	assert.Nil(t, local.diffReq, "the control plane's own storage must not answer for a data plane")
}

// A deployment whose endpoint is configured but whose client resolves
// in-process is a routing ambiguity, not a local read. Saying so beats
// reporting this server's storage under the deployment's name.
func TestHandleStorageSchemaDiff_RefusesNonRoutableDeployment(t *testing.T) {
	svc := newStorageSchemaService(t, storageSchemaRoutingConfig())
	svc.SetStorageSchemaService(&fakeStorageSchemaService{
		diffResp: &ternv1.StorageSchemaDiffResponse{Report: storageSchemaReportMessage("control_plane_storage")},
	})
	svc.RegisterTernClient("west", "production", &mockTernClient{})

	rec := storageSchemaDiffRequest(t, svc, `{"deployment":"west","environment":"production"}`)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "in-process client")
	assert.Contains(t, rec.Body.String(), "west")
}

// Naming half a target is refused with the half that is missing, on both
// halves: a deployment with no environment has no single endpoint, and an
// environment with no deployment selects nothing.
func TestHandleStorageSchemaDiff_RefusesHalfNamedTarget(t *testing.T) {
	svc := newStorageSchemaService(t, storageSchemaRoutingConfig())
	svc.SetStorageSchemaService(&fakeStorageSchemaService{
		diffResp: &ternv1.StorageSchemaDiffResponse{Report: storageSchemaReportMessage("control_plane_storage")},
	})

	deploymentOnly := storageSchemaDiffRequest(t, svc, `{"deployment":"west"}`)
	require.Equal(t, http.StatusBadRequest, deploymentOnly.Code)
	assert.Contains(t, deploymentOnly.Body.String(), "needs an environment")

	environmentOnly := storageSchemaDiffRequest(t, svc, `{"environment":"production"}`)
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

// Both routes are admin-only, and the read-only diff is no exception. A scoped
// database operator is denied on it even though it is a GET, because the tier
// rule admits it as a write — the storage database is SchemaBot's own
// bookkeeping, not any team's database, so there is nothing for a per-database
// grant to scope to.
func TestStorageSchemaRoutes_DenyScopedOperator(t *testing.T) {
	svc := newStorageSchemaService(t, scopedWriteConfig())
	local := &fakeStorageSchemaService{
		diffResp: &ternv1.StorageSchemaDiffResponse{Report: storageSchemaReportMessage("schemabot_storage")},
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
		{http.MethodPost, "/api/storage/schema/diff", `{}`},
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
	assert.Nil(t, local.diffReq, "a denied request must not read the storage database")
	assert.Nil(t, local.applyReq, "a denied request must not converge the storage database")
}

// An admin is allowed on both routes under the same configuration that denies
// the scoped operator, so the denial above is the grant working rather than the
// routes being closed to everyone.
func TestStorageSchemaRoutes_AllowAdmin(t *testing.T) {
	svc := newStorageSchemaService(t, scopedWriteConfig())
	svc.SetStorageSchemaService(&fakeStorageSchemaService{
		diffResp: &ternv1.StorageSchemaDiffResponse{Report: storageSchemaReportMessage("schemabot_storage")},
	})

	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	admin := auth.WithUser(t.Context(), &auth.User{Subject: "alice", Groups: []string{"schema-admins"}})
	req := httptest.NewRequestWithContext(admin, http.MethodPost, "/api/storage/schema/diff", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.True(t, decodeDiffResponse(t, rec).Report.Converged)
}
