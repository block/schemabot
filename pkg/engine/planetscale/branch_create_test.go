package planetscale

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/psclient"
)

const branchNameTakenMessage = "Name has already been taken"

// createBranchErrorFromAPI returns the error the PlanetScale SDK builds from a
// branch create the API answered with status and body. Its message cannot be
// set on a hand-built *ps.Error, so the test goes through the real SDK parser.
func createBranchErrorFromAPI(t *testing.T, status int, body string) error {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)

	client, err := psclient.NewPSClientWithBaseURL("token-name", "token-value", srv.URL)
	require.NoError(t, err)
	_, err = client.CreateBranch(t.Context(), &ps.CreateDatabaseBranchRequest{
		Organization: "org", Database: "commerce", Name: "schemabot-commerce-1a2b", ParentBranch: "main",
	})
	require.Error(t, err)
	return err
}

// createBranchClient serves a parent branch with safe schema changes on, fails
// every branch create with createErr, and serves existing as the branch that
// already carries the name. Creates and reads of the named branch are counted.
type createBranchClient struct {
	psclient.PSClient
	createErr   error
	existing    *ps.DatabaseBranch
	createCalls int
	getCalls    int
}

func (c *createBranchClient) GetBranch(_ context.Context, req *ps.GetDatabaseBranchRequest) (*ps.DatabaseBranch, error) {
	if req.Branch == "main" {
		return &ps.DatabaseBranch{Name: "main", SafeMigrations: true, Region: ps.Region{Slug: "us-east"}}, nil
	}
	c.getCalls++
	return c.existing, nil
}

func (c *createBranchClient) CreateBranch(context.Context, *ps.CreateDatabaseBranchRequest) (*ps.DatabaseBranch, error) {
	c.createCalls++
	return nil, c.createErr
}

// A branch create that PlanetScale refuses because the name is taken means an
// earlier attempt already made the branch, so the existing branch is read and
// reused instead of failing the apply. The SDK does not keep the HTTP status,
// so the refusal counts whether the API answers it as an unprocessable request,
// a bad request, or a conflict, which the SDK leaves without a code.
func TestCreateBranch_NameTakenReusesTheExistingBranch(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{name: "an unprocessable request", status: http.StatusUnprocessableEntity, body: `{"code":"unprocessable","message":"Name has already been taken"}`},
		{name: "a bad request", status: http.StatusBadRequest, body: `{"code":"bad_request","message":"Name has already been taken"}`},
		{name: "a conflict", status: http.StatusConflict, body: `{"code":"conflict","message":"Name has already been taken"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
			existing := &ps.DatabaseBranch{Name: "schemabot-commerce-1a2b", Ready: true}
			client := &createBranchClient{
				createErr: createBranchErrorFromAPI(t, tt.status, tt.body),
				existing:  existing,
			}

			branch, err := e.createBranch(t.Context(), client, "org", "commerce", "schemabot-commerce-1a2b", "main")

			require.NoError(t, err)
			assert.Same(t, existing, branch)
			assert.Equal(t, 1, client.createCalls)
			assert.Equal(t, 1, client.getCalls)
		})
	}
}

// Only an API refusal that says the name is taken proves the branch exists.
// The phrase inside an error that is not an answer from the API, inside a body
// that is not an API error, or inside a permission or not found answer is not
// that proof, and a refusal with a different message says nothing about the
// name. Each fails the create with the original error and never reads or
// reuses a branch.
func TestCreateBranch_OtherFailuresAreReturnedAndNeverReuseABranch(t *testing.T) {
	tests := []struct {
		name      string
		createErr error
	}{
		{name: "a transport failure whose text has the phrase", createErr: errors.New("read tcp: connection reset: " + branchNameTakenMessage)},
		{name: "a wrapped failure that is not from the API", createErr: errors.New("proxy rejected the request: " + branchNameTakenMessage)},
		{name: "a body that is not an API error", createErr: createBranchErrorFromAPI(t, http.StatusBadGateway, `{"error":"Name has already been taken"}`)},
		{name: "a body that is not JSON", createErr: createBranchErrorFromAPI(t, http.StatusBadGateway, `<html>Name has already been taken</html>`)},
		{name: "a permission refusal with the phrase", createErr: createBranchErrorFromAPI(t, http.StatusForbidden, `{"code":"unauthorized","message":"Name has already been taken"}`)},
		{name: "a not found answer with the phrase", createErr: createBranchErrorFromAPI(t, http.StatusNotFound, `{"code":"not_found","message":"Name has already been taken"}`)},
		{name: "an unprocessable request with a different message", createErr: createBranchErrorFromAPI(t, http.StatusUnprocessableEntity, `{"code":"unprocessable","message":"Branch limit reached"}`)},
		{name: "a bad request with a different message", createErr: createBranchErrorFromAPI(t, http.StatusBadRequest, `{"code":"bad_request","message":"Parent branch is not ready"}`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
			client := &createBranchClient{
				createErr: tt.createErr,
				existing:  &ps.DatabaseBranch{Name: "schemabot-commerce-1a2b", Ready: true},
			}

			branch, err := e.createBranch(t.Context(), client, "org", "commerce", "schemabot-commerce-1a2b", "main")

			require.Error(t, err)
			assert.Nil(t, branch)
			assert.ErrorIs(t, err, tt.createErr)
			assert.Contains(t, err.Error(), "create branch schemabot-commerce-1a2b: ")
			assert.Equal(t, 1, client.createCalls)
			assert.Zero(t, client.getCalls)
		})
	}
}
