//go:build integration

package localscale

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/block/schemabot/pkg/testutil"
)

// newMetadataServer returns a Server backed by a fresh metadata database,
// opened as the server opens its own, so stored timestamps scan as the strings
// the handlers parse.
func newMetadataServer(t *testing.T) *Server {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), metadataOpTimeout)
	defer cancel()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testutil.MySQLContainerRequest("mysql:8.0", "localscale"),
		Started:          true,
	})
	testcontainers.CleanupContainer(t, container)
	require.NoError(t, err, "start metadata MySQL")
	dsn, err := testutil.MySQLDSN(ctx, container, "localscale")
	require.NoError(t, err)
	db, err := sql.Open(mysqlDriverName, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, testutil.PingMySQL(ctx, db))

	s := &Server{metadataDB: db, logger: quietLogger()}
	require.NoError(t, s.initMetadata(ctx), "create the metadata schema")
	return s
}

// listedDeployRequest is the part of a listed deploy request these tests read.
type listedDeployRequest struct {
	Number     uint64 `json:"number"`
	Branch     string `json:"branch"`
	IntoBranch string `json:"into_branch"`
	State      string `json:"state"`
	Deployment string `json:"deployment_state"`
}

// The deploy request listing honors the branch, into_branch and state filters
// and reports each deploy request's open-or-closed state, as PlanetScale's
// does: the PlanetScale engine lists the open deploy requests from a resumed
// branch into main to find one an earlier drive opened, and a listing that
// ignored the filters, or left the state out, would make it either adopt a
// closed or unrelated deploy request or find none and open a second one.
func TestHandleListDeployRequests_HonorsFiltersAndReportsState(t *testing.T) {
	s := newMetadataServer(t)
	const org, database = "org", "testdb"
	insert := func(number uint64, branch, intoBranch, deployState string, closed bool) {
		t.Helper()
		_, err := s.metadataDB.ExecContext(opContext(t),
			`INSERT INTO localscale_deploy_requests (number, org, database_name, branch, into_branch, deployment_state, closed_at)
			 VALUES (?, ?, ?, ?, ?, ?, IF(?, NOW(), NULL))`,
			number, org, database, branch, intoBranch, deployState, closed)
		require.NoError(t, err)
	}
	insert(1, "feature-a", "main", "ready", false)
	insert(2, "feature-a", "main", "ready", true)
	insert(3, "feature-a", "staging", "pending", false)
	insert(4, "feature-b", "main", "in_progress", false)
	insert(5, "feature-a", "main", "no_changes", false)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/organizations/{org}/databases/{db}/deploy-requests", s.handleError(s.handleListDeployRequests))
	list := func(query string) ([]listedDeployRequest, int) {
		t.Helper()
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/organizations/org/databases/testdb/deploy-requests"+query, nil))
		if rec.Code != http.StatusOK {
			t.Logf("GET %s: %d %s", query, rec.Code, rec.Body.String())
			return nil, rec.Code
		}
		var body struct {
			Data []listedDeployRequest `json:"data"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body), "body: %s", rec.Body.String())
		return body.Data, rec.Code
	}
	numbers := func(listed []listedDeployRequest) []uint64 {
		out := make([]uint64, 0, len(listed))
		for _, dr := range listed {
			out = append(out, dr.Number)
		}
		return out
	}

	t.Run("no filter lists every deploy request with its state", func(t *testing.T) {
		listed, code := list("")
		require.Equal(t, http.StatusOK, code)
		assert.Equal(t, []uint64{5, 4, 3, 2, 1}, numbers(listed))
		states := map[uint64]string{}
		for _, dr := range listed {
			states[dr.Number] = dr.State
		}
		assert.Equal(t, map[uint64]string{1: "open", 2: "closed", 3: "open", 4: "open", 5: "open"}, states)
	})

	t.Run("the open deploy requests from one branch into another", func(t *testing.T) {
		listed, code := list("?branch=feature-a&into_branch=main&state=open")
		require.Equal(t, http.StatusOK, code)
		assert.Equal(t, []uint64{5, 1}, numbers(listed))
		for _, dr := range listed {
			assert.Equal(t, "feature-a", dr.Branch)
			assert.Equal(t, "main", dr.IntoBranch)
			assert.Equal(t, "open", dr.State)
		}
	})

	t.Run("the closed deploy requests", func(t *testing.T) {
		listed, code := list("?state=closed")
		require.Equal(t, http.StatusOK, code)
		require.Equal(t, []uint64{2}, numbers(listed))
		assert.Equal(t, "closed", listed[0].State)
		assert.Equal(t, "ready", listed[0].Deployment, "the deployment state is left as it was when the deploy request was closed")
	})

	t.Run("a branch with no deploy request lists none", func(t *testing.T) {
		listed, code := list("?branch=feature-c&state=open")
		require.Equal(t, http.StatusOK, code)
		assert.Empty(t, listed)
	})

	t.Run("a state that is neither open nor closed is refused", func(t *testing.T) {
		_, code := list("?state=deploying")
		assert.Equal(t, http.StatusBadRequest, code)
	})
}
