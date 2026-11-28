package commands

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

// jsonAPI serves a fixed JSON body per path and counts the requests to each.
type jsonAPI struct {
	replies map[string]jsonReply
	hits    map[string]int
}

type jsonReply struct {
	status int
	body   any
}

func (s *jsonAPI) start(t *testing.T) string {
	t.Helper()
	s.hits = map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.hits[r.Method+" "+r.URL.Path]++
		reply, ok := s.replies[r.Method+" "+r.URL.Path]
		if !ok {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if reply.status != 0 {
			w.WriteHeader(reply.status)
		}
		assert.NoError(t, json.NewEncoder(w).Encode(reply.body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// An agent reading locks gets the same document GET /api/locks returns: the
// fields the server leaves out stay out, rather than appearing as zero values
// that read as a real repository, PR, or timestamp.
func TestLocksJSONMatchesTheAPIDocument(t *testing.T) {
	api := &jsonAPI{replies: map[string]jsonReply{
		"GET /api/locks": {body: map[string]any{"locks": []map[string]any{
			{"id": 7, "database": "shop", "database_type": "mysql", "owner": "example/store#42",
				"repository": "example/store", "pull_request": 42, "created_at": "2026-09-01T02:55:00Z"},
			{"database": "billing", "database_type": "postgres", "owner": "cli:dana@laptop"},
		}}},
	}}
	out, err := runLockCmd(api.start(t), (&LocksCmd{JSON: true}).Run)
	require.NoError(t, err)

	var doc map[string][]map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &doc), out)
	require.Len(t, doc["locks"], 2)
	assert.Equal(t, map[string]any{
		"id": float64(7), "database": "shop", "database_type": "mysql", "owner": "example/store#42",
		"repository": "example/store", "pull_request": float64(42), "created_at": "2026-09-01T02:55:00Z",
	}, doc["locks"][0])
	assert.Equal(t, map[string]any{
		"database": "billing", "database_type": "postgres", "owner": "cli:dana@laptop",
	}, doc["locks"][1])
}

// No locks is an empty list, so a caller can range over it without a nil check.
func TestLocksJSONWithNoLocksIsAnEmptyList(t *testing.T) {
	api := &jsonAPI{replies: map[string]jsonReply{
		"GET /api/locks": {body: map[string]any{"locks": nil}},
	}}
	out, err := runLockCmd(api.start(t), (&LocksCmd{JSON: true}).Run)
	require.NoError(t, err)
	assert.JSONEq(t, `{"locks": []}`, out)
}

// progress --json prints one snapshot even though watching is the default,
// because the watcher is interactive and its keys can change the apply.
func TestProgressJSONPrintsOneSnapshotWithoutWatching(t *testing.T) {
	snapshot := apitypes.ProgressResponse{
		State: "running", Engine: "Spirit", ApplyID: "apply-example-73", Database: "shop",
		DatabaseType: "mysql", Environment: "production",
		Tables: []*apitypes.TableProgressResponse{{
			TableName: "orders", DDL: "ALTER TABLE `orders` ADD INDEX `idx_status`(`status`)",
			Status: "running", RowsCopied: 6000000, RowsTotal: 10000000, PercentComplete: 60,
		}},
	}
	api := &jsonAPI{replies: map[string]jsonReply{
		"GET /api/progress/apply/apply-example-73": {body: snapshot},
	}}
	cmd := &ProgressCmd{ControlFlags: ControlFlags{ApplyID: "apply-example-73"}, Watch: true, JSON: true}
	out, err := runLockCmd(api.start(t), cmd.Run)
	require.NoError(t, err)

	var got apitypes.ProgressResponse
	require.NoError(t, json.Unmarshal([]byte(out), &got), out)
	assert.Equal(t, snapshot, got)
	assert.Equal(t, 1, api.hits["GET /api/progress/apply/apply-example-73"])
}

// settings --json says, for each known setting, its value and whether that
// value is stored or is the default the server falls back to.
func TestSettingsJSON(t *testing.T) {
	t.Run("ListFallsBackToTheDefault", func(t *testing.T) {
		api := &jsonAPI{replies: map[string]jsonReply{
			"GET /api/settings": {body: map[string]any{"settings": []map[string]string{}}},
		}}
		out, err := runLockCmd(api.start(t), (&SettingsCmd{JSON: true}).Run)
		require.NoError(t, err)
		assert.JSONEq(t, `{"settings": [{
			"key": "spirit_debug_logs", "value": "false", "set": false, "default": "false",
			"description": "Enable verbose Spirit debug logs (replication events, etc.)"
		}]}`, out)
	})

	t.Run("ListReportsAStoredValue", func(t *testing.T) {
		api := &jsonAPI{replies: map[string]jsonReply{
			"GET /api/settings": {body: map[string]any{"settings": []map[string]string{
				{"key": "spirit_debug_logs", "value": "true"},
			}}},
		}}
		out, err := runLockCmd(api.start(t), (&SettingsCmd{JSON: true}).Run)
		require.NoError(t, err)
		assert.JSONEq(t, `{"settings": [{
			"key": "spirit_debug_logs", "value": "true", "set": true, "default": "false",
			"description": "Enable verbose Spirit debug logs (replication events, etc.)"
		}]}`, out)
	})

	t.Run("GetAnUnsetKey", func(t *testing.T) {
		api := &jsonAPI{replies: map[string]jsonReply{
			"GET /api/settings/spirit_debug_logs": {status: http.StatusNotFound, body: map[string]string{"error": "not found"}},
		}}
		out, err := runLockCmd(api.start(t), (&SettingsCmd{Args: []string{"spirit_debug_logs"}, JSON: true}).Run)
		require.NoError(t, err)
		assert.JSONEq(t, `{
			"key": "spirit_debug_logs", "value": "false", "set": false, "default": "false",
			"description": "Enable verbose Spirit debug logs (replication events, etc.)"
		}`, out)
	})

	// An empty value reads as unset whichever command reports it, so a
	// caller that sets "" and reads it back gets the same answer.
	t.Run("AnEmptyValueReadsAsUnsetOnEveryPath", func(t *testing.T) {
		unset := `{
			"key": "spirit_debug_logs", "value": "false", "set": false, "default": "false",
			"description": "Enable verbose Spirit debug logs (replication events, etc.)"
		}`
		api := &jsonAPI{replies: map[string]jsonReply{
			"POST /api/settings":                  {body: map[string]string{}},
			"GET /api/settings":                   {body: map[string]any{"settings": []map[string]string{{"key": "spirit_debug_logs", "value": ""}}}},
			"GET /api/settings/spirit_debug_logs": {body: map[string]string{"key": "spirit_debug_logs", "value": ""}},
		}}
		endpoint := api.start(t)

		out, err := runLockCmd(endpoint, (&SettingsCmd{Args: []string{"spirit_debug_logs", ""}, JSON: true}).Run)
		require.NoError(t, err)
		assert.JSONEq(t, unset, out, "set")

		out, err = runLockCmd(endpoint, (&SettingsCmd{Args: []string{"spirit_debug_logs"}, JSON: true}).Run)
		require.NoError(t, err)
		assert.JSONEq(t, unset, out, "get")

		out, err = runLockCmd(endpoint, (&SettingsCmd{JSON: true}).Run)
		require.NoError(t, err)
		assert.JSONEq(t, `{"settings": [`+unset+`]}`, out, "list")
	})

	t.Run("SetReportsTheStoredValue", func(t *testing.T) {
		api := &jsonAPI{replies: map[string]jsonReply{
			"POST /api/settings": {body: map[string]string{}},
		}}
		out, err := runLockCmd(api.start(t), (&SettingsCmd{Args: []string{"spirit_debug_logs", "true"}, JSON: true}).Run)
		require.NoError(t, err)
		assert.JSONEq(t, `{
			"key": "spirit_debug_logs", "value": "true", "set": true, "default": "false",
			"description": "Enable verbose Spirit debug logs (replication events, etc.)"
		}`, out)
	})
}

// The freeze reads print the API's own types, so an agent can tell an
// in-effect freeze from a scheduled one and read an override's blockers.
func TestFreezeReadsJSON(t *testing.T) {
	freeze := freezeFixture()
	override := overrideFixture()

	t.Run("List", func(t *testing.T) {
		list := apitypes.FreezeListResponse{InEffect: []apitypes.Freeze{freeze}, Scheduled: []apitypes.Freeze{}, Recent: []apitypes.Freeze{freeze}}
		api := &jsonAPI{replies: map[string]jsonReply{"GET /api/freezes": {body: list}}}
		out, err := runLockCmd(api.start(t), (&FreezeListCmd{JSON: true}).Run)
		require.NoError(t, err)
		var got apitypes.FreezeListResponse
		require.NoError(t, json.Unmarshal([]byte(out), &got), out)
		assert.Equal(t, list, got)
	})

	t.Run("Show", func(t *testing.T) {
		api := &jsonAPI{replies: map[string]jsonReply{"GET /api/freezes/freeze-abc": {body: freeze}}}
		out, err := runLockCmd(api.start(t), (&FreezeShowCmd{ID: "freeze-abc", JSON: true}).Run)
		require.NoError(t, err)
		var got apitypes.Freeze
		require.NoError(t, json.Unmarshal([]byte(out), &got), out)
		assert.Equal(t, freeze, got)
	})

	t.Run("Override", func(t *testing.T) {
		api := &jsonAPI{replies: map[string]jsonReply{"GET /api/freezes/overrides/ovr-123": {body: override}}}
		out, err := runLockCmd(api.start(t), (&FreezeOverrideCmd{ID: "ovr-123", JSON: true}).Run)
		require.NoError(t, err)
		var got apitypes.FreezeOverrideRequest
		require.NoError(t, json.Unmarshal([]byte(out), &got), out)
		assert.Equal(t, override, got)
	})
}

// pull --json prints the same document as -o json, after any --table filter,
// so an agent uses the one JSON flag every other read command takes.
func TestPullJSONMatchesOutputJSON(t *testing.T) {
	pulled := &apitypes.PullSchemaResponse{
		Database: "shop", Type: "mysql", Environment: "staging", TableCount: 2,
		Namespaces: map[string]*apitypes.PulledNamespace{
			"shop": {Tables: map[string]string{
				"orders":    "CREATE TABLE `orders` (`id` bigint NOT NULL);",
				"customers": "CREATE TABLE `customers` (`id` bigint NOT NULL);",
			}},
		},
	}
	api := &jsonAPI{replies: map[string]jsonReply{"POST /api/pull": {body: pulled}}}
	endpoint := api.start(t)

	viaFlag, err := runLockCmd(endpoint, (&PullCmd{Database: "shop", Environment: "staging", Table: "orders", Output: "pretty", JSON: true}).Run)
	require.NoError(t, err)
	viaOutput, err := runLockCmd(endpoint, (&PullCmd{Database: "shop", Environment: "staging", Table: "orders", Output: "json"}).Run)
	require.NoError(t, err)
	assert.JSONEq(t, viaOutput, viaFlag)

	var doc apitypes.PullSchemaResponse
	require.NoError(t, json.Unmarshal([]byte(viaFlag), &doc), viaFlag)
	require.Contains(t, doc.Namespaces, "shop")
	assert.Equal(t, map[string]string{"orders": "CREATE TABLE `orders` (`id` bigint NOT NULL);"}, doc.Namespaces["shop"].Tables)
	assert.EqualValues(t, 1, doc.TableCount, "the table count follows the --table filter")
	assert.Equal(t, 2, api.hits["POST /api/pull"])
}
