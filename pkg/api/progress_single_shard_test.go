package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// A sharded apply on a keyspace's only shard, whose finalizer has no VSchema
// change to show, reads as one change on one database: the progress response
// says so, so the CLI renders it the way its PR comments do rather than as a
// section per shard and finalizer. Anything the comments render shard by shard
// — a VSchema change, a keyspace across several shards, a stored plan that
// cannot be read — keeps the flag off.
func TestProgressByApplyIDReportsASingleShardApply(t *testing.T) {
	finalizeOnly := &storage.Plan{Namespaces: map[string]*storage.NamespacePlanData{"shop_001": {Finalize: true}}}
	vschemaChange := &storage.Plan{Namespaces: map[string]*storage.NamespacePlanData{"shop_001": {
		Finalize:  true,
		Artifacts: map[string]string{storage.VSchemaArtifactName: `{"tables":{"orders":{}}}`},
	}}}
	onlyShard := []string{"shop_001/-/orders", "shop_001/group_finalizer"}
	twoShards := []string{"shop_001/-80/orders", "shop_001/80-/orders", "shop_001/group_finalizer"}

	cases := []struct {
		name  string
		keys  []string
		plans *staticPlanStore
		want  bool
	}{
		{"the keyspace's only shard, finalized without a VSchema change", onlyShard, &staticPlanStore{plan: finalizeOnly}, true},
		{"a VSchema change to show", onlyShard, &staticPlanStore{plan: vschemaChange}, false},
		{"a keyspace across two shards", twoShards, &staticPlanStore{plan: finalizeOnly}, false},
		{"a stored plan that cannot be read", onlyShard, &staticPlanStore{err: errors.New("storage unavailable")}, false},
		{"a stored plan row that is missing", onlyShard, &staticPlanStore{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			apply := activeTestApply("apply-single-shard")
			apply.PlanID = 7
			var ops []*storage.ApplyOperation
			for i, key := range tc.keys {
				kind := storage.ApplyOperationKindWork
				if _, ok := state.NamespaceFinalizerKey(key); ok {
					kind = storage.ApplyOperationKindGroupFinalizer
				}
				ops = append(ops, &storage.ApplyOperation{
					ID: int64(i + 1), ApplyID: apply.ID, Deployment: "cake", OperationKey: key,
					OperationKind: kind, State: state.ApplyOperation.Running,
				})
			}
			svc := New(&mockStorageWithApplyStores{
				plans:      tc.plans,
				applies:    &staticApplyStore{apply: apply},
				tasks:      &capturingTaskStore{},
				controls:   &memoryControlRequestStore{},
				operations: &staticApplyOperationStore{operations: ops},
			}, testServerConfig(), map[string]tern.Client{"default/staging": &mockTernClient{isRemote: true}},
				slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
			mux := http.NewServeMux()
			svc.ConfigureRoutes(mux)

			w := httptest.NewRecorder()
			mux.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/progress/apply/apply-single-shard", nil))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			var resp apitypes.ProgressResponse
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
			assert.Equal(t, tc.want, resp.SingleShard)
			assert.Len(t, resp.Operations, len(tc.keys), "every shard and finalizer row is still listed")
		})
	}
}
