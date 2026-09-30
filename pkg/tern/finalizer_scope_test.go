package tern

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// A targets list puts the target in front of every operation key of a
// deployment that addresses several targets, so a group_finalizer's drive has to
// read its namespace from behind the target. Each case drives the finalizer on
// orders-001 (operation 51) over the remote path and checks which namespaces it
// dispatches: exactly its own namespace, or every namespace its plan finalizes
// for a deployment-scoped finalizer, and nothing at all for a key that does not
// belong to the operation's own target.
func TestGRPCClient_ResumeApplyOperationScopesTargetsListFinalizer(t *testing.T) {
	finalizer := func(id int64, target, key string) *storage.ApplyOperation {
		return &storage.ApplyOperation{
			ID:            id,
			ApplyID:       8,
			Deployment:    "orders-deployment",
			Target:        target,
			OperationKey:  key,
			OperationKind: storage.ApplyOperationKindGroupFinalizer,
			State:         state.ApplyOperation.Pending,
		}
	}
	cases := []struct {
		name           string
		planNamespaces []string
		ops            []*storage.ApplyOperation
		wantNamespaces []string
		wantErr        string
	}{
		{
			name:           "targets-list namespace finalizer dispatches its namespace",
			planNamespaces: []string{"ns_0", "ns_1"},
			ops: []*storage.ApplyOperation{
				finalizer(51, "orders-001", "orders-001/ns_0/group_finalizer"),
				finalizer(52, "orders-002", "orders-002/ns_0/group_finalizer"),
			},
			wantNamespaces: []string{"ns_0"},
		},
		{
			name:           "targets-list deployment-scoped finalizer dispatches every finalizer namespace",
			planNamespaces: []string{"ns_0", "ns_1"},
			ops: []*storage.ApplyOperation{
				finalizer(51, "orders-001", "orders-001/group_finalizer"),
				finalizer(52, "orders-002", "orders-002/group_finalizer"),
			},
			wantNamespaces: []string{"ns_0", "ns_1"},
		},
		{
			name:           "targets-list deployment-scoped finalizer whose target shares a namespace's name dispatches every finalizer namespace",
			planNamespaces: []string{"orders", "orders_lookup"},
			ops: []*storage.ApplyOperation{
				finalizer(51, "orders", "orders/group_finalizer"),
				finalizer(52, "orders-002", "orders-002/group_finalizer"),
			},
			wantNamespaces: []string{"orders", "orders_lookup"},
		},
		{
			name:           "single-target namespace finalizer whose namespace shares the target's name dispatches that namespace",
			planNamespaces: []string{"orders", "orders_lookup"},
			ops: []*storage.ApplyOperation{
				finalizer(51, "orders", "orders/group_finalizer"),
			},
			wantNamespaces: []string{"orders"},
		},
		{
			name:           "targets-list key led by another target fails closed",
			planNamespaces: []string{"ns_0"},
			ops: []*storage.ApplyOperation{
				finalizer(51, "orders-001", "orders-002/ns_0/group_finalizer"),
				finalizer(52, "orders-002", "orders-002/group_finalizer"),
			},
			wantErr: "malformed operation key",
		},
		{
			name:           "unqualified namespace key on a targets-list deployment fails closed",
			planNamespaces: []string{"ns_0"},
			ops: []*storage.ApplyOperation{
				finalizer(51, "orders-001", "ns_0/group_finalizer"),
				finalizer(52, "orders-002", "orders-002/ns_0/group_finalizer"),
			},
			wantErr: "malformed operation key",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := &capturingTernServer{remoteApplyID: "remote-targets-finalizer-1"} // default Progress = COMPLETED
			client, cleanup := testCapturingGRPCClient(t, server)
			defer cleanup()

			apply := &storage.Apply{
				ID:              8,
				ApplyIdentifier: "apply-targets-finalizer",
				PlanID:          99,
				Database:        "orders",
				DatabaseType:    storage.DatabaseTypeStrata,
				Environment:     "staging",
				State:           state.Apply.Pending,
			}
			schemaFiles := schema.SchemaFiles{}
			namespaces := map[string]*storage.NamespacePlanData{}
			for _, ns := range tc.planNamespaces {
				schemaFiles[ns] = &schema.Namespace{Files: map[string]string{storage.VSchemaArtifactName: `{"sharded":true}`}}
				namespaces[ns] = &storage.NamespacePlanData{Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded":true}`}}
			}
			ops := make(map[int64]*storage.ApplyOperation, len(tc.ops))
			for _, op := range tc.ops {
				ops[op.ID] = op
			}
			client.storage = &mockStorage{
				applies: &mockApplyStore{apply: apply},
				tasks:   &mockTaskStore{getByApplyIDErr: errors.New("finalizer drive must not load tasks")},
				plans: &mockPlanStore{plan: &storage.Plan{
					ID:             apply.PlanID,
					PlanIdentifier: "plan-targets-finalizer",
					SchemaFiles:    schemaFiles,
					Namespaces:     namespaces,
				}},
				operations: &mockApplyOperationStore{ops: ops},
			}

			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			err := client.ResumeApplyOperation(ctx, apply, 51)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				assert.Nil(t, server.getApplyRequest(), "a finalizer key that does not name its own target's scope must not reach the data plane")
				return
			}
			require.NoError(t, err)
			req := server.getApplyRequest()
			require.NotNil(t, req, "expected the finalizer to dispatch a VSchema apply to remote Tern")
			got := make([]string, 0, len(req.DdlChanges))
			for _, change := range req.DdlChanges {
				got = append(got, change.Namespace)
			}
			assert.ElementsMatch(t, tc.wantNamespaces, got)
		})
	}
}

// A group_finalizer key names one namespace, or the whole deployment, in either
// key shape. The reader must land on the same scope the writer meant, and must
// refuse a key it cannot place rather than finalize the wrong namespace.
func TestFinalizerNamespaceFromKey(t *testing.T) {
	cases := []struct {
		name            string
		key             string
		target          string
		targetQualified bool
		wantNamespace   string
		wantErr         string
	}{
		{name: "single-target namespace", key: "ns_0/group_finalizer", target: "orders-001", wantNamespace: "ns_0"},
		{name: "single-target deployment-scoped", key: "group_finalizer", target: "orders-001", wantNamespace: ""},
		{name: "single-target namespace sharing the target's name", key: "orders/group_finalizer", target: "orders", wantNamespace: "orders"},
		{name: "targets-list namespace", key: "orders-001/ns_0/group_finalizer", target: "orders-001", targetQualified: true, wantNamespace: "ns_0"},
		{name: "targets-list deployment-scoped", key: "orders-001/group_finalizer", target: "orders-001", targetQualified: true, wantNamespace: ""},
		{name: "targets-list deployment-scoped for a target sharing a namespace's name", key: "orders/group_finalizer", target: "orders", targetQualified: true, wantNamespace: ""},
		{name: "targets-list namespace whose siblings planned no work", key: "orders-001/ns_0/group_finalizer", target: "orders-001", wantNamespace: "ns_0"},
		{name: "targets-list namespace led by another target", key: "orders-002/ns_0/group_finalizer", target: "orders-001", targetQualified: true, wantErr: `names target "orders-002", not the operation's target "orders-001"`},
		{name: "targets-list deployment-scoped led by another target", key: "orders-002/group_finalizer", target: "orders-001", targetQualified: true, wantErr: `must lead with target "orders-001"`},
		{name: "unqualified namespace on a targets-list deployment", key: "ns_0/group_finalizer", target: "orders-001", targetQualified: true, wantErr: `must lead with target "orders-001"`},
		{name: "bare key on a targets-list deployment", key: "group_finalizer", target: "orders-001", targetQualified: true, wantErr: `must lead with target "orders-001"`},
		{name: "two components with no target on the row", key: "orders-001/ns_0/group_finalizer", target: "", wantErr: `names target "orders-001", not the operation's target ""`},
		{name: "too many components", key: "orders-001/ns_0/-80/group_finalizer", target: "orders-001", targetQualified: true, wantErr: "more components than a target and a namespace"},
		{name: "empty component", key: "orders-001//group_finalizer", target: "orders-001", targetQualified: true, wantErr: "empty component"},
		{name: "empty scope", key: "/group_finalizer", target: "orders-001", wantErr: "after a scope"},
		{name: "not a finalizer key", key: "orders-001/ns_0/-80/orders", target: "orders-001", targetQualified: true, wantErr: "after a scope"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := finalizerNamespaceFromKey(tc.key, tc.target, tc.targetQualified)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				assert.Empty(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantNamespace, got)
		})
	}
}

// The key writer leads a key with its target exactly where the deployment
// addresses several targets, so the reader decides the shape from the apply's
// operation rows the same way: by the targets on the operation's own
// deployment, not on its neighbours'.
func TestOperationKeysLeadWithTarget(t *testing.T) {
	op := &storage.ApplyOperation{ID: 1, Deployment: "orders-deployment", Target: "orders-001"}
	cases := []struct {
		name string
		ops  []*storage.ApplyOperation
		want bool
	}{
		{name: "only the operation itself", ops: []*storage.ApplyOperation{op}, want: false},
		{name: "no rows listed still counts the operation itself", ops: nil, want: false},
		{name: "siblings on the same target", ops: []*storage.ApplyOperation{op, {ID: 2, Deployment: "orders-deployment", Target: "orders-001"}}, want: false},
		{name: "a second target on the same deployment", ops: []*storage.ApplyOperation{op, {ID: 2, Deployment: "orders-deployment", Target: "orders-002"}}, want: true},
		{name: "a second target on another deployment", ops: []*storage.ApplyOperation{op, {ID: 2, Deployment: "payments-deployment", Target: "payments-001"}, {ID: 3, Deployment: "payments-deployment", Target: "payments-002"}}, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, operationKeysLeadWithTarget(tc.ops, op))
		})
	}
}

// The remote drive's shard-scope guard recognizes per-shard work in both key
// shapes, so a targets-list shard operation whose tasks carry no shard is
// refused before dispatch just as a single-target one is.
func TestIsShardWorkOperationKey(t *testing.T) {
	cases := []struct {
		key    string
		target string
		want   bool
	}{
		{key: "ns_0/-80/orders", target: "orders-001", want: true},
		{key: "orders-001/ns_0/-80/orders", target: "orders-001", want: true},
		{key: "orders-002/ns_0/-80/orders", target: "orders-001", want: false},
		{key: "orders-001/ns_0/-80/orders", target: "", want: false},
		{key: "orders/-80/orders", target: "orders", want: true},
		{key: "ns_0/group_finalizer", target: "orders-001", want: false},
		{key: "orders-001", target: "orders-001", want: false},
		{key: "", target: "orders-001", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.key+" on "+tc.target, func(t *testing.T) {
			assert.Equal(t, tc.want, isShardWorkOperationKey(tc.key, tc.target))
		})
	}
}
