package tern

import (
	"context"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// copiesByNamespaceEngine plans one namespace at a time, reporting whatever
// copy disclosure the test recorded for the namespace it was asked about. Only
// Plan is exercised; the remaining Engine methods exist to satisfy the
// interface and fail the test if the plan path reaches them.
type copiesByNamespaceEngine struct {
	t      *testing.T
	copies map[string][]*engine.ExistingCopy
}

func (e *copiesByNamespaceEngine) Name() string { return "copies-by-namespace" }

func (e *copiesByNamespaceEngine) Plan(_ context.Context, req *engine.PlanRequest) (*engine.PlanResult, error) {
	require.Len(e.t, req.SchemaFiles, 1, "each namespace is planned on its own")
	return &engine.PlanResult{
		PlanID:         "plan-" + req.Database,
		NoChanges:      true,
		ExistingCopies: e.copies[req.Database],
	}, nil
}

func (e *copiesByNamespaceEngine) Apply(context.Context, *engine.ApplyRequest) (*engine.ApplyResult, error) {
	e.t.Fatal("the plan path must not apply")
	return nil, nil
}

func (e *copiesByNamespaceEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	e.t.Fatal("the plan path must not poll progress")
	return nil, nil
}

func (e *copiesByNamespaceEngine) control(op string) (*engine.ControlResult, error) {
	e.t.Fatalf("the plan path must not issue %s", op)
	return nil, nil
}

func (e *copiesByNamespaceEngine) Stop(context.Context, *engine.ControlRequest) (*engine.ControlResult, error) {
	return e.control("stop")
}

func (e *copiesByNamespaceEngine) Cancel(context.Context, *engine.ControlRequest) (*engine.ControlResult, error) {
	return e.control("cancel")
}

func (e *copiesByNamespaceEngine) Start(context.Context, *engine.ControlRequest) (*engine.ControlResult, error) {
	return e.control("start")
}

func (e *copiesByNamespaceEngine) Cutover(context.Context, *engine.ControlRequest) (*engine.ControlResult, error) {
	return e.control("cutover")
}

func (e *copiesByNamespaceEngine) Revert(context.Context, *engine.ControlRequest) (*engine.ControlResult, error) {
	return e.control("revert")
}

func (e *copiesByNamespaceEngine) SkipRevert(context.Context, *engine.ControlRequest) (*engine.ControlResult, error) {
	return e.control("skip-revert")
}

func (e *copiesByNamespaceEngine) Volume(context.Context, *engine.VolumeRequest) (*engine.VolumeResult, error) {
	e.t.Fatal("the plan path must not set volume")
	return nil, nil
}

// TestPlanMySQLNamespacesKeepsEveryNamespacesExistingCopies verifies a
// multi-namespace MySQL plan carries the copy disclosure from every namespace
// that holds unfinished work, not just the first. Each namespace is planned
// against its own target, so a namespace whose copy is dropped here is a target
// whose in-progress work the operator is never told about before confirming an
// apply that destroys it.
func TestPlanMySQLNamespacesKeepsEveryNamespacesExistingCopies(t *testing.T) {
	client := &LocalClient{config: LocalConfig{
		Database:  "commerce",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: "root:pass@tcp(127.0.0.1:3306)/commerce",
	}}
	eng := &copiesByNamespaceEngine{t: t, copies: map[string][]*engine.ExistingCopy{
		"orders": {&engine.ExistingCopy{
			Namespace:   "orders",
			Disposition: engine.CopyDiscard,
			Reason:      engine.DiscardStatementDiffers,
			Tables:      []string{"line_items"},
			Age:         45 * time.Second,
			Statement:   "ALTER TABLE line_items ADD INDEX idx_order (order_id)",
		}},
		"payments": {&engine.ExistingCopy{
			Namespace:   "payments",
			Disposition: engine.CopyAdopt,
			Tables:      []string{"charges"},
			Age:         90 * time.Second,
		}},
	}}

	result, err := client.planMySQLNamespacesWithEngine(t.Context(), eng, &ternv1.PlanRequest{
		Repository:  "block/schemabot",
		PullRequest: 42,
	}, schema.SchemaFiles{
		"orders":   {Files: map[string]string{"line_items.sql": "CREATE TABLE `line_items` (`id` int)"}},
		"payments": {Files: map[string]string{"charges.sql": "CREATE TABLE `charges` (`id` int)"}},
	})
	require.NoError(t, err)

	require.Len(t, result.ExistingCopies, 2, "every namespace holding unfinished work is disclosed")
	assert.Equal(t, "orders", result.ExistingCopies[0].Namespace)
	assert.Equal(t, engine.CopyDiscard, result.ExistingCopies[0].Disposition)
	assert.Equal(t, "payments", result.ExistingCopies[1].Namespace)
	assert.Equal(t, engine.CopyAdopt, result.ExistingCopies[1].Disposition)
}

// TestProtoExistingCopiesCarriesEveryDisclosureField verifies the plan's copy
// disclosures survive the hop onto the wire whole. The control plane renders
// the operator-facing warning from this proto alone, so a field lost here is a
// warning that either understates what applying destroys or omits the cause the
// operator needs to keep the work.
func TestProtoExistingCopiesCarriesEveryDisclosureField(t *testing.T) {
	copies := newMultisetClient("orders", storage.DatabaseTypeMySQL).protoExistingCopies(&engine.PlanResult{ExistingCopies: []*engine.ExistingCopy{
		{
			Namespace:   "orders",
			Disposition: engine.CopyDiscard,
			Reason:      engine.DiscardCheckpointExpired,
			Tables:      []string{"line_items", "shipments"},
			Age:         3 * time.Hour,
			Statement:   "ALTER TABLE line_items ADD INDEX idx_order (order_id)",
		},
		{
			Namespace:   "payments",
			Disposition: engine.CopyAdopt,
			Tables:      []string{"charges"},
			Age:         45 * time.Second,
		},
	}}, nil)

	require.Len(t, copies, 2)
	assert.Equal(t, "orders", copies[0].Namespace)
	assert.Equal(t, string(engine.CopyDiscard), copies[0].Disposition)
	assert.Equal(t, engine.DiscardCheckpointExpired, copies[0].Reason)
	assert.Equal(t, []string{"line_items", "shipments"}, copies[0].Tables)
	assert.Equal(t, int64(10800), copies[0].AgeSeconds)
	assert.Equal(t, "ALTER TABLE line_items ADD INDEX idx_order (order_id)", copies[0].Statement)

	assert.Equal(t, "payments", copies[1].Namespace)
	assert.Equal(t, string(engine.CopyAdopt), copies[1].Disposition)
	assert.Empty(t, copies[1].Reason, "an adopt needs no cause")
	assert.Equal(t, []string{"charges"}, copies[1].Tables)
	assert.Equal(t, int64(45), copies[1].AgeSeconds)
	assert.Empty(t, copies[1].Statement, "an adopt repeats the plan's own statement")
}

// TestProtoExistingCopiesOmitsAnEmptyDisclosure verifies a plan against a
// target holding no unfinished work sends no copy disclosure, so the control
// plane renders no section rather than an empty one.
func TestProtoExistingCopiesOmitsAnEmptyDisclosure(t *testing.T) {
	assert.Nil(t, newMultisetClient("orders", storage.DatabaseTypeMySQL).protoExistingCopies(&engine.PlanResult{}, nil))
}
