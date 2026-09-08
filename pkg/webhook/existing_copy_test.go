package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/engine"
)

// The plan comment separates the copies an apply resumes from the ones it
// destroys, humanizing each copy's age so the operator can weigh what is being
// thrown away.
func TestSplitExistingCopies(t *testing.T) {
	discarded, adopted, running := splitExistingCopies([]*apitypes.ExistingCopyResponse{
		{
			Namespace:   "orders_ks",
			Disposition: string(engine.CopyDiscard),
			Reason:      engine.DiscardStatementDiffers,
			Tables:      []string{"orders"},
			AgeSeconds:  11520,
			Statement:   "ALTER TABLE `orders` ADD INDEX `idx_user_created` (`user_id`)",
		},
		{
			Namespace:   "products_ks",
			Disposition: string(engine.CopyAdopt),
			Tables:      []string{"products"},
			AgeSeconds:  90,
		},
	})

	require.Len(t, discarded, 1)
	assert.Equal(t, "orders_ks", discarded[0].Namespace)
	assert.Equal(t, []string{"orders"}, discarded[0].Tables)
	assert.Equal(t, engine.DiscardStatementDiffers, discarded[0].Reason)
	assert.Equal(t, "3h 12m", discarded[0].Age)
	assert.Equal(t, "ALTER TABLE `orders` ADD INDEX `idx_user_created` (`user_id`)", discarded[0].Statement,
		"the entry carries what the plan differs from, not just that it differs")

	require.Len(t, adopted, 1)
	assert.Equal(t, "products_ks", adopted[0].Namespace)
	assert.Equal(t, "1m 30s", adopted[0].Age)

	assert.Empty(t, running, "a copy the deployment does not report as running is one that stopped")
}

// Work still being made on the target is disclosed apart from work an apply
// left behind. Both survive the apply, but only one of them stopped, so only
// one can be described as picked back up.
func TestSplitExistingCopiesSeparatesRunningWork(t *testing.T) {
	discarded, adopted, running := splitExistingCopies([]*apitypes.ExistingCopyResponse{
		{
			Namespace:   "products_ks",
			Disposition: string(engine.CopyAdopt),
			Tables:      []string{"products"},
			AgeSeconds:  90,
		},
		{
			Namespace:   "orders_ks",
			Disposition: string(engine.CopyAdopt),
			Tables:      []string{"orders", "order_items"},
			AgeSeconds:  4,
			Running:     true,
		},
	})

	assert.Empty(t, discarded, "neither copy is destroyed")

	require.Len(t, adopted, 1)
	assert.Equal(t, "products_ks", adopted[0].Namespace)
	assert.False(t, adopted[0].Running, "a copy that stopped renders dated by its staleness")

	require.Len(t, running, 1)
	assert.Equal(t, "orders_ks", running[0].Namespace)
	assert.Equal(t, []string{"orders", "order_items"}, running[0].Tables)
	assert.True(t, running[0].Running, "the entry itself carries the marker its rendering reads")
}

// A copy the deployment reports as running is only ever reassuring when it also
// survives the apply. A discard destroys it whether or not it is being made
// right now, so the running split never softens that warning.
func TestSplitExistingCopiesKeepsARunningDiscardAsADiscard(t *testing.T) {
	discarded, adopted, running := splitExistingCopies([]*apitypes.ExistingCopyResponse{
		{
			Namespace:   "orders_ks",
			Disposition: string(engine.CopyDiscard),
			Reason:      engine.DiscardStatementDiffers,
			Tables:      []string{"orders"},
			Running:     true,
		},
	})

	assert.Empty(t, adopted)
	assert.Empty(t, running)
	require.Len(t, discarded, 1)
	assert.Equal(t, "orders_ks", discarded[0].Namespace)
	assert.True(t, discarded[0].Running,
		"the entry keeps the marker inside the warning, reading \"(still copying)\" rather than dating live work as stale")
}

// The two sections are opposite promises about the operator's work, so only an
// explicit adopt verdict earns the reassuring one. A disposition this build
// does not recognize is shown as a discard: over-warning costs a second look,
// while promising survival to a copy that is destroyed costs the copy.
func TestSplitExistingCopiesFailsTowardDiscard(t *testing.T) {
	discarded, adopted, running := splitExistingCopies([]*apitypes.ExistingCopyResponse{
		{Namespace: "orders_ks", Disposition: "recycle", Tables: []string{"orders"}},
	})

	assert.Empty(t, adopted)
	assert.Empty(t, running)
	require.Len(t, discarded, 1)
	assert.Equal(t, "orders_ks", discarded[0].Namespace)
}

// A copy with no recorded progress carries no age rather than a zero one,
// which would read as having just started.
func TestSplitExistingCopiesOmitsUnknownAge(t *testing.T) {
	discarded, _, _ := splitExistingCopies([]*apitypes.ExistingCopyResponse{
		{Namespace: "orders_ks", Disposition: apitypes.ExistingCopyDiscard, Tables: []string{"orders"}},
	})

	require.Len(t, discarded, 1)
	assert.Empty(t, discarded[0].Age)
}

// A clean target produces neither section, leaving the plan comment exactly as
// it renders today.
func TestSplitExistingCopiesEmpty(t *testing.T) {
	discarded, adopted, running := splitExistingCopies(nil)

	assert.Empty(t, discarded)
	assert.Empty(t, adopted)
	assert.Empty(t, running)
}

// apitypes carries its own copy of the disposition vocabulary because it is
// dependency-free by design. This package imports both, so it is where the two
// are held together: a rename on the engine side that misses the wire side
// would otherwise silently route every copy down the discard path.
func TestExistingCopyDispositionsMatchTheEngine(t *testing.T) {
	assert.Equal(t, string(engine.CopyAdopt), apitypes.ExistingCopyAdopt)
	assert.Equal(t, string(engine.CopyDiscard), apitypes.ExistingCopyDiscard)
}
