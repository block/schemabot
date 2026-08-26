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
	discarded, adopted := splitExistingCopies([]*apitypes.ExistingCopyResponse{
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
}

// The two sections are opposite promises about the operator's work, so only an
// explicit adopt verdict earns the reassuring one. A disposition this build
// does not recognize is shown as a discard: over-warning costs a second look,
// while promising survival to a copy that is destroyed costs the copy.
func TestSplitExistingCopiesFailsTowardDiscard(t *testing.T) {
	discarded, adopted := splitExistingCopies([]*apitypes.ExistingCopyResponse{
		{Namespace: "orders_ks", Disposition: "recycle", Tables: []string{"orders"}},
	})

	assert.Empty(t, adopted)
	require.Len(t, discarded, 1)
	assert.Equal(t, "orders_ks", discarded[0].Namespace)
}

// A copy with no recorded progress carries no age rather than a zero one,
// which would read as having just started.
func TestSplitExistingCopiesOmitsUnknownAge(t *testing.T) {
	discarded, _ := splitExistingCopies([]*apitypes.ExistingCopyResponse{
		{Namespace: "orders_ks", Disposition: apitypes.ExistingCopyDiscard, Tables: []string{"orders"}},
	})

	require.Len(t, discarded, 1)
	assert.Empty(t, discarded[0].Age)
}

// A clean target produces neither section, leaving the plan comment exactly as
// it renders today.
func TestSplitExistingCopiesEmpty(t *testing.T) {
	discarded, adopted := splitExistingCopies(nil)

	assert.Empty(t, discarded)
	assert.Empty(t, adopted)
}

// apitypes carries its own copy of the disposition vocabulary because it is
// dependency-free by design. This package imports both, so it is where the two
// are held together: a rename on the engine side that misses the wire side
// would otherwise silently route every copy down the discard path.
func TestExistingCopyDispositionsMatchTheEngine(t *testing.T) {
	assert.Equal(t, string(engine.CopyAdopt), apitypes.ExistingCopyAdopt)
	assert.Equal(t, string(engine.CopyDiscard), apitypes.ExistingCopyDiscard)
}
