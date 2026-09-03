package spirit

import (
	"testing"

	"github.com/block/spirit/pkg/migration/check"
	"github.com/stretchr/testify/assert"
)

// A refusal reason is shown to the operator who submitted the schema change,
// and routing decides that by category rather than per check: whatever the
// engine's statement-scope checks report is publishable. That judgement was
// made by reading the checks in the scope — each reports the submitted
// statement back, and the statement is already on the pull request.
//
// Which checks are in the scope is governed by the dependency, not by this
// repository. One the engine adds later arrives through a version bump with no
// diff here to review, and would inherit the judgement without ever being
// read. Pinning the membership keeps the judgement attached to the checks it
// was made about.
func TestPublishableRefusalChecks(t *testing.T) {
	assert.Equal(t, []string{
		"addforeignkey",
		"enumReorder",
		"enumSetRemoval",
		"illegalClause",
		"primarykey",
		"primarykeyexists",
		"setReorder",
	}, check.ChecksInScope(check.ScopeStatement),
		"the statement scope changed: read the check and decide whether its reason is safe to show an operator before updating this list")
}
