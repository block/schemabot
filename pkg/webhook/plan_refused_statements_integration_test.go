//go:build integration

package webhook

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/proto/ternconv"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

// statementRefusingTernClient answers every plan the way a remote data plane
// answers a refusal over statements in the schema files: FailedPrecondition,
// with the statements as the status detail.
type statementRefusingTernClient struct {
	namespaceSelectingTernClient
	problems []engine.PlanProblem
}

func (c *statementRefusingTernClient) Plan(context.Context, *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	st, err := ternconv.PlanRefusalStatus("2 statements in the schema files cannot be planned", c.problems)
	if err != nil {
		return nil, err
	}
	return nil, st.Err()
}

// A pull request plans staging and production, and production's data plane
// refuses two statements in the schema files. The plan comment lists both,
// each at its file and line with the statement's opening line, and asks for a
// push instead of a re-run, which would be refused the same way. Production
// still stores a failing check row, so the merge stays blocked.
func TestE2EMultiEnvPlanListsRemotelyRefusedStatements(t *testing.T) {
	production := &statementRefusingTernClient{problems: []engine.PlanProblem{
		{File: "ns_0/orders.sql", Line: 6, Message: "Comments are not managed. Set the comment on the database directly.", Statement: "COMMENT ON TABLE orders IS 'Orders placed'"},
		{File: "ns_1/orders.sql", Line: 9, Message: "Triggers are not managed.", Statement: "CREATE TRIGGER orders_touch BEFORE UPDATE ON orders\nFOR EACH ROW EXECUTE FUNCTION touch()"},
	}}
	p := newMultiEnvPlanHarness(t, []api.TargetEntry{
		{Target: "orders-001", Namespaces: []string{"ns_0"}},
		{Target: "orders-002", Namespaces: []string{"ns_1"}},
	}, production, nil)

	comment := p.comment(t, "schemabot plan")

	assert.Contains(t, comment, "SchemaBot can't plan 2 statements in the schema files:")
	assert.Contains(t, comment, "**`ns_0/orders.sql`, line 6:** Comments are not managed. Set the comment on the database directly.\n```sql\nCOMMENT ON TABLE orders IS 'Orders placed'\n```")
	assert.Contains(t, comment, "**`ns_1/orders.sql`, line 9:** Triggers are not managed.\n```sql\nCREATE TRIGGER orders_touch BEFORE UPDATE ON orders …\n```")
	assert.Contains(t, comment, "**Production** plans once these statements change. Fix them in the schema files and push a commit.")
	assert.NotContains(t, comment, "schemabot plan -e production")
	assert.NotContains(t, comment, "rpc error")

	byEnv := p.checksByEnv(t)
	require.Contains(t, byEnv, "production")
	assert.Equal(t, checkConclusionFailure, byEnv["production"].Conclusion, "a refused plan still blocks the merge")
}
