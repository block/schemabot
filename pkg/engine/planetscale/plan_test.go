package planetscale

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/schema"
)

// A schema file that declares two tables contributes both of them to the
// desired schema, each carrying only its own statement, so the differ plans
// every table the file declares.
func TestParseDesiredSchemas_MultipleTablesInOneFile(t *testing.T) {
	ns := &schema.Namespace{Files: map[string]string{
		"tables.sql": "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`));\n" +
			"CREATE TABLE `events` (`id` bigint NOT NULL, PRIMARY KEY (`id`));\n",
	}}

	schemas, err := parseDesiredSchemas("commerce", ns)
	require.NoError(t, err)
	require.Len(t, schemas, 2)
	assert.Equal(t, "orders", schemas[0].Name)
	assert.Equal(t, "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`));", schemas[0].Schema)
	assert.Equal(t, "events", schemas[1].Name)
	assert.Equal(t, "CREATE TABLE `events` (`id` bigint NOT NULL, PRIMARY KEY (`id`));", schemas[1].Schema)
}

// emptyMainBranchClient serves a main branch with safe schema changes enabled
// and no tables in any keyspace, so every desired table plans as a CREATE.
type emptyMainBranchClient struct {
	psclient.PSClient
}

func (c *emptyMainBranchClient) GetBranch(_ context.Context, _ *ps.GetDatabaseBranchRequest) (*ps.DatabaseBranch, error) {
	return &ps.DatabaseBranch{SafeMigrations: true}, nil
}

func (c *emptyMainBranchClient) GetBranchSchema(_ context.Context, _ *ps.BranchSchemaRequest) ([]*ps.Diff, error) {
	return nil, nil
}

func planAgainstEmptyMain(t *testing.T, files map[string]string) (*engine.PlanResult, error) {
	t.Helper()
	e := NewWithClient(slog.New(slog.NewTextHandler(os.Stdout, nil)),
		func(_, _ string) (psclient.PSClient, error) { return &emptyMainBranchClient{}, nil })
	return e.Plan(t.Context(), &engine.PlanRequest{
		Database:    "commerce",
		SchemaFiles: schema.SchemaFiles{"commerce": &schema.Namespace{Files: files}},
		Credentials: &engine.Credentials{Metadata: map[string]string{
			"organization": "org",
			"token_name":   "tn",
			"token_value":  "tv",
		}},
	})
}

// A desired table whose primary key is a signed INT is planned as a CREATE
// and carries the lint finding the keyspace diff raised on that statement,
// so the reviewer sees the warning alongside the DDL it applies to.
func TestPlan_ReportsKeyspaceLintViolations(t *testing.T) {
	result, err := planAgainstEmptyMain(t, map[string]string{
		"orders.sql": "CREATE TABLE `orders` (\n  `id` int NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	})
	require.NoError(t, err)

	require.Len(t, result.Changes, 1)
	require.Len(t, result.Changes[0].TableChanges, 1)
	assert.Equal(t, "orders", result.Changes[0].TableChanges[0].Table)

	assert.Equal(t, []engine.LintViolation{{
		Table:    "orders",
		Column:   "id",
		Linter:   "primary_key",
		Message:  `Primary key column "id" has type "int"`,
		Severity: "error",
	}}, result.LintViolations)
}

// Every planned statement's lint findings reach the plan, across every
// statement in a keyspace and across keyspaces, not only the first or last
// one: a PR that adds three tables with signed INT primary keys sees the
// warning on all three.
func TestPlan_ReportsLintViolationsOfEveryStatement(t *testing.T) {
	e := NewWithClient(slog.New(slog.NewTextHandler(os.Stdout, nil)),
		func(_, _ string) (psclient.PSClient, error) { return &emptyMainBranchClient{}, nil })
	intPK := func(name string) string {
		return "CREATE TABLE `" + name + "` (\n  `id` int NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
	}
	result, err := e.Plan(t.Context(), &engine.PlanRequest{
		Database: "commerce",
		SchemaFiles: schema.SchemaFiles{
			"commerce": &schema.Namespace{Files: map[string]string{"orders.sql": intPK("orders"), "refunds.sql": intPK("refunds")}},
			"billing":  &schema.Namespace{Files: map[string]string{"invoices.sql": intPK("invoices")}},
		},
		Credentials: &engine.Credentials{Metadata: map[string]string{"organization": "org", "token_name": "tn", "token_value": "tv"}},
	})
	require.NoError(t, err)

	var tables []string
	for _, v := range result.LintViolations {
		assert.Equal(t, "primary_key", v.Linter)
		tables = append(tables, v.Table)
	}
	// Spirit does not fix the order of findings within a statement.
	assert.ElementsMatch(t, []string{"orders", "refunds", "invoices"}, tables)
}

// gatedVSchemaClient serves an empty main branch whose VSchema reads are
// scripted per keyspace, so a test can force the order in which keyspaces
// finish. The `commerce` read fails at once and then opens the gate; the
// `billing` read waits at the gate, then waits for its context to be cancelled
// or for billingHold to pass, and answers with billingErr (nil means success).
// A plan that cancels sibling keyspaces when one fails therefore sees billing
// end with the cancellation, after commerce has already failed.
type gatedVSchemaClient struct {
	emptyMainBranchClient
	commerceFailed chan struct{}
	billingErr     error
}

// billingHold bounds how long billing waits for a cancellation that a correct
// plan never sends. It only sets how long the test takes; the outcome a correct
// plan reports does not depend on it.
const billingHold = 200 * time.Millisecond

func newGatedVSchemaClient(billingErr error) *gatedVSchemaClient {
	return &gatedVSchemaClient{commerceFailed: make(chan struct{}), billingErr: billingErr}
}

func (c *gatedVSchemaClient) GetKeyspaceVSchema(ctx context.Context, req *ps.GetKeyspaceVSchemaRequest) (*ps.VSchema, error) {
	switch req.Keyspace {
	case "commerce":
		close(c.commerceFailed)
		return nil, errors.New("commerce unavailable")
	case "billing":
		select {
		case <-c.commerceFailed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(billingHold):
		}
		if c.billingErr != nil {
			return nil, c.billingErr
		}
		return &ps.VSchema{Raw: "{}"}, nil
	default:
		return nil, fmt.Errorf("unexpected keyspace %q", req.Keyspace)
	}
}

// gatedPlanRequest plans keyspaces `billing` and `commerce`, each with a
// VSchema so that each reads it through the gated client.
func gatedPlanRequest() *engine.PlanRequest {
	return &engine.PlanRequest{
		Database: "shop",
		SchemaFiles: schema.SchemaFiles{
			"billing":  &schema.Namespace{Files: map[string]string{"vschema.json": "{}"}},
			"commerce": &schema.Namespace{Files: map[string]string{"vschema.json": "{}"}},
		},
		Credentials: &engine.Credentials{Metadata: map[string]string{"organization": "org", "token_name": "tn", "token_value": "tv"}},
	}
}

// Keyspaces `billing` and `commerce` both fail, and `commerce` is forced to
// fail first: billing's VSchema read does not answer until commerce's has
// failed. The plan still reports billing, the first keyspace in sorted order,
// so the refusal an operator sees does not depend on which keyspace finished
// first.
func TestPlan_RefusalAcrossKeyspacesIsDeterministic(t *testing.T) {
	client := newGatedVSchemaClient(errors.New("billing unavailable"))
	e := NewWithClient(slog.New(slog.NewTextHandler(os.Stdout, nil)),
		func(_, _ string) (psclient.PSClient, error) { return client, nil })

	result, err := e.Plan(t.Context(), gatedPlanRequest())
	require.EqualError(t, err, "fetch VSchema for keyspace billing: billing unavailable")
	assert.Nil(t, result)
}

// Keyspace `commerce` fails first; keyspace `billing` is valid and is still
// reading its VSchema when commerce fails. The failure does not cut billing
// short, so the plan reports commerce's failure, the one real problem, rather
// than an aborted read in billing.
func TestPlan_FailingKeyspaceDoesNotAbortOthers(t *testing.T) {
	client := newGatedVSchemaClient(nil)
	e := NewWithClient(slog.New(slog.NewTextHandler(os.Stdout, nil)),
		func(_, _ string) (psclient.PSClient, error) { return client, nil })

	result, err := e.Plan(t.Context(), gatedPlanRequest())
	require.EqualError(t, err, "fetch VSchema for keyspace commerce: commerce unavailable")
	assert.Nil(t, result)
}

// A desired schema the plan cannot read fails the plan with the reason,
// rather than producing a plan that silently leaves the keyspace out.
func TestPlan_DesiredSchemaErrorFailsThePlan(t *testing.T) {
	tests := []struct {
		name    string
		files   map[string]string
		wantErr string
	}{
		{
			name: "table declared by two schema files",
			files: map[string]string{
				"orders.sql":       "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`));",
				"orders_again.sql": "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`));",
			},
			wantErr: `table "orders" is declared by both schema files "commerce/orders.sql" and "commerce/orders_again.sql"`,
		},
		{
			name: "unparseable SQL",
			files: map[string]string{
				"orders.sql": "CREATE TABLE `orders` (`id` bigint NOT NULL PRIMARY KEY",
			},
			wantErr: "split SQL for keyspace commerce",
		},
		{
			name: "statement that is not a CREATE TABLE",
			files: map[string]string{
				"orders.sql": "ALTER TABLE `orders` ADD COLUMN `note` varchar(255);",
			},
			wantErr: "parse desired schema in keyspace commerce/orders.sql",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := planAgainstEmptyMain(t, tt.files)
			require.ErrorContains(t, err, tt.wantErr)
			assert.Nil(t, result)
		})
	}
}
