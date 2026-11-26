package api

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
)

// authoredErrorCase is one marked error type: an instance, a fragment its text
// must carry, and whether a library composed that text from the repository.
type authoredErrorCase struct {
	err              error
	contains         string
	quotesRepository bool
}

// markedErrorTypes are the error types on the plan path that SchemaBot wrote
// for the person reading the pull request. The text a plan failure shows comes
// from here, so each entry is a sentence that is safe to publish.
var markedErrorTypes = map[string]authoredErrorCase{
	"DatabaseNotConfiguredError": {
		err:      &DatabaseNotConfiguredError{Database: "payments"},
		contains: `database "payments" is not configured on this server`,
	},
	"EnvironmentNotConfiguredError": {
		err:      &EnvironmentNotConfiguredError{Database: "payments", Environment: "staging"},
		contains: `database "payments" environment "staging" is not configured on this server`,
	},
	"databaseTypeMismatchError": {
		err:      &databaseTypeMismatchError{Database: "payments", RequestType: "vitess", ConfigType: "mysql"},
		contains: `database "payments" type "vitess" does not match server config type "mysql"`,
	},
	"SourcePolicyError": {
		err:      &SourcePolicyError{Reason: SourcePolicyReasonUnauthorizedRepo, Message: `database "payments" is not managed from repository "acme/web"`},
		contains: `database "payments" is not managed from repository "acme/web"`,
	},
	"RolloutMemberSelectionError": {
		err:      &RolloutMemberSelectionError{Database: "payments", Environment: "staging", Selector: "eu", Valid: []string{"us/east", "us/west"}},
		contains: `target "eu" is not a rollout member of database "payments" environment "staging"; valid targets: us/east, us/west`,
	},
	"NamespaceCoverageError": {
		err:      &NamespaceCoverageError{Database: "payments", Environment: "staging", Uncovered: []string{"ledger"}},
		contains: `declares namespaces [ledger] that no targets entry selects`,
	},
	"NamespaceSelectionError": {
		err:      &NamespaceSelectionError{Database: "payments", Environment: "staging", Target: "east", Namespace: "ledger", reason: "the schema files do not declare"},
		contains: `target "east" selects namespace "ledger", which the schema files do not declare`,
	},
	"DropTablesInUseError": {
		err:      &DropTablesInUseError{Entry: "orders_new", ApplyID: "apply-1", ApplyState: state.Apply.Stopped, Table: "orders"},
		contains: `drop_tables entry "orders_new" is a working table of apply apply-1`,
	},
	"UnselectedTableDropError": {
		err:      &UnselectedTableDropError{Deployment: "us", Target: "east", Placed: []string{"ledger_old"}, PlacedNamespaces: []string{"ledger"}},
		contains: `the plan from deployment "us" target "east" proposes dropping "ledger_old" in namespaces [ledger]`,
	},
	"SharedSchemaIgnoreListsError": {
		err: &SharedSchemaIgnoreListsError{Environment: "staging", Doubled: []DoublyDeclaredTables{{
			Databases: [2]string{"payments", "billing"}, Namespaces: [2]string{"core", "core"}, Target: "east", Tables: []string{"invoices"},
		}}},
		contains: `"payments" and "billing" both declare "invoices" in namespace "core" on target "east"`,
	},
	"operatorMessageError": {
		err:      operatorMessagef("re-plan the environment, then apply again"),
		contains: "re-plan the environment, then apply again",
	},
	"schemaFileParseError": {
		err:              &schemaFileParseError{message: "split schema file core/orders.sql: syntax error near 'CREAT'", err: errors.New("syntax error near 'CREAT'")},
		contains:         "split schema file core/orders.sql",
		quotesRepository: true,
	},
}

// unmarkedErrorTypes are the error types declared in this package that stay
// unmarked, each with the reason. An unmarked error is never shown as it
// arrived, so the reason is either that its text carries a cause, or that no
// plan reaches it.
var unmarkedErrorTypes = map[string]string{
	// Their text carries a cause that can name a host, a DSN, or a driver's words.
	"RemoteDeploymentUnavailableError": "text carries the data plane's dial error",
	"NamespaceResolutionError":         "text carries the data plane's error",
	"DropTablesCheckError":             "text carries a storage error",
	"UnselectedTableDropCheckError":    "text carries its cause; a schema file parse failure inside it is marked by itself",
	"PRSchemaAdmissionError":           "text is the admission callback's error",
	"applyStorageError":                "text carries a storage error",
	"storageSchemaTargetError":         "text carries the target's error",
	"webhookOpsRequestError":           "text carries the wrapped request error",
	"blockedPlanError":                 "text is the cause's text",

	// The webhook renders each of these with a template of its own.
	"ChangeGateRefusedError":          "rendered by the change gate refusal comment",
	"ChangeGateUnavailableError":      "rendered through its UserMessage",
	"PullRequestApprovalRefusedError": "answers a freeze approval request, not a plan",

	// No plan reaches them: they answer a pull, an apply, a rollback, or a
	// control request.
	"unsupportedPullSchemaError":      "answers a pull",
	"unsupportedLintDialectError":     "answers a pull",
	"unlintablePulledTableError":      "answers a pull",
	"UnselectedPullNamespaceError":    "answers a pull",
	"UnsupportedFeatureError":         "answers an apply",
	"applyLockReleasedError":          "answers an apply",
	"PlanNotFoundError":               "answers an apply or a stored plan lookup",
	"PlanEnvironmentMismatchError":    "answers an apply or a stored plan lookup",
	"PlanRoutingMetadataError":        "answers an apply or a stored plan lookup",
	"MemberPlanRefusedError":          "answers an apply",
	"MemberWorkRefusedError":          "answers an apply",
	"PlanMemberMismatchError":         "answers an apply",
	"unsafeOptInRequiredError":        "answers an apply",
	"RolloutShapeRefusedError":        "answers an apply",
	"DeferredCutoverCapacityError":    "answers an apply",
	"RolloutRollbackUnsupportedError": "answers a rollback",
	"RolloutUnrenderedError":          "answers POST /api/plan, which the webhook does not call",
	"terminalControlError":            "answers a control request",
	"controlOperationHTTPError":       "answers a control request",
}

// Every error type declared in this package is either marked as written for
// the reader or listed with the reason it is not. A type added without either
// fails here, so a plan failure the webhook should show cannot quietly lose its
// text, and a type nobody classified cannot be assumed safe.
func TestEveryErrorTypeIsMarkedOrExplained(t *testing.T) {
	declared, marked := parseErrorTypes(t)

	assert.ElementsMatch(t, slices.Collect(maps.Keys(markedErrorTypes)), marked,
		"markedErrorTypes must list exactly the types that implement AuthoredText")

	for _, name := range declared {
		_, isMarked := markedErrorTypes[name]
		_, isExplained := unmarkedErrorTypes[name]
		assert.True(t, isMarked || isExplained,
			"%s is an error type with no entry: add it to markedErrorTypes and give it AuthoredText, or to unmarkedErrorTypes with the reason", name)
		assert.False(t, isMarked && isExplained, "%s is listed as both marked and unmarked", name)
	}
	for name, reason := range unmarkedErrorTypes {
		assert.Contains(t, declared, name, "unmarkedErrorTypes lists %s, which is no longer an error type of this package", name)
		assert.NotEmpty(t, reason, name)
	}
	for name := range markedErrorTypes {
		assert.Contains(t, declared, name, "markedErrorTypes lists %s, which is no longer an error type of this package", name)
	}
}

// A marked error gives up its own text, flagged for what the text quotes, and
// the wrap the plan adds around it, "source policy: ..." for one, stays out of
// what a renderer is handed.
func TestMarkedErrorsReturnTheirAuthoredText(t *testing.T) {
	for name, tc := range markedErrorTypes {
		t.Run(name, func(t *testing.T) {
			for _, err := range []error{tc.err, fmt.Errorf("plan %s: %w", "payments/staging", tc.err)} {
				text, quotesRepository, ok := AuthoredTextOf(err)
				require.True(t, ok)
				assert.Equal(t, tc.err.Error(), text)
				assert.Contains(t, text, tc.contains)
				assert.Equal(t, tc.quotesRepository, quotesRepository)
			}
		})
	}
}

// An internal failure that the plan wraps with context is not marked, nor is a
// wrapper that carries one: only text SchemaBot wrote reaches a renderer.
func TestInternalErrorsAreNotMarked(t *testing.T) {
	dial := errors.New("dial tcp 10.0.0.5:3306: connect: connection refused")

	for name, err := range map[string]error{
		"wrapped dial error":     fmt.Errorf("resolve target: %w", dial),
		"tern client":            fmt.Errorf("database %q (staging): %w", "payments", dial),
		"remote deployment":      &RemoteDeploymentUnavailableError{Deployment: "us", Target: "east", Err: dial},
		"namespace resolution":   &NamespaceResolutionError{Database: "payments", Cause: dial},
		"drop_tables check":      &DropTablesCheckError{Database: "payments", Environment: "staging", Err: dial},
		"unselected drops check": &UnselectedTableDropCheckError{Database: "payments", Environment: "staging", Err: dial},
		"admission callback":     &PRSchemaAdmissionError{Cause: dial},
		"apply storage":          &applyStorageError{Operation: "queue apply", Err: dial},
		"engine permanent":       &engine.PermanentError{Err: dial},
	} {
		text, _, ok := AuthoredTextOf(err)
		assert.False(t, ok, "%s must stay unmarked", name)
		assert.Empty(t, text, name)
	}
}

// The engine's own authored errors are recognized through the same marker: a
// message SchemaBot composed, and a refusal of the schema files. The cause of
// the first, and the wrap the plan adds around either, stays out of the text.
func TestEngineAuthoredErrorsAreMarked(t *testing.T) {
	dial := errors.New("dial tcp 10.0.0.5:3306: connect: connection refused")

	tests := []struct {
		name             string
		err              error
		want             string
		quotesRepository bool
	}{
		{
			name: "operator message",
			err:  engine.OperatorErrorf(dial, "the copy of %q has not started", "orders"),
			want: `the copy of "orders" has not started`,
		},
		{
			name: "drop_tables entry also ignored",
			err:  refuseDropTablesAlsoIgnored([]string{"jobs_old"}, []string{"jobs_old"}),
			want: `drop_tables entry "jobs_old" is also withheld by ignore_tables. Remove it from one of the two lists`,
		},
		{
			name:             "plan refusal",
			err:              &engine.PlanRefusedError{Err: errors.New(`the schema files drop table "orders_seq"`)},
			want:             `the schema files drop table "orders_seq"`,
			quotesRepository: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, err := range []error{tt.err, fmt.Errorf("plan namespace payments via 10.0.0.5:3306: %w", tt.err)} {
				text, quotesRepository, ok := AuthoredTextOf(err)
				require.True(t, ok)
				assert.Equal(t, tt.want, text)
				assert.Equal(t, tt.quotesRepository, quotesRepository)
			}
		})
	}
}

// A schema file the shared schema check cannot parse is marked the same way as
// the one the drop check cannot, and the parser's own error stays its cause.
func TestSharedSchemaParseFailureIsMarked(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectForDatabaseType("mysql"))
	require.NoError(t, err)

	_, err = tablesCreatedIn(parser, "core", &ternv1.SchemaFiles{Files: map[string]string{"orders.sql": "CREATE TABLE orders (id bigint"}})

	require.Error(t, err)
	text, quotesRepository, ok := AuthoredTextOf(fmt.Errorf("schema files of database %q: %w", "payments", err))
	require.True(t, ok)
	assert.Contains(t, text, "schema file core/orders.sql")
	assert.True(t, quotesRepository)
	parseErr, ok := errors.AsType[*schemaFileParseError](err)
	require.True(t, ok)
	assert.Error(t, errors.Unwrap(parseErr))
}

// parseErrorTypes parses this package's non-test source and returns the types
// that declare an Error method and the types that declare AuthoredText, both
// sorted.
func parseErrorTypes(t *testing.T) (declared, marked []string) {
	t.Helper()
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		for _, d := range file.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || len(fn.Recv.List) != 1 {
				continue
			}
			receiver := receiverTypeName(fn.Recv.List[0].Type)
			switch {
			case fn.Name.Name == "Error" && returnsString(fn):
				declared = append(declared, receiver)
			case fn.Name.Name == "AuthoredText":
				marked = append(marked, receiver)
			}
		}
	}
	slices.Sort(declared)
	slices.Sort(marked)
	return declared, marked
}

func receiverTypeName(expr ast.Expr) string {
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	if ident, ok := expr.(*ast.Ident); ok {
		return ident.Name
	}
	return ""
}

func returnsString(fn *ast.FuncDecl) bool {
	if fn.Type.Params.NumFields() != 0 || fn.Type.Results.NumFields() != 1 {
		return false
	}
	ident, ok := fn.Type.Results.List[0].Type.(*ast.Ident)
	return ok && ident.Name == "string"
}
