package webhook

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/checkstate"
)

// Every durable block a writer records has to mean something to the reader
// that explains it. An unclassified reason falls back to the generic guard
// remedy, which is safe but wrong for a block that actually owes a
// reconciliation, so a new block reason fails here rather than shipping with
// the wrong next action.
func TestEveryCheckBlockReasonIsClassified(t *testing.T) {
	t.Parallel()

	require.NotEmpty(t, allCheckBlockReasons)
	for _, block := range allCheckBlockReasons {
		assert.True(t, checkstate.BlockingReasonIsClassified(block.blockingReason),
			"blocking reason %q needs an entry in pkg/checkstate's classification", block.blockingReason)
	}
}

// The registry above is only complete if it lists every block the package
// declares, so the declarations themselves are the source of truth for the
// test rather than a list someone has to remember to extend.
func TestAllCheckBlockReasonsListsEveryDeclaredBlock(t *testing.T) {
	t.Parallel()

	declared := declaredCheckBlockReasonVars(t)
	require.NotEmpty(t, declared)

	listed := make(map[string]bool, len(allCheckBlockReasons))
	for _, block := range allCheckBlockReasons {
		listed[block.blockingReason] = true
	}
	for name, reason := range declared {
		assert.True(t, listed[reason],
			"%s declares blocking reason %q; add it to allCheckBlockReasons", name, reason)
	}
}

// declaredCheckBlockReasonVars returns every package-level declaration that
// names a durable blocking reason, keyed by declaration name.
//
// Both shapes count. A `checkBlockReason` composite literal is the usual one,
// but a reason can also be declared as a bare const aliasing a checkstate
// constant, and a reason the registry does not list takes the unknown-block
// default and prints the guard remedy for a block that may owe something else.
func declaredCheckBlockReasonVars(t *testing.T) map[string]string {
	t.Helper()

	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	var files []*ast.File
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, parseErr)
		files = append(files, file)
	}
	require.NotEmpty(t, files, "the webhook package sources must parse for this test to mean anything")

	// The reason is usually a checkstate constant reference, so the constant
	// names are resolved back to their values here rather than duplicated.
	constants := map[string]string{
		"BlockSchemaRemovedAfterApplyStarted":   checkstate.BlockSchemaRemovedAfterApplyStarted,
		"BlockRollbackCompleted":                checkstate.BlockRollbackCompleted,
		"BlockApplyCancelled":                   checkstate.BlockApplyCancelled,
		"BlockApplyCancelledAfterTaskCompleted": checkstate.BlockApplyCancelledAfterTaskCompleted,
		"BlockConfigDiscoveryUnavailable":       checkstate.BlockConfigDiscoveryUnavailable,
		"BlockConfigDiscoveryFailed":            checkstate.BlockConfigDiscoveryFailed,
		"BlockPlanPublishVerificationFailed":    checkstate.BlockPlanPublishVerificationFailed,
		"BlockPRFileCapExceeded":                checkstate.BlockPRFileCapExceeded,
		"BlockManagedDirMissingConfig":          checkstate.BlockManagedDirMissingConfig,
		"BlockNoAllowedConfiguredEnvironments":  checkstate.BlockNoAllowedConfiguredEnvironments,
		"BlockParticipantUnresolved":            checkstate.BlockParticipantUnresolved,
		"BlockReviewTimeDeploymentDrift":        checkstate.BlockReviewTimeDeploymentDrift,
	}

	found := map[string]string{}
	for _, file := range files {
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || (gen.Tok != token.VAR && gen.Tok != token.CONST) {
				continue
			}
			for _, spec := range gen.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if !ok || len(value.Names) != 1 || len(value.Values) != 1 {
					continue
				}
				name := value.Names[0].Name
				switch declared := value.Values[0].(type) {
				case *ast.CompositeLit:
					ident, ok := declared.Type.(*ast.Ident)
					if !ok || ident.Name != "checkBlockReason" {
						continue
					}
					reason, ok := blockingReasonFromLiteral(declared, constants)
					require.True(t, ok, "could not resolve the blocking reason of %s", name)
					found[name] = reason
				case *ast.SelectorExpr:
					// A bare alias of a checkstate block constant is a reason
					// declaration too, even without the surrounding struct.
					pkg, ok := declared.X.(*ast.Ident)
					if !ok || pkg.Name != "checkstate" || !strings.HasPrefix(declared.Sel.Name, "Block") {
						continue
					}
					reason, ok := constants[declared.Sel.Name]
					require.True(t, ok, "%s names checkstate.%s, which this test does not know; add it to the constants map",
						name, declared.Sel.Name)
					found[name] = reason
				}
			}
		}
	}
	return found
}

func blockingReasonFromLiteral(lit *ast.CompositeLit, constants map[string]string) (string, bool) {
	for _, element := range lit.Elts {
		kv, ok := element.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok || key.Name != "blockingReason" {
			continue
		}
		selector, ok := kv.Value.(*ast.SelectorExpr)
		if !ok {
			return "", false
		}
		reason, ok := constants[selector.Sel.Name]
		return reason, ok
	}
	return "", false
}
