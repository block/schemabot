package enginetest

import (
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// TestContractCaseCoverage holds the contract-case registry, the Harness
// fixture fields, and the engine.Engine method set in lockstep. A new Harness
// fixture must be consumed by exactly one registered case, each case's subtest
// must be the run function named for it, and every engine.Engine method must
// either be pinned by a case or carry a documented exclusion — so a new engine
// capability cannot land without a conformance decision. Runs without a
// database.
func TestContractCaseCoverage(t *testing.T) {
	harnessType := reflect.TypeFor[Harness]()
	engineType := reflect.TypeFor[engine.Engine]()

	seenCases := make(map[Case]bool, len(contractCases))
	claimedFields := make(map[string]Case, len(contractCases))
	pinnedMethods := make(map[string]bool)
	for _, c := range contractCases {
		require.False(t, seenCases[c.name], "case %q is registered more than once", c.name)
		seenCases[c.name] = true

		field, ok := harnessType.FieldByName(c.harnessField)
		require.True(t, ok, "case %q names Harness fixture field %q, which does not exist", c.name, c.harnessField)
		require.Equal(t, reflect.Func, field.Type.Kind(),
			"case %q names Harness field %q, which is not a fixture function", c.name, c.harnessField)
		prev, claimed := claimedFields[c.harnessField]
		require.False(t, claimed, "Harness fixture %q is consumed by both case %q and case %q", c.harnessField, prev, c.name)
		claimedFields[c.harnessField] = c.name

		runFunc := runtime.FuncForPC(reflect.ValueOf(c.run).Pointer())
		require.NotNil(t, runFunc, "resolve the run function for case %q", c.name)
		require.True(t, strings.HasSuffix(runFunc.Name(), "."+expectedRunName(c.name)),
			"case %q runs %q; want %s", c.name, runFunc.Name(), expectedRunName(c.name))

		for _, method := range c.engineMethods {
			_, ok := engineType.MethodByName(method)
			require.True(t, ok, "case %q pins engine.Engine method %q, which does not exist", c.name, method)
			_, excluded := engineMethodExclusions[method]
			require.False(t, excluded,
				"engine.Engine method %q is pinned by case %q and excluded in engineMethodExclusions — remove one so coverage stays honest", method, c.name)
			pinnedMethods[method] = true
		}
	}

	// Every fixture field on Harness is consumed by a registered case; a
	// fixture with no case would be dead weight the suite silently ignores.
	// Non-function fields (Skips) are configuration, not fixtures.
	var unclaimed []string
	for field := range harnessType.Fields() {
		if field.Type.Kind() != reflect.Func {
			continue
		}
		if _, ok := claimedFields[field.Name]; !ok {
			unclaimed = append(unclaimed, field.Name)
		}
	}
	sort.Strings(unclaimed)
	require.Empty(t, unclaimed, "Harness fixture fields no contract case consumes")

	// Every engine.Engine method is classified: pinned by a case or excluded
	// with a documented reason.
	var unclassified []string
	engineMethods := make(map[string]bool, engineType.NumMethod())
	for method := range engineType.Methods() {
		engineMethods[method.Name] = true
		if reason, excluded := engineMethodExclusions[method.Name]; excluded {
			require.NotEmpty(t, reason,
				"engine.Engine method %q is excluded without a reason — document why the suite does not pin it", method.Name)
			continue
		}
		if !pinnedMethods[method.Name] {
			unclassified = append(unclassified, method.Name)
		}
	}
	sort.Strings(unclassified)
	require.Empty(t, unclassified, "engine.Engine methods with neither a contract case nor a documented exclusion")

	var staleExclusions []string
	for method := range engineMethodExclusions {
		if !engineMethods[method] {
			staleExclusions = append(staleExclusions, method)
		}
	}
	sort.Strings(staleExclusions)
	require.Empty(t, staleExclusions, "excluded methods missing from engine.Engine")
}

// expectedRunName maps a case name to its run function's name: each hyphened
// word capitalized, prefixed with "run" (cancel-already-completed →
// runCancelAlreadyCompleted).
func expectedRunName(c Case) string {
	var b strings.Builder
	b.WriteString("run")
	for part := range strings.SplitSeq(string(c), "-") {
		if part == "" {
			continue
		}
		b.WriteString(strings.ToUpper(part[:1]))
		b.WriteString(part[1:])
	}
	return b.String()
}
