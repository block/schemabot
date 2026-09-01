package enginetest

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// TestContractCaseCoverage holds the contract-case registry, the Harness
// fixture fields, and the engine.Engine method set in lockstep. A new Harness
// fixture must be consumed by exactly one registered case, and every
// engine.Engine method must either be pinned by a case or carry a documented
// exclusion. Runs without a database.
func TestContractCaseCoverage(t *testing.T) {
	harnessType := reflect.TypeFor[Harness]()
	engineType := reflect.TypeFor[engine.Engine]()

	seenCases := make(map[Case]bool, len(contractCases))
	claimedFields := make(map[string]Case, len(contractCases))
	pinnedMethods := make(map[string]bool)
	distinctRunFuncPointers := make(map[uintptr]bool, len(contractCases))
	for _, c := range contractCases {
		require.False(t, seenCases[c.name], "case %q is registered more than once", c.name)
		seenCases[c.name] = true

		field, ok := harnessType.FieldByName(c.harnessField)
		require.True(t, ok, "case %q names Harness fixture field %q, which does not exist", c.name, c.harnessField)
		require.Equal(t, c.fixtureType, field.Type,
			"case %q names Harness field %q with an unexpected fixture signature", c.name, c.harnessField)
		prev, claimed := claimedFields[c.harnessField]
		require.False(t, claimed, "Harness fixture %q is consumed by both case %q and case %q", c.harnessField, prev, c.name)
		claimedFields[c.harnessField] = c.name

		distinctRunFuncPointers[reflect.ValueOf(c.run).Pointer()] = true

		for _, method := range c.engineMethods {
			_, ok := engineType.MethodByName(method)
			require.True(t, ok, "case %q pins engine.Engine method %q, which does not exist", c.name, method)
			_, excluded := engineMethodExclusions[method]
			require.False(t, excluded,
				"engine.Engine method %q is pinned by case %q and excluded in engineMethodExclusions — remove one so coverage stays honest", method, c.name)
			pinnedMethods[method] = true
		}
	}
	require.Len(t, distinctRunFuncPointers, len(contractCases), "each contract case must have its own run function")

	// This reflection ratchet follows pkg/storage/storagetest/storagetest.go;
	// extract the pattern if a third package needs it.
	nonFixtureFields := map[string]string{
		"Skips": "suite configuration documenting unsupported contract cases",
	}
	var unclaimed []string
	for field := range harnessType.Fields() {
		if reason, excluded := nonFixtureFields[field.Name]; excluded {
			assert.NotEmpty(t, reason, "Harness field %q is excluded without a reason", field.Name)
			continue
		}
		if _, ok := claimedFields[field.Name]; !ok {
			unclaimed = append(unclaimed, field.Name)
		}
	}
	sort.Strings(unclaimed)
	assert.Empty(t, unclaimed, "Harness fields with neither a contract case nor a documented exclusion")

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
	assert.Empty(t, unclassified, "engine.Engine methods with neither a contract case nor a documented exclusion")

	var staleExclusions []string
	for method := range engineMethodExclusions {
		if !engineMethods[method] {
			staleExclusions = append(staleExclusions, method)
		}
	}
	sort.Strings(staleExclusions)
	assert.Empty(t, staleExclusions, "excluded methods missing from engine.Engine")
}

// optionalCapabilityDecisions must classify every exported optional interface
// in package engine. Add new optional capabilities here with an explicit reason.
var optionalCapabilityDecisions = map[reflect.Type]string{
	reflect.TypeFor[engine.Drainer]():                         "lifecycle cleanup is outside the error-typing contract",
	reflect.TypeFor[engine.ShutdownHalter]():                  "shutdown behavior has engine-specific tests",
	reflect.TypeFor[engine.DeferredCutoverSignalChecker]():    "recovery signaling has engine-specific tests",
	reflect.TypeFor[engine.ExternallyAuthoritativeProgress](): "routing policy is outside the conformance suite",
	reflect.TypeFor[engine.SynchronousWorkRegistration]():     "registration timing is outside the conformance suite",
	reflect.TypeFor[engine.ControlResumeValidator]():          "resume metadata is engine-specific",
}

func TestOptionalCapabilityCoverage(t *testing.T) {
	interfaces := exportedInterfaces(t, "..")
	delete(interfaces, "Engine")
	for typ, reason := range optionalCapabilityDecisions {
		assert.NotEmpty(t, reason, "optional engine capability %s has no conformance decision", typ.Name())
		assert.True(t, interfaces[typ.Name()], "classified optional engine capability %s does not exist", typ.Name())
		delete(interfaces, typ.Name())
	}
	assert.Empty(t, interfaces, "exported optional engine interfaces without a conformance decision")
}

func TestCaseConstantCoverage(t *testing.T) {
	constants := caseConstants(t)
	for _, c := range contractCases {
		delete(constants, c.name)
	}
	assert.Empty(t, constants, "Case constants missing from contractCases")
}

// exportedInterfaces detects exported interface declarations written as named
// type definitions. Interface type aliases are outside its syntactic scope.
func exportedInterfaces(t *testing.T, dir string) map[string]bool {
	t.Helper()
	interfaces := make(map[string]bool)
	for _, file := range parsePackageFiles(t, dir, "engine", false) {
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if ok && spec.Name.IsExported() {
				if _, ok := spec.Type.(*ast.InterfaceType); ok {
					interfaces[spec.Name.Name] = true
				}
			}
			return true
		})
	}
	return interfaces
}

// caseConstants detects constants whose ValueSpec explicitly names Case and
// whose value is a string literal. Untyped constants in grouped const blocks
// are outside its syntactic scope, including declarations that inherit a type.
func caseConstants(t *testing.T) map[Case]bool {
	t.Helper()
	constants := make(map[Case]bool)
	for _, file := range parsePackageFiles(t, ".", "enginetest", true) {
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.ValueSpec)
			if !ok {
				return true
			}
			typ, ok := spec.Type.(*ast.Ident)
			if !ok || typ.Name != "Case" {
				return true
			}
			for _, value := range spec.Values {
				literal, ok := value.(*ast.BasicLit)
				if ok {
					constants[Case(literal.Value[1:len(literal.Value)-1])] = true
				}
			}
			return true
		})
	}
	return constants
}

func parsePackageFiles(t *testing.T, dir, packageName string, includeTests bool) []*ast.File {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	files := make([]*ast.File, 0, len(entries))
	fileSet := token.NewFileSet()
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || (!includeTests && strings.HasSuffix(entry.Name(), "_test.go")) {
			continue
		}
		file, err := parser.ParseFile(fileSet, filepath.Join(dir, entry.Name()), nil, 0)
		require.NoError(t, err)
		if file.Name.Name == packageName {
			files = append(files, file)
		}
	}
	return files
}

func TestRunExecutesEveryRegisteredCase(t *testing.T) {
	executed := make(map[Case]int)
	recorders := make(map[Case]*methodRecorder, len(contractCases))
	engineFor := func(c Case) fakeEngine {
		recorder := &methodRecorder{}
		recorders[c] = recorder
		return fakeEngine{recorder: recorder}
	}
	markControl := func(c Case) func(*testing.T) ControlFixture {
		return func(*testing.T) ControlFixture {
			executed[c]++
			return ControlFixture{Engine: engineFor(c), Req: &engine.ControlRequest{Database: string(c)}}
		}
	}
	Run(t, Harness{
		CancelAlreadyCompleted: markControl(CaseCancelAlreadyCompleted),
		StopAlreadyCompleted:   markControl(CaseStopAlreadyCompleted),
		CancelNonexistent:      markControl(CaseCancelNonexistent),
		StopNonexistent:        markControl(CaseStopNonexistent),
		TerminalProgress: func(*testing.T) []ProgressFixture {
			executed[CaseProgressTerminalTruth]++
			return []ProgressFixture{{Name: "completed", Engine: engineFor(CaseProgressTerminalTruth), Req: &engine.ProgressRequest{}, Want: engine.StateCompleted}}
		},
		NotReady: func(*testing.T) NotReadyFixture {
			executed[CaseNotReadyDistinguishable]++
			return NotReadyFixture{Invoke: func(context.Context) error { return engine.NewNotReadyError("not ready") }}
		},
	})
	for _, c := range contractCases {
		assert.Equal(t, 1, executed[c.name], "registered case %q did not execute exactly once", c.name)
		var invoked []string
		if recorder := recorders[c.name]; recorder != nil {
			invoked = recorder.methods()
		}
		assert.ElementsMatch(t, c.engineMethods, invoked,
			"registered case %q did not invoke exactly its declared engine.Engine methods", c.name)
	}
}

type methodRecorder struct {
	mu      sync.Mutex
	invoked []string
}

func (r *methodRecorder) record(method string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.invoked = append(r.invoked, method)
}

func (r *methodRecorder) methods() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.invoked...)
}

type fakeEngine struct {
	recorder *methodRecorder
}

func (f fakeEngine) Name() string {
	f.recorder.record("Name")
	return "fake"
}
func (f fakeEngine) Plan(context.Context, *engine.PlanRequest) (*engine.PlanResult, error) {
	f.recorder.record("Plan")
	return nil, nil
}
func (f fakeEngine) Apply(context.Context, *engine.ApplyRequest) (*engine.ApplyResult, error) {
	f.recorder.record("Apply")
	return nil, nil
}
func (f fakeEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	f.recorder.record("Progress")
	return &engine.ProgressResult{State: engine.StateCompleted}, nil
}
func (f fakeEngine) Stop(_ context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	f.recorder.record("Stop")
	if req.Database == string(CaseStopNonexistent) {
		return nil, engine.NewPermanentError("not found")
	}
	return nil, engine.NewAlreadyCompletedError("completed")
}
func (f fakeEngine) Cancel(_ context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	f.recorder.record("Cancel")
	if req.Database == string(CaseCancelNonexistent) {
		return nil, engine.NewPermanentError("not found")
	}
	return nil, engine.NewAlreadyCompletedError("completed")
}
func (f fakeEngine) Start(context.Context, *engine.ControlRequest) (*engine.ControlResult, error) {
	f.recorder.record("Start")
	return nil, nil
}
func (f fakeEngine) Cutover(context.Context, *engine.ControlRequest) (*engine.ControlResult, error) {
	f.recorder.record("Cutover")
	return nil, nil
}
func (f fakeEngine) Revert(context.Context, *engine.ControlRequest) (*engine.ControlResult, error) {
	f.recorder.record("Revert")
	return nil, nil
}
func (f fakeEngine) SkipRevert(context.Context, *engine.ControlRequest) (*engine.ControlResult, error) {
	f.recorder.record("SkipRevert")
	return nil, nil
}
func (f fakeEngine) Volume(context.Context, *engine.VolumeRequest) (*engine.VolumeResult, error) {
	f.recorder.record("Volume")
	return nil, nil
}
