#!/usr/bin/env bash
# Run one LocalScale test shard so that a hang or a kill names the test.
#
# Why this exists: when a shard stops making progress, the only thing that
# makes it diagnosable is output that has already reached the log by the time
# the process dies. Two properties are needed for that, and the obvious
# invocation has neither.
#
# 1. Output has to stream. `go test -v` writes per-test output directly only
#    when the pattern resolves to a single package; as soon as it resolves to
#    more, each package's output is held until that package finishes, and a
#    shard killed mid-package emits nothing at all -- not one `=== RUN` line.
#    Testing one package per invocation restores streaming without dropping a
#    package from the run. Piping the result through anything that buffers to
#    EOF (`tail`, `sort`) throws the same diagnosability away again, because a
#    killed process never reaches EOF.
#
# 2. Go's own timeout has to fire before the runner is killed. That timeout
#    panics with a goroutine dump headed by `running tests: <name>`, which
#    names the stuck test outright; the job timeout just kills the runner and
#    says nothing. Go's timeout is per invocation and measured from when the
#    test binary starts, while the job timeout covers setup too, so a
#    per-invocation value cannot on its own be held below the job timeout.
#    This script spends a single wall-clock budget across the packages,
#    passing each invocation only the time left, which bounds the whole step
#    regardless of how many packages there are.
#
# The budget is therefore tied to the `timeout-minutes` of the calling job in
# .github/workflows/test.yaml: it must stay under that, minus worst-case setup
# (checkout, Go config, cache restore, image pre-pull, image build and test
# compile together take a bit over 2.5 minutes), minus enough room for the
# goroutine dump to be written and the step to report. Raising either value to
# quiet a slow shard defeats the point -- a shard that needs longer than a
# healthy one is the thing this output exists to identify. If the job's
# timeout changes, change the default here to match.
#
# Usage: run-localscale-shard.sh <run-filter>
#
# LOCALSCALE_BUDGET_SECONDS overrides the budget for local runs.

set -uo pipefail

if [ "$#" -ne 1 ] || [ -z "${1:-}" ]; then
	echo "::error::usage: run-localscale-shard.sh <run-filter>" >&2
	exit 2
fi

run_filter="$1"
budget="${LOCALSCALE_BUDGET_SECONDS:-360}"

# Below this there is not enough time left for a package to reach a verdict,
# so stop and say so rather than starting one that is certain to time out.
min_slice=30

# Only packages that carry test files need a slice of the budget. Selecting
# them by what they contain rather than by name means a package that grows its
# first test joins the run on its own, so nothing is left untested by
# omission.
packages=$(go list -tags=integration \
	-f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' \
	./pkg/localscale/...)
if [ -z "$packages" ]; then
	echo "::error::no LocalScale package contains test files" >&2
	exit 1
fi

started=$(date +%s)
status=0

for pkg in $packages; do
	remaining=$((budget - ($(date +%s) - started)))
	if [ "$remaining" -lt "$min_slice" ]; then
		echo "::error::LocalScale shard exhausted its ${budget}s budget with ${pkg} untested; failing the shard rather than reporting a pass that skipped a package"
		status=1
		break
	fi
	# Each package gets whatever is left, so Go reports the hang itself
	# instead of the runner being killed with nothing written.
	go test -tags=integration -timeout="${remaining}s" -count=1 -v \
		-run "$run_filter" "$pkg" || status=1
done

exit "$status"
