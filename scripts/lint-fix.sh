#!/bin/bash
#
# Runs golangci-lint with --fix on staged Go files.
# Used by the pre-commit hook.
#
# Auto-fixable issues (formatting, imports, whitespace) are fixed automatically.
# Non-auto-fixable issues require manual fixes before committing.
#
# How it works:
#
#   1. User stages bad file:     git add service.go
#   2. User commits:             git commit -m "..."  (triggers pre-commit hook)
#   3. Hook runs lint --fix:     golangci-lint --fix ./pkg/...  (fixes working tree)
#   4. Hook re-stages:           git add service.go  (staging area gets fixed version)
#   5. Hook verifies:            golangci-lint run ./pkg/...  (confirms no remaining issues)
#   6. Commit proceeds with the fixed version

set -e

STAGED_GO_FILES=$(git diff --cached --name-only --diff-filter=ACM | grep '\.go$' || true)

if [ -z "$STAGED_GO_FILES" ]; then
    exit 0
fi

# Use local golangci-lint if available, otherwise Docker.
# Check common Go binary paths since git hooks may not inherit the full user PATH.
LINT_CMD=""
for candidate in golangci-lint "$HOME/go/bin/golangci-lint" "$GOPATH/bin/golangci-lint" "$GOBIN/golangci-lint"; do
    if command -v "$candidate" >/dev/null 2>&1; then
        LINT_CMD="$candidate"
        break
    fi
done
if [ -z "$LINT_CMD" ]; then
    LINT_CMD="docker run --rm -v $(pwd):/app -w /app golangci/golangci-lint:latest golangci-lint"
fi

# Separate files by build tag requirements:
# - e2e/consumermodule/ files belong to a nested Go module; root package
#   patterns (./e2e/...) never descend into it, so it lints from its own
#   directory (mirroring the CI lint matrix's consumer-module job)
# - other e2e/ files need the e2e build tag
# - integration/ files need the integration build tag (test-only package)
# - everything else runs with default + integration tags
HAS_CONSUMER_MODULE=$(echo "$STAGED_GO_FILES" | grep -c '^e2e/consumermodule/' || true)
HAS_E2E=$(echo "$STAGED_GO_FILES" | grep -v '^e2e/consumermodule/' | grep -c '^e2e/' || true)
HAS_INTEGRATION_DIR=$(echo "$STAGED_GO_FILES" | grep -c '^integration/' || true)
PKG_FILES=$(echo "$STAGED_GO_FILES" | grep -v '^e2e/' | grep -v '^integration/' || true)

# Detect the merge-base so we only flag issues introduced by this branch.
# If merge-base equals HEAD (e.g., after git reset --soft for squashing),
# skip --new-from-rev to avoid treating every changed line as "new".
NEW_FROM_REV=""
for base_branch in origin/main origin/master; do
    if git rev-parse --verify "$base_branch" >/dev/null 2>&1; then
        MERGE_BASE=$(git merge-base HEAD "$base_branch" 2>/dev/null || true)
        if [ -n "$MERGE_BASE" ] && [ "$MERGE_BASE" != "$(git rev-parse HEAD)" ]; then
            NEW_FROM_REV="$MERGE_BASE"
        fi
        break
    fi
done

lint_and_fix() {
    local build_tags="$1"
    shift
    local packages=("$@")
    local tag_flag=""
    local new_flag=""

    if [ -n "$build_tags" ]; then
        tag_flag="--build-tags=$build_tags"
    fi

    if [ -n "$NEW_FROM_REV" ]; then
        new_flag="--new-from-rev=$NEW_FROM_REV"
    fi

    echo "Running golangci-lint --fix${build_tags:+ ($build_tags)}..."
    $LINT_CMD run --fix --timeout=5m $tag_flag "${packages[@]}" || true

    # Re-stage any files that were fixed
    for file in $STAGED_GO_FILES; do
        if [ -f "$file" ] && ! git diff --quiet "$file" 2>/dev/null; then
            echo "Auto-fixed: $file"
            git add "$file"
        fi
    done

    # Verify no remaining issues
    if ! $LINT_CMD run --timeout=5m $tag_flag $new_flag "${packages[@]}"; then
        echo ""
        echo "golangci-lint found issues that cannot be auto-fixed."
        echo "Please fix them manually before committing."
        exit 1
    fi
}

# Lint regular packages (default + integration build tags)
if [ -n "$PKG_FILES" ]; then
    PACKAGES=$(echo "$PKG_FILES" | xargs -n1 dirname | sort -u | sed 's|^|./|' | sed 's|$|/...|')
    lint_and_fix "" $PACKAGES
    lint_and_fix "integration" $PACKAGES
fi

# Lint integration/ directory files with the integration build tag.
if [ "$HAS_INTEGRATION_DIR" -gt 0 ]; then
    lint_and_fix "integration" ./integration/...
fi

# Lint e2e files with both e2e and integration build tags.
if [ "$HAS_E2E" -gt 0 ]; then
    for tag in e2e integration; do
        lint_and_fix "$tag" ./e2e/...
    done
fi

# Lint the nested consumer module from its own directory. Root package
# patterns never reach a nested module, so without this branch its files
# would only be checked in CI.
if [ "$HAS_CONSUMER_MODULE" -gt 0 ]; then
    CM_DIR="e2e/consumermodule"
    CM_LINT_CMD="$LINT_CMD"
    if [[ "$LINT_CMD" == docker* ]]; then
        CM_LINT_CMD="docker run --rm -v $(pwd):/app -w /app/$CM_DIR golangci/golangci-lint:latest golangci-lint"
    fi

    echo "Running golangci-lint --fix (consumer module)..."
    (cd "$CM_DIR" && $CM_LINT_CMD run --fix --timeout=5m ./...) || true

    # Re-stage any files that were fixed
    for file in $STAGED_GO_FILES; do
        if [ -f "$file" ] && ! git diff --quiet "$file" 2>/dev/null; then
            echo "Auto-fixed: $file"
            git add "$file"
        fi
    done

    CM_NEW_FLAG=""
    if [ -n "$NEW_FROM_REV" ]; then
        CM_NEW_FLAG="--new-from-rev=$NEW_FROM_REV"
    fi
    if ! (cd "$CM_DIR" && $CM_LINT_CMD run --timeout=5m $CM_NEW_FLAG ./...); then
        echo ""
        echo "golangci-lint found issues in $CM_DIR that cannot be auto-fixed."
        echo "Please fix them manually before committing."
        exit 1
    fi
fi

# Run closeandlog analyzer on staged packages to flag _ = x.Close() patterns.
# Mirrors the build-tag matrix used for golangci-lint above. The build tags go
# through GOFLAGS rather than `go run -tags=...`: that flag selects the files
# compiled into the checker, while the tags the checker must analyze under are
# the ones its package loader sees, and singlechecker's own -tags flag is a no-op.
run_closeandlog() {
    local build_tags="$1"
    shift
    local packages=("$@")

    if ! GOFLAGS=${build_tags:+-tags=$build_tags} go run ./cmd/closeandlog-check "${packages[@]}" 2>&1; then
        echo ""
        echo "closeandlog: use utils.CloseAndLog(x) instead of discarding Close() errors."
        echo "See: https://github.com/block/spirit/blob/main/pkg/utils/close.go"
        exit 1
    fi
}

if [ -n "$PKG_FILES" ]; then
    CLOSEANDLOG_PKGS=$(echo "$PKG_FILES" | xargs -n1 dirname | sort -u | sed 's|^|./|' | sed 's|$|/...|')
    echo "Running closeandlog analyzer..."
    run_closeandlog "" $CLOSEANDLOG_PKGS
    run_closeandlog "integration" $CLOSEANDLOG_PKGS
    run_closeandlog "e2e" $CLOSEANDLOG_PKGS
fi

# The integration/ and e2e/ directories are test-only packages that the pkg
# patterns above never reach, so each is checked under the tag that builds it.
if [ "$HAS_INTEGRATION_DIR" -gt 0 ]; then
    echo "Running closeandlog analyzer (integration/)..."
    run_closeandlog "integration" ./integration/...
fi

if [ "$HAS_E2E" -gt 0 ]; then
    echo "Running closeandlog analyzer (e2e/)..."
    run_closeandlog "e2e" ./e2e/...
fi

# Run webhookheaders analyzer when any pkg/webhook/ source (excluding templates)
# is staged. Flags inline `## ...` markdown headers that should live in
# pkg/webhook/templates/ and render via templates.Render…
if [ -n "$PKG_FILES" ]; then
    WEBHOOK_FILES=$(echo "$PKG_FILES" | grep '^pkg/webhook/' | grep -v '^pkg/webhook/templates/' || true)
    if [ -n "$WEBHOOK_FILES" ]; then
        # `grep -v` exits non-zero (and would trip `set -e`) when every package
        # is filtered out, so suppress that exit and fall through to a clear
        # message instead of dying silently.
        WEBHOOK_PKGS=$(go list ./pkg/webhook/... | grep -v '/templates$' || true)
        if [ -z "$WEBHOOK_PKGS" ]; then
            echo "webhookheaders: no packages to check (only pkg/webhook/templates exists), skipping."
        else
            echo "Running webhookheaders analyzer..."
            if ! go run ./cmd/webhookheaders-check $WEBHOOK_PKGS 2>&1; then
                echo ""
                echo 'webhookheaders: move the `## ...` body into pkg/webhook/templates/ and render it via a templates.Render… helper.'
                exit 1
            fi
        fi
    fi
fi

# Run severityglyphs analyzer on staged packages to flag severity glyph
# literals that should reference the pkg/glyph constants instead. The analyzer
# skips test files itself (rendered-output assertions deliberately pin literal
# glyphs); pkg/glyph (the vocabulary's home) and the analyzer's own glyph
# table are excluded here. Mirrors the closeandlog build-tag matrix.
run_severityglyphs() {
    local build_tags="$1"
    shift
    local patterns=("$@")
    local tag_flag=""

    if [ -n "$build_tags" ]; then
        tag_flag="-tags=$build_tags"
    fi

    # `go list` takes the tags as a flag; the analyzer takes them via GOFLAGS.
    # Fail closed: if go list can't resolve the staged packages, the analyzer
    # can't vouch for them, so block the commit rather than silently skipping.
    local listed
    if ! listed=$(go list $tag_flag "${patterns[@]}"); then
        echo "severityglyphs: go list failed for staged packages; commit blocked until it resolves."
        exit 1
    fi

    # `grep -v` exits non-zero (and would trip `set -e`) when every package
    # is filtered out, so suppress that exit and skip the run instead.
    local packages
    packages=$(echo "$listed" | grep -v '/pkg/glyph$' | grep -v '/pkg/analyzers/severityglyphs' | grep -v '/testdata/' || true)
    if [ -z "$packages" ]; then
        echo "severityglyphs: all staged packages are excluded from the check; skipping."
        return 0
    fi

    if ! GOFLAGS=${build_tags:+-tags=$build_tags} go run ./cmd/severityglyphs-check $packages 2>&1; then
        echo ""
        echo "severityglyphs: use the pkg/glyph constants (glyph.Escalation, glyph.Refused,"
        echo "glyph.Failed, glyph.Attention, glyph.Info) instead of literal severity glyphs."
        exit 1
    fi
}

if [ -n "$PKG_FILES" ]; then
    SEVERITYGLYPHS_PKGS=$(echo "$PKG_FILES" | xargs -n1 dirname | sort -u | sed 's|^|./|' | sed 's|$|/...|')
    echo "Running severityglyphs analyzer..."
    run_severityglyphs "" $SEVERITYGLYPHS_PKGS
    run_severityglyphs "integration" $SEVERITYGLYPHS_PKGS
fi

echo "All lint checks passed!"
