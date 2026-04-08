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
# - e2e/ files need the e2e build tag
# - integration/ files need the integration build tag (test-only package)
# - everything else runs with default + integration tags
HAS_E2E=$(echo "$STAGED_GO_FILES" | grep -c '^e2e/' || true)
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

# Run closeandlog analyzer on staged packages to flag _ = x.Close() patterns.
# Mirrors the build-tag matrix used for golangci-lint above.
run_closeandlog() {
    local build_tags="$1"
    shift
    local packages=("$@")
    local tag_flag=""

    if [ -n "$build_tags" ]; then
        tag_flag="-tags=$build_tags"
    fi

    if ! go run $tag_flag ./cmd/closeandlog-check "${packages[@]}" 2>&1; then
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
fi

# e2e packages are test-only (_test.go) so singlechecker can't analyze them.
# The e2e tests reuse the same patterns caught by the default + integration runs.

echo "All lint checks passed!"
