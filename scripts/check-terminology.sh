#!/bin/bash
set -euo pipefail

# Fail when "schema change" appears hyphenated in prose. The rule and its
# rationale live in AGENTS.md (Terminology); this makes it self-maintaining
# instead of relying on the next sweep to notice.
#
# Identifiers are exempt and are carved out mechanically rather than by
# judgement: URLs and markdown anchor targets are stripped before matching,
# the match must start at a non-letter boundary so the literal inside a
# longer word (VSchema-changed) is skipped, the legitimate hyphenated
# identifiers (script names and TEMPLATES.md anchors the docs tooling
# refers to) are allowlisted by value, and the files that must contain
# the literal (the rule text, the generated snapshot, this script, and the
# generator script's own name) are skipped by path.

cd "$(git rev-parse --show-toplevel)"

ALLOWLIST='generate-schema-change|schema-change-branch|schema-change-apply-automatic'

# grep exits 1 for "no match", which is a normal outcome at every stage
# below. Anything higher is a real failure (bad pattern, unreadable tree)
# and must abort the check rather than pass it as a clean tree.
tolerate_no_match() {
    local status=0
    "$@" || status=$?
    if [ "$status" -gt 1 ]; then
        echo "error: '$*' failed with status $status" >&2
        exit 2
    fi
}

hits=$(tolerate_no_match git grep -n -I -i 'schema-change' -- . \
        ':!AGENTS.md' \
        ':!TEMPLATES.md' \
        ':!scripts/check-terminology.sh' \
        ':!scripts/generate-schema-change.sh' \
    | perl -pe 's{https?://\S+}{}g; s{\(#[^)]*\)}{}g' \
    | tolerate_no_match grep -i -E '(^|[^A-Za-z])schema-change' \
    | tolerate_no_match grep -v -i -E "$ALLOWLIST")

if [ -n "$hits" ]; then
    echo "$hits"
    echo 'error: write "schema change" without a hyphen in prose (see AGENTS.md, Terminology)' >&2
    exit 1
fi

echo 'terminology check passed: no hyphenated "schema-change" in prose'
