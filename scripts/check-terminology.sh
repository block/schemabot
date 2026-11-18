#!/bin/bash
set -euo pipefail

# Fail when "schema change" appears hyphenated in prose, or when "cutover"
# is written as two words in the docs. The rules and their rationale live in
# AGENTS.md (Terminology); this makes them self-maintaining instead of
# relying on the next sweep to notice.
#
# Identifiers are exempt and are carved out mechanically rather than by
# judgement: URLs and markdown anchor targets are stripped before matching,
# the match must start at a non-letter boundary so the literal inside a
# longer word (VSchema-changed) is skipped, the legitimate hyphenated
# identifiers (script names and TEMPLATES.md anchors the docs tooling
# refers to) are allowlisted by value, and the files that must contain
# the literal (the generated snapshot, this script, and the generator
# script's own name) are skipped by path. AGENTS.md is checked like any other
# file except for its Terminology section, which quotes the forms it forbids.

cd "$(git rev-parse --show-toplevel)"

ALLOWLIST='generate-schema-change|schema-change-branch|schema-change-apply-automatic'

# The Terminology section of AGENTS.md runs from its "**Terminology:**"
# paragraph up to the "**Driver terminology:**" one. Both markers must be
# found, so a renamed heading fails the check instead of exempting the file.
terminology_start=$(grep -n -m1 '^\*\*Terminology:\*\*' AGENTS.md | cut -d: -f1 || true)
terminology_end=$(grep -n -m1 '^\*\*Driver terminology:\*\*' AGENTS.md | cut -d: -f1 || true)
if [ -z "$terminology_start" ] || [ -z "$terminology_end" ]; then
    echo 'error: could not find the Terminology section of AGENTS.md (from "**Terminology:**" to "**Driver terminology:**")' >&2
    exit 2
fi
export terminology_start terminology_end

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
        ':!TEMPLATES.md' \
        ':!scripts/check-terminology.sh' \
        ':!scripts/generate-schema-change.sh' \
    | perl -ne 'print unless /^AGENTS\.md:(\d+):/ && $1 >= $ENV{terminology_start} && $1 < $ENV{terminology_end}' \
    | ALLOWLIST="$ALLOWLIST" perl -pe 's{https?://\S+}{}g; s{\(#[^)]*\)}{}g; s{(?<![A-Za-z0-9_-])(?:$ENV{ALLOWLIST})(?![A-Za-z0-9_-])}{}gi' \
    | tolerate_no_match grep -i -E '(^|[^A-Za-z])schema-change')

if [ -n "$hits" ]; then
    echo "$hits"
    echo 'error: write "schema change" without a hyphen in prose (see AGENTS.md, Terminology)' >&2
    exit 1
fi

# "cutover" is one word in the docs, the generated TEMPLATES.md included, so
# the PR comment and CLI text it previews are held to the rule too. The verb
# forms are what get split, so those are what is matched, joined by a hyphen
# or by any run of spaces that stays inside one Markdown block: a line break
# within a paragraph renders as a space, so "cut" ending one line and "over"
# starting the next is a hit, while a blank line between them separates two
# blocks and is not. "cutting over" is exempt: it is the name of the
# cutting_over state that the PR comment and CLI show. URLs and inline code
# spans are blanked first, so a link or a quoted engine flag is not a hit,
# and so is the Terminology section of AGENTS.md. Blanking keeps every
# offset, so each hit reports its own line.
find_split_cutover() {
    git ls-files -z -- '*.md' | xargs -0 perl -0777 -ne '
        my $text = $_;
        $text =~ s{https?://\S+}{" " x length($&)}ge;
        $text =~ s{`[^`\n]+`}{" " x length($&)}ge;
        if ($ARGV eq "AGENTS.md") {
            my @lines = split /\n/, $text, -1;
            for my $i ($ENV{terminology_start} - 1 .. $ENV{terminology_end} - 2) {
                $lines[$i] = " " x length($lines[$i]);
            }
            $text = join "\n", @lines;
        }
        while ($text =~ /(?<![A-Za-z_])cuts?(?:-|[ \t]+\n?[ \t]*|\n[ \t]*)over(?![A-Za-z])/gi) {
            my $start = $-[0];
            my $line = 1 + (substr($text, 0, $start) =~ tr/\n//);
            my $from = rindex($text, "\n", $start) + 1;
            my $to = index($text, "\n", $start);
            $to = length($text) if $to < 0;
            print "$ARGV:$line:", substr($_, $from, $to - $from), "\n";
        }
    '
}
cutover_hits=$(find_split_cutover)

if [ -n "$cutover_hits" ]; then
    echo "$cutover_hits"
    echo 'error: write "cutover" as one word in the docs, rewording a verb around the noun (see AGENTS.md, Terminology)' >&2
    exit 1
fi

echo 'terminology check passed: no hyphenated "schema-change" in prose, no two-word "cut over" in the docs'
