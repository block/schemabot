#!/bin/bash
set -euo pipefail

# Generate all template previews and create/update a GitHub gist.
# Also regenerates TEMPLATES.md via update-templates.sh.
# Usage: ./scripts/preview-gist.sh
#
# Requires: gh CLI authenticated, bin/schemabot built

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"
BINARY="$REPO_ROOT/bin/schemabot"
GIST_FILE="$REPO_ROOT/.gist-id"

if [ ! -x "$BINARY" ]; then
    echo "Building schemabot..."
    make -C "$REPO_ROOT" build
fi

# Regenerate TEMPLATES.md
"$SCRIPT_DIR/update-templates.sh"

# Use TEMPLATES.md as the gist content (copy to temp file for gh gist filename)
TMPDIR=$(mktemp -d)
cp "$REPO_ROOT/TEMPLATES.md" "$TMPDIR/schemabot-templates.md"
TMPFILE="$TMPDIR/schemabot-templates.md"

# Create or update gist
if [ -f "$GIST_FILE" ]; then
    GIST_ID=$(cat "$GIST_FILE")
    echo "Updating gist $GIST_ID..."
    gh gist edit "$GIST_ID" "$TMPFILE" && echo "Updated: https://gist.github.com/$GIST_ID"
else
    echo "Creating new gist..."
    GIST_URL=$(gh gist create -d "SchemaBot Templates" "$TMPFILE")
    GIST_ID=$(basename "$GIST_URL")
    echo "$GIST_ID" > "$GIST_FILE"
    echo "Created: $GIST_URL"
fi

rm -rf "$TMPDIR"
