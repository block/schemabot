#!/bin/bash
# run-spirit-fmt — Canonicalize CREATE TABLE .sql files via spirit fmt.
#
# spirit fmt round-trips CREATE TABLE statements through a real MySQL server,
# normalizing constructs like BOOLEAN → tinyint(1), DEFAULT TRUE → DEFAULT 1,
# and charset/collation to their canonical forms. This prevents declarative
# schema diffing tools from detecting spurious changes.
#
# Modes:
#   Format (default):  Rewrites .sql files in-place to their canonical form.
#                      Use during development: make fmt-schema
#   Check (--check):   Compares files against canonical form without modifying them.
#                      Exits 1 if any files need formatting. Use in CI: make fmt-schema-check
#
# Usage:
#   scripts/run-spirit-fmt.sh [--check] <path>...
#
# Arguments:
#   --check      Check-only mode — never modifies files, exits 1 if not canonical
#   <path>...    SQL files or directories to format (default: pkg/schema/mysql)
#
# MySQL connection (in order of preference):
#   1. MYSQL_SERVER env var (if set)
#   2. Local MySQL on 127.0.0.1:3306 (if reachable)
#   3. Ephemeral Docker container (started and stopped automatically)
#
# Exportable: other repos can download and use this script directly.
#   curl -sL https://raw.githubusercontent.com/block/schemabot/main/scripts/run-spirit-fmt.sh | bash -s -- path/to/sql/
#
# Spirit binary (in order of preference):
#   1. spirit in PATH
#   2. go install github.com/block/spirit/cmd/spirit@v0.12.0
set -e

CHECK_MODE=false
DOCKER_CONTAINER=""
PATHS=()

# Parse args.
while [ $# -gt 0 ]; do
    case "$1" in
        --check) CHECK_MODE=true; shift ;;
        *) PATHS+=("$1"); shift ;;
    esac
done

# Default path if none provided.
if [ ${#PATHS[@]} -eq 0 ]; then
    PATHS=("pkg/schema/mysql")
fi

# Minimum spirit version required for the fmt command.
SPIRIT_MIN_VERSION="0.12.0"

# Check if a spirit binary supports the fmt command (introduced in v0.12.0).
spirit_has_fmt() {
    "$1" fmt --help &>/dev/null 2>&1
}

# Ensure spirit binary is available with fmt support.
ensure_spirit() {
    # Check PATH (includes hermit-managed binaries if activated).
    if command -v spirit &>/dev/null && spirit_has_fmt "$(command -v spirit)"; then
        return
    fi
    # Check repo-local hermit bin directory.
    if [ -x "./bin/spirit" ] && spirit_has_fmt "./bin/spirit"; then
        export PATH="./bin:$PATH"
        return
    fi
    # Check go install location.
    local gobin="${GOBIN:-$(go env GOPATH)/bin}"
    if [ -x "$gobin/spirit" ] && spirit_has_fmt "$gobin/spirit"; then
        export PATH="$gobin:$PATH"
        return
    fi
    # Auto-install via go install.
    echo "spirit >= $SPIRIT_MIN_VERSION not found, installing via go install..."
    go install "github.com/block/spirit/cmd/spirit@v${SPIRIT_MIN_VERSION}" 2>&1
    if [ -x "$gobin/spirit" ] && spirit_has_fmt "$gobin/spirit"; then
        export PATH="$gobin:$PATH"
        return
    fi
    echo "ERROR: spirit >= $SPIRIT_MIN_VERSION not found."
    echo ""
    echo "Install via one of:"
    echo "  brew install block/tap/spirit      # macOS (once v$SPIRIT_MIN_VERSION is published)"
    echo "  hermit install spirit              # if using hermit"
    echo "  go install github.com/block/spirit/cmd/spirit@v$SPIRIT_MIN_VERSION"
    exit 1
}

# Try connecting to MySQL at the given host:port.
mysql_reachable() {
    local host="$1"
    local h="${host%%:*}"
    local p="${host##*:}"
    if command -v mysqladmin &>/dev/null; then
        mysqladmin -h "$h" -P "$p" -u root --connect-timeout=2 ping &>/dev/null 2>&1
    elif command -v nc &>/dev/null; then
        nc -z -w2 "$h" "$p" &>/dev/null 2>&1
    else
        return 1
    fi
}

cleanup() {
    if [ -n "$DOCKER_CONTAINER" ]; then
        echo "Stopping ephemeral MySQL container..."
        docker rm -f "$DOCKER_CONTAINER" &>/dev/null 2>&1 || true
    fi
    if [ -n "$TEMP_DIR" ]; then
        rm -rf "$TEMP_DIR"
    fi
}
trap cleanup EXIT

# Determine MySQL host.
if [ -n "$MYSQL_SERVER" ]; then
    MYSQL_HOST="$MYSQL_SERVER"
    echo "Using MYSQL_SERVER=$MYSQL_HOST"
elif mysql_reachable "127.0.0.1:3306"; then
    MYSQL_HOST="127.0.0.1:3306"
    echo "Using local MySQL on 127.0.0.1:3306"
else
    echo "No local MySQL found, starting ephemeral Docker container..."
    DOCKER_CONTAINER="spirit-fmt-$$"
    docker run -d --name "$DOCKER_CONTAINER" \
        -p 127.0.0.1:13399:3306 \
        -e MYSQL_ALLOW_EMPTY_PASSWORD=yes \
        -e MYSQL_INITDB_SKIP_TZINFO=1 \
        mysql:8.4 \
        >/dev/null

    echo -n "Waiting for MySQL"
    for i in $(seq 1 30); do
        if docker exec "$DOCKER_CONTAINER" mysqladmin ping -h localhost --silent &>/dev/null 2>&1; then
            echo " ready"
            break
        fi
        echo -n "."
        sleep 1
    done

    if ! docker exec "$DOCKER_CONTAINER" mysqladmin ping -h localhost --silent &>/dev/null 2>&1; then
        echo " timed out"
        exit 1
    fi

    MYSQL_HOST="127.0.0.1:13399"
fi

ensure_spirit

export MYSQL_SERVER="$MYSQL_HOST"
export MYSQL_USER="${MYSQL_USER:-root}"
export MYSQL_PASSWORD="${MYSQL_PASSWORD:-}"

if [ "$CHECK_MODE" = true ]; then
    # Check mode: copy files to a temp dir, format there, compare.
    # Never modifies the original files.
    TEMP_DIR=$(mktemp -d)
    NEEDS_FORMAT=false

    for path in "${PATHS[@]}"; do
        if [ -d "$path" ]; then
            for f in "$path"/*.sql; do
                [ -f "$f" ] || continue
                cp "$f" "$TEMP_DIR/"
                if ! spirit fmt "$TEMP_DIR/$(basename "$f")" 2>&1; then
                    echo "  ERROR formatting: $f"
                    exit 1
                fi
                if ! diff -q "$f" "$TEMP_DIR/$(basename "$f")" >/dev/null 2>&1; then
                    echo "  needs formatting: $f"
                    NEEDS_FORMAT=true
                fi
            done
        elif [ -f "$path" ]; then
            cp "$path" "$TEMP_DIR/"
            if ! spirit fmt "$TEMP_DIR/$(basename "$path")" 2>&1; then
                echo "  ERROR formatting: $path"
                exit 1
            fi
            if ! diff -q "$path" "$TEMP_DIR/$(basename "$path")" >/dev/null 2>&1; then
                echo "  needs formatting: $path"
                NEEDS_FORMAT=true
            fi
        fi
    done

    if [ "$NEEDS_FORMAT" = true ]; then
        echo ""
        echo "Run 'make fmt-schema' (or 'scripts/run-spirit-fmt.sh') to fix."
        exit 1
    else
        echo "Schema files are canonically formatted."
    fi
else
    # Format mode: run spirit fmt in-place.
    OUTPUT=$(spirit fmt "${PATHS[@]}" 2>&1)
    if [ -n "$OUTPUT" ]; then
        echo "Formatted:"
        echo "$OUTPUT"
    else
        echo "Schema files already canonical — no changes."
    fi
fi
