#!/bin/bash
set -euo pipefail

# Create a MySQL user with the privileges SchemaBot needs for Spirit online DDL.
# Idempotent — safe to re-run. Adds missing grants without affecting existing ones.
#
# Usage:
#   ./setup-db-user.sh -h <host> -u <admin_user> -d <database> [options]
#
# The admin password is read from the MYSQL_PWD environment variable or
# prompted interactively. It is never passed as a CLI argument.
#
# Examples:
#   # Interactive (prompts for password)
#   ./setup-db-user.sh -h localhost -u root -d myapp
#
#   # Non-interactive (e.g. in CI/bootstrap scripts)
#   MYSQL_PWD=secret ./setup-db-user.sh -h localhost -u root -d myapp
#
#   # Multiple databases
#   ./setup-db-user.sh -h localhost -u root -d orders -d users
#
#   # Write generated DSNs to a file instead of terminal
#   ./setup-db-user.sh -h localhost -u root -d myapp -o dsn.env

MYSQL_HOST=""
MYSQL_PORT="3306"
ADMIN_USER=""
DATABASES=()
SB_USER="schemabot"
SB_PASS=""
SB_PASS_PROVIDED=false
STORAGE_DB="schemabot"
OUTPUT_FILE=""

usage() {
    echo "Usage: $0 -h <host> -u <admin_user> -d <database> [options]"
    echo ""
    echo "Required:"
    echo "  -h, --host          MySQL host"
    echo "  -u, --user          Admin MySQL user (e.g. root)"
    echo "  -d, --database      Target database(s) (repeat for multiple)"
    echo ""
    echo "Optional:"
    echo "  -P, --port          MySQL port (default: 3306)"
    echo "  --sb-user           SchemaBot MySQL user to create (default: schemabot)"
    echo "  --sb-password-file  Read SchemaBot user password from file"
    echo "  --storage-db        Storage database name (default: schemabot)"
    echo "  -o, --output        Write DSNs to file instead of terminal"
    echo ""
    echo "The admin password is read from MYSQL_PWD env var or prompted interactively."
    exit 1
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--host)            MYSQL_HOST="$2"; shift 2 ;;
        -P|--port)            MYSQL_PORT="$2"; shift 2 ;;
        -u|--user)            ADMIN_USER="$2"; shift 2 ;;
        -d|--database)        DATABASES+=("$2"); shift 2 ;;
        --sb-user)            SB_USER="$2"; shift 2 ;;
        --sb-password-file)   SB_PASS=$(cat "$2"); SB_PASS_PROVIDED=true; shift 2 ;;
        --storage-db)         STORAGE_DB="$2"; shift 2 ;;
        -o|--output)          OUTPUT_FILE="$2"; shift 2 ;;
        --help)               usage ;;
        *)                    echo "Unknown option: $1"; usage ;;
    esac
done

if [ -z "$MYSQL_HOST" ] || [ -z "$ADMIN_USER" ] || [ ${#DATABASES[@]} -eq 0 ]; then
    usage
fi

# Read admin password from MYSQL_PWD or prompt
if [ -z "${MYSQL_PWD:-}" ]; then
    if [ -t 0 ]; then
        read -rsp "Enter MySQL password for $ADMIN_USER@$MYSQL_HOST: " MYSQL_PWD
        echo ""
        export MYSQL_PWD
    else
        echo "Error: MYSQL_PWD not set and stdin is not a terminal"
        exit 1
    fi
fi

# Auto-generate SchemaBot user password if not provided
if [ -z "$SB_PASS" ]; then
    SB_PASS=$(openssl rand -hex 16)
fi

# MySQL config for admin connection (avoids credentials on command line)
MYSQL_CNF=$(mktemp)
chmod 600 "$MYSQL_CNF"
cat > "$MYSQL_CNF" <<EOF
[client]
user=$ADMIN_USER
password=$MYSQL_PWD
EOF
trap 'rm -f "$MYSQL_CNF" "$SB_CNF"' EXIT
SB_CNF=""

MYSQL_CMD="mysql --defaults-extra-file=$MYSQL_CNF -h $MYSQL_HOST -P $MYSQL_PORT --batch"

if ! $MYSQL_CMD -e "SELECT 1" > /dev/null 2>&1; then
    echo "Error: cannot connect to MySQL at $MYSQL_HOST:$MYSQL_PORT as $ADMIN_USER"
    exit 1
fi

echo "Setting up SchemaBot MySQL user"
echo "================================"
echo "  Host:     $MYSQL_HOST:$MYSQL_PORT"
echo "  User:     $SB_USER"
echo "  Storage:  $STORAGE_DB"
echo "  Targets:  ${DATABASES[*]}"
echo ""

# Escape single quotes in passwords for safe SQL interpolation
SB_PASS_ESC="${SB_PASS//\'/\'\'}"

# All statements are idempotent:
#   CREATE USER IF NOT EXISTS — no-op if user exists
#   GRANT — additive, no-op if grant already exists
#   CREATE DATABASE IF NOT EXISTS — no-op if database exists
#
# SET ROLE ALL activates granted roles (e.g. rds_superuser_role on RDS)
# so the admin user can grant global privileges like REPLICATION CLIENT.
SQL="SET ROLE ALL;
CREATE USER IF NOT EXISTS '${SB_USER}'@'%' IDENTIFIED BY '${SB_PASS_ESC}';"

# Only reset the password if explicitly provided via --sb-password-file.
# Without this guard, re-runs would auto-generate a new random password
# and silently break services configured with the original credentials.
if [ "$SB_PASS_PROVIDED" = true ]; then
    SQL+="
ALTER USER '${SB_USER}'@'%' IDENTIFIED BY '${SB_PASS_ESC}';"
fi

SQL+="
GRANT SELECT ON \`performance_schema\`.* TO '${SB_USER}'@'%';
CREATE DATABASE IF NOT EXISTS \`${STORAGE_DB}\`;
GRANT ALL PRIVILEGES ON \`${STORAGE_DB}\`.* TO '${SB_USER}'@'%';
"

for DB in "${DATABASES[@]}"; do
    SQL+="GRANT ALTER, CREATE, DELETE, DROP, INDEX, INSERT, LOCK TABLES, SELECT, TRIGGER, UPDATE ON \`${DB}\`.* TO '${SB_USER}'@'%';
"
done

SQL+="FLUSH PRIVILEGES;"

$MYSQL_CMD -e "$SQL"

# Global privileges: REPLICATION CLIENT, REPLICATION SLAVE, RELOAD, PROCESS,
# CONNECTION_ADMIN are needed for Spirit online DDL.
# Try granting directly first (works on self-managed MySQL).
# On RDS, direct global grants may silently fail — fall back to granting
# rds_superuser_role which provides all of these.
if ! $MYSQL_CMD -e "SET ROLE ALL; GRANT REPLICATION CLIENT, REPLICATION SLAVE, RELOAD, PROCESS, CONNECTION_ADMIN ON *.* TO '${SB_USER}'@'%'" 2>/dev/null; then
    echo "  Direct global privilege grants not available, trying rds_superuser_role..."
    if ! $MYSQL_CMD -e "SET ROLE ALL; GRANT 'rds_superuser_role' TO '${SB_USER}'@'%'; SET DEFAULT ROLE ALL TO '${SB_USER}'@'%'" 2>/dev/null; then
        echo "ERROR: cannot grant global privileges or rds_superuser_role to '${SB_USER}'@'%'"
        echo "  The user needs REPLICATION CLIENT, REPLICATION SLAVE, RELOAD, PROCESS,"
        echo "  and CONNECTION_ADMIN for Spirit online DDL."
        exit 1
    fi
fi

# Verify grants by connecting AS the new user (so SET ROLE ALL activates its roles)
echo ""
echo "Verifying grants for '${SB_USER}'@'%'..."
SB_CNF=$(mktemp)
chmod 600 "$SB_CNF"
cat > "$SB_CNF" <<SBEOF
[client]
user=$SB_USER
password=$SB_PASS
SBEOF
SB_CMD="mysql --defaults-extra-file=$SB_CNF -h $MYSQL_HOST -P $MYSQL_PORT --batch"

if ! $SB_CMD -e "SELECT 1" > /dev/null 2>&1; then
    echo "ERROR: cannot connect to MySQL as '${SB_USER}'@'%' — verify the user was created and the password is correct"
    exit 1
fi

GRANTS=$($SB_CMD -e "SET ROLE ALL; SHOW GRANTS")

FAILED=0

# Use here-strings instead of echo|grep to avoid broken pipe errors
# with set -o pipefail (grep -q closes pipe before echo finishes).

# Global privileges — satisfied by direct grants or rds_superuser_role membership
HAS_RDS_ROLE=false
if grep -q "rds_superuser_role" <<< "$GRANTS"; then
    HAS_RDS_ROLE=true
    echo "  rds_superuser_role: granted (provides global privileges)"
fi

for PRIV in "REPLICATION CLIENT" "REPLICATION SLAVE" "RELOAD" "PROCESS"; do
    if grep -q "$PRIV" <<< "$GRANTS" || [ "$HAS_RDS_ROLE" = true ]; then
        echo "  $PRIV: ok"
    else
        echo "  $PRIV: MISSING"
        FAILED=1
    fi
done

# Performance schema
if grep -q "performance_schema" <<< "$GRANTS"; then
    echo "  SELECT on performance_schema: ok"
else
    echo "  SELECT on performance_schema: MISSING"
    FAILED=1
fi

# Storage database
if grep -q "\`${STORAGE_DB}\`" <<< "$GRANTS"; then
    echo "  ALL on ${STORAGE_DB}: ok"
else
    echo "  ALL on ${STORAGE_DB}: MISSING"
    FAILED=1
fi

# Per-database privileges
for DB in "${DATABASES[@]}"; do
    if grep -q "\`${DB}\`" <<< "$GRANTS"; then
        echo "  Schema change privileges on ${DB}: ok"
    else
        echo "  Schema change privileges on ${DB}: MISSING"
        FAILED=1
    fi
done

echo ""
if [ "$FAILED" -eq 1 ]; then
    echo "ERROR: some grants are missing. Fix manually or re-run with a user that has GRANT OPTION."
    exit 1
fi

echo "All grants verified."
echo ""
echo "Done."

# Write DSNs to file only when explicitly requested via -o.
# Never print credentials to stdout.
if [ -n "$OUTPUT_FILE" ]; then
    DSN_OUTPUT="SCHEMABOT_STORAGE_DSN=${SB_USER}:${SB_PASS}@tcp(${MYSQL_HOST}:${MYSQL_PORT})/${STORAGE_DB}?parseTime=true"
    for DB in "${DATABASES[@]}"; do
        DB_UPPER=$(echo "$DB" | tr '[:lower:]' '[:upper:]')
        DSN_OUTPUT+=$'\n'"SCHEMABOT_${DB_UPPER}_DSN=${SB_USER}:${SB_PASS}@tcp(${MYSQL_HOST}:${MYSQL_PORT})/${DB}?parseTime=true"
    done
    echo "$DSN_OUTPUT" > "$OUTPUT_FILE"
    chmod 600 "$OUTPUT_FILE"
    echo ""
    echo "DSNs written to $OUTPUT_FILE"
fi
