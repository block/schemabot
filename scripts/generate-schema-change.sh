#!/bin/bash
# Generate schema change scenarios for manual testing.
#
# Usage:
#   ./scripts/generate-schema-change.sh add-column           # MySQL: add column
#   ./scripts/generate-schema-change.sh add-column --vitess   # Vitess: add column
#   ./scripts/generate-schema-change.sh add-table             # MySQL: add table
#   ./scripts/generate-schema-change.sh add-table --vitess    # Vitess: add table + VSchema
#   ./scripts/generate-schema-change.sh drop-column           # MySQL: drop column (undo add-column)
#   ./scripts/generate-schema-change.sh drop-column --vitess  # Vitess: drop column
#   ./scripts/generate-schema-change.sh drop-table            # MySQL: drop table (undo add-table)
#   ./scripts/generate-schema-change.sh drop-table --vitess   # Vitess: drop table + VSchema
#   ./scripts/generate-schema-change.sh reset                 # Reset all to baseline
#   ./scripts/generate-schema-change.sh reset --vitess        # Reset Vitess to baseline
#
# After generating, run:
#   bin/schemabot plan -s <schema-dir> -e staging
#   bin/schemabot apply -s <schema-dir> -e staging

set -e

MYSQL_DIR="examples/mysql/schema/testapp"
VITESS_SHARDED_DIR="examples/vitess/schema/testapp_sharded"
VITESS_UNSHARDED_DIR="examples/vitess/schema/testapp"
VITESS_SCHEMA_DIR="examples/vitess/schema"

# Parse args
COMMAND="${1:-}"
VITESS=0
for arg in "$@"; do
    case "$arg" in
        --vitess) VITESS=1 ;;
    esac
done

if [ "$VITESS" = "1" ]; then
    SCHEMA_DIR="$VITESS_SCHEMA_DIR"
    TABLE_DIR="$VITESS_SHARDED_DIR"
    SEQ_DIR="$VITESS_UNSHARDED_DIR"
else
    SCHEMA_DIR="$MYSQL_DIR"
    TABLE_DIR="$MYSQL_DIR"
    SEQ_DIR=""
fi

add_column() {
    local file="$TABLE_DIR/users.sql"
    if grep -q "phone" "$file" 2>/dev/null; then
        echo "Column 'phone' already exists in $file"
        exit 1
    fi
    # Add phone column after email
    sed -i '' 's/`email` varchar(255) NOT NULL,/`email` varchar(255) NOT NULL,\
  `phone` varchar(20) DEFAULT NULL,/' "$file" 2>/dev/null || \
    sed -i 's/`email` varchar(255) NOT NULL,/`email` varchar(255) NOT NULL,\n  `phone` varchar(20) DEFAULT NULL,/' "$file"
    echo "Added column 'phone' to users in $file"
    echo "Plan: bin/schemabot plan -s $SCHEMA_DIR -e staging"
}

drop_column() {
    local file="$TABLE_DIR/users.sql"
    if ! grep -q "phone" "$file" 2>/dev/null; then
        echo "Column 'phone' does not exist in $file"
        exit 1
    fi
    sed -i '' '/`phone`/d' "$file" 2>/dev/null || sed -i '/`phone`/d' "$file"
    echo "Dropped column 'phone' from users in $file"
    echo "Plan: bin/schemabot plan -s $SCHEMA_DIR -e staging"
}

add_table() {
    # Create the table SQL
    local table_file="$TABLE_DIR/notifications.sql"
    if [ -f "$table_file" ]; then
        echo "Table 'notifications' already exists at $table_file"
        exit 1
    fi

    if [ "$VITESS" = "1" ]; then
        cat > "$table_file" << 'EOF'
CREATE TABLE `notifications` (
  `id` bigint unsigned NOT NULL,
  `user_id` bigint NOT NULL,
  `message` text NOT NULL,
  `read_at` datetime(6) DEFAULT NULL,
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`id`),
  KEY `idx_user_id` (`user_id`),
  KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
EOF

        # Add sequence table
        cat > "$SEQ_DIR/notifications_seq.sql" << 'EOF'
CREATE TABLE `notifications_seq` (
  `id` bigint unsigned NOT NULL DEFAULT '0',
  `next_id` bigint unsigned DEFAULT NULL,
  `cache` bigint unsigned DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB COMMENT='vitess_sequence';
EOF

        # Update sharded VSchema
        local vschema="$VITESS_SHARDED_DIR/vschema.json"
        python3 -c "
import json
with open('$vschema') as f:
    v = json.load(f)
v['tables']['notifications'] = {
    'column_vindexes': [{'column': 'user_id', 'name': 'hash'}],
    'auto_increment': {'column': 'id', 'sequence': 'notifications_seq'}
}
with open('$vschema', 'w') as f:
    json.dump(v, f, indent=2)
    f.write('\n')
"

        # Update unsharded VSchema
        local unsharded_vschema="$VITESS_UNSHARDED_DIR/vschema.json"
        python3 -c "
import json
with open('$unsharded_vschema') as f:
    v = json.load(f)
v['tables']['notifications_seq'] = {'type': 'sequence'}
with open('$unsharded_vschema', 'w') as f:
    json.dump(v, f, indent=2)
    f.write('\n')
"

        echo "Added table 'notifications' + sequence + VSchema entries"
    else
        cat > "$table_file" << 'EOF'
CREATE TABLE `notifications` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `user_id` bigint NOT NULL,
  `message` text NOT NULL,
  `read_at` datetime(6) DEFAULT NULL,
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`id`),
  KEY `idx_user_id` (`user_id`),
  KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
EOF
        echo "Added table 'notifications' to $table_file"
    fi
    echo "Plan: bin/schemabot plan -s $SCHEMA_DIR -e staging"
}

drop_table() {
    local table_file="$TABLE_DIR/notifications.sql"
    if [ ! -f "$table_file" ]; then
        echo "Table 'notifications' does not exist at $table_file"
        exit 1
    fi

    rm "$table_file"

    if [ "$VITESS" = "1" ]; then
        rm -f "$SEQ_DIR/notifications_seq.sql"

        # Remove from sharded VSchema
        local vschema="$VITESS_SHARDED_DIR/vschema.json"
        python3 -c "
import json
with open('$vschema') as f:
    v = json.load(f)
v['tables'].pop('notifications', None)
with open('$vschema', 'w') as f:
    json.dump(v, f, indent=2)
    f.write('\n')
"

        # Remove from unsharded VSchema
        local unsharded_vschema="$VITESS_UNSHARDED_DIR/vschema.json"
        python3 -c "
import json
with open('$unsharded_vschema') as f:
    v = json.load(f)
v['tables'].pop('notifications_seq', None)
with open('$unsharded_vschema', 'w') as f:
    json.dump(v, f, indent=2)
    f.write('\n')
"

        echo "Dropped table 'notifications' + sequence + VSchema entries"
    else
        echo "Dropped table 'notifications' from $table_file"
    fi
    echo "Plan: bin/schemabot plan -s $SCHEMA_DIR -e staging"
}

reset_all() {
    # Remove any generated files
    rm -f "$TABLE_DIR/notifications.sql"
    [ -n "$SEQ_DIR" ] && rm -f "$SEQ_DIR/notifications_seq.sql"

    # Restore users.sql to baseline (remove phone column if present)
    sed -i '' '/`phone`/d' "$TABLE_DIR/users.sql" 2>/dev/null || \
    sed -i '/`phone`/d' "$TABLE_DIR/users.sql" 2>/dev/null || true

    if [ "$VITESS" = "1" ]; then
        # Reset VSchema files
        python3 -c "
import json
for path in ['$VITESS_SHARDED_DIR/vschema.json', '$VITESS_UNSHARDED_DIR/vschema.json']:
    with open(path) as f:
        v = json.load(f)
    v['tables'].pop('notifications', None)
    v['tables'].pop('notifications_seq', None)
    v.get('vindexes', {}).pop('xxhash', None)
    for tbl in v.get('tables', {}).values():
        if 'column_vindexes' in tbl:
            tbl['column_vindexes'] = [cv for cv in tbl['column_vindexes'] if cv.get('name') != 'xxhash']
    with open(path, 'w') as f:
        json.dump(v, f, indent=2)
        f.write('\n')
"
    fi
    if [ "$VITESS" = "1" ]; then
        echo "Reset Vitess schema to baseline"
    else
        echo "Reset MySQL schema to baseline"
    fi
}

vschema_change() {
    if [ "$VITESS" = "0" ]; then
        echo "vschema-change requires --vitess"
        exit 1
    fi

    local vschema="$VITESS_SHARDED_DIR/vschema.json"
    if python3 -c "import json; v=json.load(open('$vschema')); exit(0 if 'xxhash' in v.get('vindexes',{}) else 1)" 2>/dev/null; then
        echo "VSchema change already applied"
        exit 1
    fi

    # Add a new vindex and apply it to the orders table
    python3 -c "
import json
with open('$vschema') as f:
    v = json.load(f)
v['vindexes']['xxhash'] = {'type': 'xxhash'}
v['tables']['orders']['column_vindexes'].append({
    'column': 'status',
    'name': 'xxhash'
})
with open('$vschema', 'w') as f:
    json.dump(v, f, indent=2)
    f.write('\n')
"
    echo "Added xxhash vindex + orders.status column vindex"
    echo "Plan: bin/schemabot plan -s $SCHEMA_DIR -e staging"
}

undo_vschema_change() {
    if [ "$VITESS" = "0" ]; then
        echo "undo-vschema-change requires --vitess"
        exit 1
    fi

    local vschema="$VITESS_SHARDED_DIR/vschema.json"
    python3 -c "
import json
with open('$vschema') as f:
    v = json.load(f)
v['vindexes'].pop('xxhash', None)
v['tables']['orders']['column_vindexes'] = [
    cv for cv in v['tables']['orders']['column_vindexes']
    if cv.get('name') != 'xxhash'
]
with open('$vschema', 'w') as f:
    json.dump(v, f, indent=2)
    f.write('\n')
"
    echo "Removed xxhash vindex + orders.status column vindex"
    echo "Plan: bin/schemabot plan -s $SCHEMA_DIR -e staging"
}

case "$COMMAND" in
    add-column)          add_column ;;
    drop-column)         drop_column ;;
    add-table)           add_table ;;
    drop-table)          drop_table ;;
    vschema-change)      vschema_change ;;
    undo-vschema-change) undo_vschema_change ;;
    reset)               reset_all ;;
    *)
        echo "Usage: $0 <command> [--vitess]"
        echo ""
        echo "Commands:"
        echo "  add-column           Add 'phone' column to users table"
        echo "  drop-column          Remove 'phone' column from users table"
        echo "  add-table            Add 'notifications' table (+ VSchema for Vitess)"
        echo "  drop-table           Remove 'notifications' table (+ VSchema for Vitess)"
        echo "  vschema-change       Add xxhash vindex to orders (Vitess only)"
        echo "  undo-vschema-change  Remove xxhash vindex from orders (Vitess only)"
        echo "  reset                Reset schema to baseline"
        echo ""
        echo "Flags:"
        echo "  --vitess      Target Vitess schema (default: MySQL)"
        exit 1
        ;;
esac
