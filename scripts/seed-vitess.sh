#!/bin/bash
# Seed Vitess tables (testapp_sharded) with data for testing shard progress.
# Connects to vtgate MySQL and inserts into the sharded keyspace so that
# online DDL operations have realistic row counts and per-shard progress.
#
# Usage:
#   ./scripts/seed-vitess.sh                       # 150 MB per table, staging only
#   ./scripts/seed-vitess.sh 50                    # 50 MB per table, staging
#   ./scripts/seed-vitess.sh 200 production        # 200 MB per table, production

set -e

TARGET_MB=${1:-150}
ENV=${2:-staging}

STAGING_VTGATE_PORT=${STAGING_VTGATE_PORT:-13376}
PRODUCTION_VTGATE_PORT=${PRODUCTION_VTGATE_PORT:-13377}
VTGATE_USER=${VTGATE_MYSQL_USER:-vt_dba_tcp}

case "$ENV" in
  staging)     VTGATE_PORT=$STAGING_VTGATE_PORT ;;
  production)  VTGATE_PORT=$PRODUCTION_VTGATE_PORT ;;
  *)           echo "Unknown env: $ENV (use staging or production)"; exit 1 ;;
esac

MYSQL="mysql -h 127.0.0.1 -P $VTGATE_PORT -u $VTGATE_USER --batch --skip-column-names"
BATCH_SIZE=2000

# Verify connectivity before starting
if ! $MYSQL -e "SELECT 1" testapp_sharded >/dev/null 2>&1; then
    echo "ERROR: Cannot connect to vtgate at 127.0.0.1:$VTGATE_PORT (user: $VTGATE_USER)"
    echo "Skipping Vitess seeding."
    exit 0
fi

# Bytes per row estimates (data + indexes, conservative).
#   users:    ~163 bytes/row
#   orders:   ~148 bytes/row
#   products: ~314 bytes/row
users_rows=$((TARGET_MB * 1024 * 1024 / 163))
orders_rows=$((TARGET_MB * 1024 * 1024 / 148))
products_rows=$((TARGET_MB * 1024 * 1024 / 314))

echo "Seeding Vitess testapp_sharded (${TARGET_MB} MB per table)"
echo "  users: $users_rows rows, orders: $orders_rows rows, products: $products_rows rows"
echo ""

# Initialize Vitess sequences (idempotent).
# Sequence tables need exactly one row with id=0 for vtgate's sequence machinery.
# Use REPLACE to overwrite any stale rows, and bump cache for faster batch inserts.
echo "Initializing sequences..."
$MYSQL testapp <<EOSQL
INSERT IGNORE INTO users_seq (id, next_id, cache) VALUES (0, 1, 1000);
INSERT IGNORE INTO orders_seq (id, next_id, cache) VALUES (0, 1, 1000);
INSERT IGNORE INTO products_seq (id, next_id, cache) VALUES (0, 1, 1000);
EOSQL

seed_users() {
    echo "Seeding users ($users_rows rows)..."
    awk -v total="$users_rows" -v batch="$BATCH_SIZE" 'BEGIN {
        srand()
        for (i = 1; i <= total; i++) {
            if ((i-1) % batch == 0) {
                if (i > 1) print ";"
                printf "INSERT INTO users (email, full_name) VALUES "
            } else {
                printf ","
            }
            printf "(\047user%d_%05d@example.com\047,\047User %d\047)", i, int(rand()*99999), i
            if (i % 100000 == 0) {
                printf "  users: %d / %d rows\n", i, total > "/dev/stderr"
            }
        }
        print ";"
    }' | $MYSQL testapp_sharded
    echo "  users: done"
}

seed_orders() {
    echo "Seeding orders ($orders_rows rows)..."
    awk -v total="$orders_rows" -v batch="$BATCH_SIZE" -v max_uid="$users_rows" 'BEGIN {
        srand()
        split("pending,processing,shipped,delivered", s, ",")
        for (i = 1; i <= total; i++) {
            if ((i-1) % batch == 0) {
                if (i > 1) print ";"
                printf "INSERT INTO orders (user_id, total_cents, status) VALUES "
            } else {
                printf ","
            }
            uid = int(1 + rand() * max_uid)
            cents = int(100 + rand() * 100000)
            si = int(1 + rand() * 4)
            printf "(%d,%d,\047%s\047)", uid, cents, s[si]
            if (i % 100000 == 0) {
                printf "  orders: %d / %d rows\n", i, total > "/dev/stderr"
            }
        }
        print ";"
    }' | $MYSQL testapp_sharded
    echo "  orders: done"
}

seed_products() {
    echo "Seeding products ($products_rows rows)..."
    awk -v total="$products_rows" -v batch="$BATCH_SIZE" 'BEGIN {
        srand()
        for (i = 1; i <= total; i++) {
            if ((i-1) % batch == 0) {
                if (i > 1) print ";"
                printf "INSERT INTO products (name, description, price_cents, sku) VALUES "
            } else {
                printf ","
            }
            cents = int(100 + rand() * 100000)
            printf "(\047Product %d\047,\047Description for product %d with details\047,%d,\047SKU-%d-%05d\047)", i, i, cents, i, int(rand()*99999)
            if (i % 100000 == 0) {
                printf "  products: %d / %d rows\n", i, total > "/dev/stderr"
            }
        }
        print ";"
    }' | $MYSQL testapp_sharded
    echo "  products: done"
}

# Seed tables concurrently for speed
seed_users &
pid_users=$!
seed_orders &
pid_orders=$!
seed_products &
pid_products=$!

failed=0
wait $pid_users || failed=1
wait $pid_orders || failed=1
wait $pid_products || failed=1

[ $failed -ne 0 ] && echo "Some seeds failed!" && exit 1

echo ""
echo "Done! Seeded ${TARGET_MB} MB per table into testapp_sharded via vtgate"
