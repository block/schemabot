#!/bin/bash
# Seed testapp with data for gRPC mode testing.
# Connects directly to exposed Tern MySQL ports.
#
# Usage:
#   ./scripts/seed-grpc.sh                       # 50 MB per table, both envs
#   ./scripts/seed-grpc.sh 50 staging            # 50 MB per table, staging only
#   ./scripts/seed-grpc.sh 200 production        # 200 MB per table, production only

set -e

TARGET_MB=${1:-50}
ENV=${2:-both}

STAGING_PORT=${TERN_STAGING_MYSQL_PORT:-13382}
PRODUCTION_PORT=${TERN_PRODUCTION_MYSQL_PORT:-13383}
export MYSQL_PWD=testpassword
MYSQL_OPTS="-u root --batch --skip-column-names"

# Determine which ports to seed
case "$ENV" in
    staging)    ports="$STAGING_PORT";                      labels="staging" ;;
    production) ports="$PRODUCTION_PORT";                   labels="production" ;;
    both)       ports="$STAGING_PORT $PRODUCTION_PORT";     labels="staging production" ;;
    *)
        echo "Usage: $0 [target_mb] [staging|production|both]"
        exit 1
        ;;
esac

port_arr=($ports)
label_arr=($labels)

label_for_port() {
    for i in "${!port_arr[@]}"; do
        [ "${port_arr[$i]}" = "$1" ] && echo "${label_arr[$i]}" && return
    done
    echo "$1"
}

# Estimate rows needed per table based on observed bytes/row.
rows_for_table() {
    local table=$1
    local mb=$2
    local bytes=$((mb * 1024 * 1024))
    case "$table" in
        users)    echo $((bytes / 163)) ;;
        orders)   echo $((bytes / 148)) ;;
        products) echo $((bytes / 314)) ;;
    esac
}

seed_port() {
    local port=$1
    local label
    label=$(label_for_port "$port")

    local users_rows orders_rows products_rows
    users_rows=$(rows_for_table users "$TARGET_MB")
    orders_rows=$(rows_for_table orders "$TARGET_MB")
    products_rows=$(rows_for_table products "$TARGET_MB")

    echo "$label: seeding $users_rows users, $orders_rows orders, $products_rows products..."

    mysql -h 127.0.0.1 -P "$port" $MYSQL_OPTS testapp <<-EOSQL
		SET unique_checks=0, foreign_key_checks=0, sql_log_bin=0;
		SET SESSION bulk_insert_buffer_size=67108864;

		INSERT INTO users (email)
		SELECT CONCAT(SUBSTRING(MD5(seq + RAND()), 1, 20), '@example.com')
		FROM (SELECT @r1 := @r1 + 1 as seq FROM
		    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
		    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
		    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c,
		    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) d,
		    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) e,
		    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) f,
		    (SELECT @r1 := 0) r
		) nums LIMIT $users_rows;

		INSERT INTO orders (user_id, total_cents, status)
		SELECT
		    FLOOR(1 + RAND() * $users_rows),
		    FLOOR(100 + RAND() * 100000),
		    ELT(FLOOR(1 + RAND() * 4), 'pending', 'processing', 'shipped', 'delivered')
		FROM (SELECT @r2 := @r2 + 1 as seq FROM
		    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
		    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
		    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c,
		    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) d,
		    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) e,
		    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) f,
		    (SELECT @r2 := 0) r
		) nums LIMIT $orders_rows;

		INSERT INTO products (name, description, price_cents, sku)
		SELECT
		    CONCAT('Product ', SUBSTRING(MD5(seq), 1, 10)),
		    CONCAT('Description for product ', seq, '. This is a sample product with details.'),
		    FLOOR(100 + RAND() * 100000),
		    CONCAT('SKU-', SUBSTRING(MD5(seq + RAND()), 1, 12))
		FROM (SELECT @r3 := @r3 + 1 as seq FROM
		    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
		    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
		    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c,
		    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) d,
		    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) e,
		    (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) f,
		    (SELECT @r3 := 0) r
		) nums LIMIT $products_rows;

		ANALYZE TABLE users, orders, products;
	EOSQL

    # Print final sizes
    echo ""
    echo "=== $label ==="
    mysql -h 127.0.0.1 -P "$port" $MYSQL_OPTS --column-names testapp -e "
        SELECT table_name, table_rows,
            ROUND(data_length / 1024 / 1024, 1) AS data_mb,
            ROUND(index_length / 1024 / 1024, 1) AS index_mb,
            ROUND((data_length + index_length) / 1024 / 1024, 1) AS total_mb
        FROM information_schema.tables
        WHERE table_schema = 'testapp' AND table_name IN ('users','orders','products')
        ORDER BY table_name;"
}

echo "Seeding ${TARGET_MB} MB per table (users, orders, products)"
echo "Database: testapp (gRPC mode)"
echo "Environments: $labels"
echo ""

# Seed all environments concurrently
pids=()
for port in $ports; do
    seed_port "$port" &
    pids+=($!)
done

failed=0
for pid in "${pids[@]}"; do
    wait "$pid" || failed=1
done

[ $failed -ne 0 ] && echo "Some seeds failed!" && exit 1

echo ""
echo "Done!"
