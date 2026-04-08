#!/bin/bash
# Seed testapp with sample data (10k rows each for users and orders)
#
# Usage:
#   ./scripts/seed.sh                    # Seed both staging and production
#   ./scripts/seed.sh staging            # Seed staging only
#   ./scripts/seed.sh production         # Seed production only

set -e

ENV=${1:-both}
COMPOSE_FILE="deploy/local/docker-compose.yml"

seed_service() {
    local svc=$1
    docker compose -f "$COMPOSE_FILE" exec -T -e MYSQL_PWD=testpassword "$svc" mysql -uroot testapp -e "
        INSERT INTO users (email)
        SELECT CONCAT('user', seq, '@example.com')
        FROM (SELECT @row := @row + 1 as seq FROM
            (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
            (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
            (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c,
            (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) d,
            (SELECT @row := 0) r
        ) nums LIMIT 10000;
        INSERT INTO orders (user_id, total_cents, status)
        SELECT
            FLOOR(1 + RAND() * 10000),
            FLOOR(100 + RAND() * 100000),
            ELT(FLOOR(1 + RAND() * 3), 'pending', 'completed', 'shipped')
        FROM (SELECT @row2 := @row2 + 1 as seq FROM
            (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
            (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
            (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c,
            (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) d,
            (SELECT @row2 := 0) r
        ) nums LIMIT 10000;"
    echo "Seeded $svc (10k users, 10k orders)"
}

echo "Seeding testapp databases..."

case "$ENV" in
    staging)
        seed_service mysql-staging
        ;;
    production)
        seed_service mysql-production
        ;;
    both)
        seed_service mysql-staging
        seed_service mysql-production
        ;;
    *)
        echo "Usage: $0 [staging|production|both]"
        exit 1
        ;;
esac

echo "Done."
