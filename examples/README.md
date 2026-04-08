# Examples

## MySQL Test Schema

The `mysql/schema/testapp/` directory contains example SQL files and a `schemabot.yaml` config for testing SchemaBot locally.

### Quick Start

```bash
# Install the CLI
make install

# Start local environment (applies schema and seeds 10k rows)
make demo
```

### Using the CLI

```bash
# Plan - see what changes would be made
schemabot plan -s examples/mysql/schema/testapp --endpoint http://localhost:13370

# Apply - run the schema change
schemabot apply -s examples/mysql/schema/testapp -y --endpoint http://localhost:13370

# Progress - check schema change status
schemabot progress -d testapp --endpoint http://localhost:13370
```

Or run from the schema directory (uses current directory by default):

```bash
cd examples/mysql/schema/testapp
schemabot plan --endpoint http://localhost:13370
schemabot apply -y --endpoint http://localhost:13370
```

### Connect to MySQL

```bash
make mysql DB=testapp
```

| Service | Endpoint |
|---------|----------|
| SchemaBot API | http://localhost:13370 |
| TestApp MySQL | localhost:13372 |
| SchemaBot MySQL | localhost:13371 |

Credentials: `root` / `testpassword`

### Modifying the Schema

Edit the schema files in `mysql/schema/testapp/` and run plan to see the diff:

```bash
# Edit mysql/schema/testapp/users.sql to add a column, then:
schemabot plan -s examples/mysql/schema/testapp --endpoint http://localhost:13370
```

Output:
```
SchemaBot will perform the following actions:

  ~ users
      ALTER TABLE `users` ADD COLUMN `name` varchar(255)

Plan: 0 to create, 1 to change, 0 to drop.
```

### Configuration

The `mysql/schema/testapp/schemabot.yaml` file:

```yaml
database: testapp
type: mysql
```

Schema files (`.sql`) live alongside it in the same directory.

## Multi-App Monorepo

The `multi-app/` directory contains a multi-service monorepo layout following a monorepo convention:

```
multi-app/
  payments-service/mysql/schema/
    schemabot.yaml              (database: payments — at root because multiple namespaces)
    payments/transactions.sql
    payments_audit/audit_log.sql
  orders-service/mysql/schema/
    orders/schemabot.yaml       (database: orders — inside namespace dir)
    orders/orders.sql
  inventory-service/mysql/schema/
    inventory/schemabot.yaml    (database: inventory — inside namespace dir)
    inventory/products.sql
```

Each service has its own `schemabot.yaml` config. For single-namespace databases, the config lives inside the namespace directory alongside the SQL files. For multiple namespaces (like `payments` with `payments` + `payments_audit`), the config lives at the schema root above the namespace directories.

Use `deploy/aws/scripts/setup-test-repo.sh` to push this layout to a test repo.
