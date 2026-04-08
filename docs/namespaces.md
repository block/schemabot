# Schema Namespaces

SchemaBot uses **namespaces** to organize declarative schema files. A namespace maps to the database-specific grouping concept:

| Database | Namespace maps to | How to list |
|----------|------------------|-------------|
| MySQL | Schema name | `SHOW DATABASES` |
| Vitess | Keyspace | `SHOW KEYSPACES` |
| PostgreSQL | Schema | `\dn` |

## Schema Directory Structure

The schema directory is the source of truth. Each subdirectory is a namespace containing SQL files and optional configuration.

### MySQL — Single schema name

The simplest case. One database, one schema name:

```
myapp/schema/
├── schemabot.yaml
└── testapp/
    ├── users.sql
    ├── orders.sql
    └── products.sql
```

```yaml
# schemabot.yaml
database: testapp
type: mysql
```

### MySQL — Multiple schema names on the same database

A single MySQL database server can have multiple schema names (`SHOW DATABASES`). Each subdirectory is a namespace, all managed under one `schemabot.yaml`:

```
myapp/schema/
├── schemabot.yaml
├── app_primary/
│   ├── users.sql
│   └── sessions.sql
└── app_analytics/
    ├── events.sql
    └── metrics.sql
```

```yaml
# schemabot.yaml
database: myapp
type: mysql
```

SchemaBot plans each namespace independently — `app_primary` and `app_analytics` each produce their own `SchemaChange` with separate table changes.

### MySQL — Different databases entirely

When an app talks to separate database servers, each gets its own schema directory with its own `schemabot.yaml`:

```
myapp/
├── primary-schema/
│   ├── schemabot.yaml          # database: app_primary, type: mysql
│   └── app_primary/
│       ├── users.sql
│       └── sessions.sql
└── analytics-schema/
    ├── schemabot.yaml          # database: app_analytics, type: mysql
    └── app_analytics/
        ├── events.sql
        └── metrics.sql
```

These are completely independent SchemaBot configurations — different targets, different credentials, different plans.

### Vitess — Multiple keyspaces

Vitess databases have multiple keyspaces under a single `schemabot.yaml`. Each keyspace subdirectory is a namespace:

```
myapp/schema/
├── schemabot.yaml
├── commerce/
│   ├── orders_seq.sql
│   ├── products_seq.sql
│   └── vschema.json
└── commerce_sharded/
    ├── orders.sql
    ├── products.sql
    └── vschema.json
```

```yaml
# schemabot.yaml
database: commerce
type: vitess
```

SchemaBot plans all keyspaces together — a single plan can contain changes across `commerce` and `commerce_sharded`. This is necessary because DDL and VSchema changes across keyspaces may need to be deployed atomically (e.g., moving a table between keyspaces).

### Vitess — VSchema changes

`vschema.json` controls Vitess routing (which tables live in which keyspace, sharding strategy, vindexes, etc.). When a `vschema.json` file changes:

1. **Plan** detects the diff and includes it as a `FileChange` for display
2. **Apply** sends the full `vschema.json` content to the branch (not the diff)
3. The PlanetScale engine applies both DDL and VSchema atomically

A plan can have DDL-only changes, VSchema-only changes, or both.

## Summary

| Scenario | schemabot.yaml | Namespaces | Example |
|---|---|---|---|
| MySQL, single schema | 1 | 1 | `testapp/` |
| MySQL, multiple schema names | 1 | many | `app_primary/`, `app_analytics/` |
| MySQL, different databases | 1 per database | 1 each | separate directories |
| Vitess, multiple keyspaces | 1 | many | `commerce/`, `commerce_sharded/` |

## How Namespaces Flow Through the System

The namespace key is always the directory name. Each stage maps namespaces to progressively richer types:

**1. Schema directory** — directories on disk

```
schema/
├── schemabot.yaml
├── commerce/            ← namespace "commerce"
│   ├── orders_seq.sql
│   └── vschema.json
└── commerce_sharded/    ← namespace "commerce_sharded"
    ├── orders.sql
    └── vschema.json
```

**2. SchemaFiles** — parsed into `map[string]*Namespace`

```
"commerce"         → {Files: {"orders_seq.sql": "CREATE TABLE ...", "vschema.json": "{...}"}}
"commerce_sharded" → {Files: {"orders.sql": "CREATE TABLE ...", "vschema.json": "{...}"}}
```

**3. PlanResult.Changes** — engine outputs `[]SchemaChange`

```
{Namespace: "commerce",         Tables: [{Table: "orders_seq", Operation: "alter", DDL: "..."}], Files: []}
{Namespace: "commerce_sharded", Tables: [{Table: "orders", Operation: "create", DDL: "..."}], Files: [{Name: "vschema.json", Diff: "..."}]}
```
