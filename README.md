# <a href="./assets/schemabot-avatar.svg"><img src="./assets/schemabot-avatar.svg" alt="SchemaBot" style="height: 1em; max-width: 100%;"></a> SchemaBot

GitOps for database schemas. Define your desired schema in SQL files, open a PR, and SchemaBot plans and executes your schema changes safely.

> [!WARNING]
> **Experimental** — SchemaBot is under active development. APIs and configuration may change without notice.

## CLI Demo

SchemaBot provides a fully interactive CLI for planning, applying, and monitoring schema changes:

![SchemaBot CLI Demo](./docs/assets/cli-demo.gif)

## PR Demo

_Coming soon_

## How It Works

SchemaBot uses **declarative schema** — you describe the desired end state in SQL files, and SchemaBot figures out the DDL needed to get there:

```sql
-- schema/testapp/users.sql
CREATE TABLE users (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,       -- add a column: just edit this file
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
```

SchemaBot diffs your schema files against the live database, computes the DDL, and applies it:

```
$ schemabot plan -s ./schema -e staging

  ALTER TABLE `users` ADD COLUMN `email` VARCHAR(255) NOT NULL

Plan: 1 table to alter

$ schemabot apply -s ./schema -e staging -y

Apply started: apply-a1b2c3d4
status=running  progress=45%  table=users  rows=1.2M/2.7M  eta=3m
status=completed
```

SchemaBot handles the full lifecycle:
- **Plan** — diff desired vs current schema → compute DDL
- **Apply** — execute DDL online using Spirit (MySQL) or PlanetScale deploy requests (Vitess)
- **Progress** — track row copy progress, ETA, per-table/per-shard status
- **Control** — `stop` (pause), `start` (resume), `volume` (adjust speed), `cutover` (trigger table swap), `revert` (roll back)

Simple changes (e.g., adding a column) use instant DDL and complete in milliseconds. Operations that require a row copy (e.g., adding an index) run online without blocking reads or writes.

## Quick Start

```bash
make demo    # Start services, apply schema, seed data
make test    # Run all tests (unit + integration + e2e)
```

Connect to databases:
```bash
make mysql              # SchemaBot storage DB (port 13371)
make mysql DB=staging   # Staging testapp (port 13372)
make mysql DB=production # Production testapp (port 13373)
```

## Architecture

See [docs/architecture.md](./docs/architecture.md) for the full documentation.

## Configuration

See [docs/configuration.md](./docs/configuration.md) for setup instructions (local mode, gRPC mode, secret resolution).

## Docs

General design docs are in the [docs](./docs/) folder.

## Contributing

Contributors are welcome — see [CONTRIBUTING.md](./CONTRIBUTING.md).

For feature requests and bugs, [open an issue](https://github.com/block/schemabot/issues).
