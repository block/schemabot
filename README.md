# <a href="./assets/schemabot-avatar.svg"><img src="./assets/schemabot-avatar.svg" alt="SchemaBot" style="height: 1em; max-width: 100%;"></a> SchemaBot

![MySQL: GA](https://img.shields.io/badge/MySQL-GA-brightgreen)
![Vitess: GA](https://img.shields.io/badge/Vitess-GA-brightgreen)
![PostgreSQL: early alpha](https://img.shields.io/badge/PostgreSQL-early_alpha-orange)

GitOps for database schemas. Define your desired schema in SQL files, open a PR, and SchemaBot plans and executes your schema changes safely.

These badges are about capability. **GA** means the engine can change a large table online and gives an operator a real lever over a change that is already running; **early alpha** means it cannot yet. [docs/engines.md](./docs/engines.md) defines both and is the capability matrix: how each engine executes a change, which control operations it supports, and how it manages load. A narrower engine is not a looser one, which is [docs/invariants.md](./docs/invariants.md)'s subject: the runtime safety guarantees that hold whichever engine runs the change.

## Schema Changes via Pull Request

Open a PR with schema changes and SchemaBot handles the rest — plan, apply, and verify across environments:

![SchemaBot PR Demo](./assets/pr-demo.gif)

## Interactive CLI

SchemaBot provides a fully interactive CLI for planning, applying, and monitoring schema changes:

![SchemaBot CLI Demo](./assets/cli-demo.gif)

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
- **Apply** — execute DDL online using [Spirit](https://github.com/block/spirit) (MySQL), [PlanetScale deploy requests](https://planetscale.com/docs/vitess/schema-changes/deploy-requests) (Vitess), or [pg-sprite](https://github.com/block/pg-sprite) (PostgreSQL)
- **Progress** — track row copy progress, ETA, per-table/per-shard status
- **Control** — `stop` (pause), `start` (resume), `cutover` (trigger table swap), `revert` (roll back)

Simple changes (e.g., adding a column) use instant DDL and complete in milliseconds. Operations that require a row copy (e.g., adding an index) run online without blocking reads or writes.

Not every engine supports every feature, and some share a verb without sharing its meaning: pausing and resuming a running change, `revert`, automatic throttling, and drop recovery all vary by engine. [docs/engines.md](./docs/engines.md) is the capability matrix showing which engine does what and why.

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

General design docs live in the [docs](./docs/) folder. Three good places to start — each
has a table of contents, so jump straight to the question you came with:

- [docs/invariants.md](./docs/invariants.md) is the registry of runtime safety invariants:
  what must never be false while SchemaBot is running, why each rule matters, where it is
  enforced, and what these guarantees deliberately do not cover. It opens with what happens
  when GitHub is down.
- [docs/engines.md](./docs/engines.md) is the engine capability matrix: how each engine
  executes a change, which control operations it supports, how it manages load, and why the
  differences exist.
- [docs/postgresql.md](./docs/postgresql.md) is the PostgreSQL support envelope: what plans,
  what applies, and how each refusal is reported.

## Releases

Releases are published as binaries on the [GitHub Releases page](https://github.com/block/schemabot/releases), as container images at `ghcr.io/block/schemabot`, and, if you run Kubernetes, as a Helm chart at `oci://ghcr.io/block/charts/schemabot`.

We run what we ship. Every tag is deployed to production at Block, where SchemaBot runs the majority of Block's schema change traffic across a large fleet of MySQL and Vitess databases (with PostgreSQL planned). We actively use SchemaBot to run schema changes against tables of many shapes and sizes, with some spanning many terabytes in MySQL and 100s of shards in Vitess. Our release cadence keeps the project continuously validated against real production workloads and gets fixes out fast. Because SchemaBot is pre-1.0, the release notes are the compatibility contract: give them a read before upgrading.

See [docs/release.md](./docs/release.md) for how releases are cut and what is checked before a tag is published.

## Contributing

Contributors are welcome — see [CONTRIBUTING.md](./CONTRIBUTING.md).

For feature requests and bugs, [open an issue](https://github.com/block/schemabot/issues).
