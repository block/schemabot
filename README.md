<p align="center">
  <img src="./assets/schemabot-avatar.svg" alt="SchemaBot" width="140" />
</p>

<h1 align="center">SchemaBot</h1>

<p align="center"><b>Ship schema changes as fast as your code, with the safety net your database deserves</b></p>

<p align="center">
  <img alt="MySQL: GA" src="https://img.shields.io/badge/MySQL-GA-brightgreen" />
  <img alt="Vitess: GA" src="https://img.shields.io/badge/Vitess-GA-brightgreen" />
  <img alt="PostgreSQL: early alpha" src="https://img.shields.io/badge/PostgreSQL-early_alpha-orange" />
</p>

<p align="center">
  <a href="./docs/vision.md">Vision</a> ·
  <a href="#the-pr-workflow">PR workflow</a> ·
  <a href="#from-your-terminal">CLI</a> ·
  <a href="#quick-start">Quick start</a> ·
  <a href="#docs">Docs</a>
</p>

---

SchemaBot is declarative and GitOps-driven: SQL files in your repository provide a shared source of truth across environments. It compares those files with the live database, shows the exact change, and applies it through pull requests or a CLI.

## The PR workflow

[![SchemaBot PR workflow: plan, apply, progress, completion, checks, and merge](./assets/pr-workflow-demo.gif)](./docs/pre-merge-workflow.md)

[Walk through the illustrated PR workflow](./docs/pre-merge-workflow.md#the-pr-workflow-step-by-step)

## From your terminal

**Plan and apply a change.** Review the SQL, apply the change, and follow it to completion.

<img src="./assets/cli-plan-apply.gif" width="800" alt="Plan and apply a schema change through the CLI">

**Know your database fleet.** Explore live schemas, spot lint issues, and follow changes through their logs.

<img src="./assets/cli-ops.gif" width="800" alt="Inspect fleet status, follow one change, and read its logs">

[Get started with the CLI](./docs/cli.md) · [Explore your database fleet](./docs/schema-intelligence.md)

Block runs SchemaBot for the majority of its production schema changes, including MySQL tables spanning terabytes and Vitess databases with hundreds of shards.

## Your policies, enforced

Review the proposed SQL before it runs. Configure who can apply it and which environments it must pass through.

| Protection | How it works |
|---|---|
| **Require a review before apply** | Enable the [review gate](./docs/configuration.md#review-gate) to require approval from a configured reviewer before PR changes run. The author's own approval does not count |
| **Prove the change in earlier environments** | Set the [promotion order](./docs/pre-merge-workflow.md#promotion-order), such as staging → production. PR applies are blocked until the earlier environment's check passes |
| **Keep destructive changes explicit** | Lint findings flag unsafe changes. Applying them requires acknowledgment of the exact plan; a changed plan needs a new acknowledgment |
| **Merge what is already live** | Configure SchemaBot's checks as [required checks](./docs/check-runs.md#branch-protection-setup). They pass when the managed live schema matches the PR, so unfinished changes cannot merge |

These are PR workflow gates. Direct CLI and API calls use [server permissions](./docs/auth.md); they do not require PR review or enforce promotion order.

These boundaries matter just as much for a coding agent as for a person. Give agents [schema context and scoped access](./docs/ai-agents.md) while keeping policy on the server.

## Quick Start

Grab the CLI from [Releases](#releases). From your application’s project directory, run:

```bash
schemabot init
```

![Connect a database with the SchemaBot wizard and review your first schema change plan](./assets/init-demo.gif)

The wizard connects to your MySQL or PostgreSQL database, imports its schema into declarative `.sql` files, and verifies a no-change plan before it finishes. It reads your application's schema and never changes it. SchemaBot keeps its own plans and progress in a separate database, which can live on the same server. Make your first edit and run `schemabot plan` to preview changes. Run `schemabot apply` when you’re ready to apply them. [docs/init.md](./docs/init.md) walks through each step, including the flag form for agents and scripts.

Want to try SchemaBot with demo databases? See the [examples guide](./examples/README.md).

To run the PR workflow for your team, deploy the server from [Releases](#releases) (binary, container image, or Helm chart), then follow [docs/github-app-setup.md](./docs/github-app-setup.md) to wire up GitHub and [docs/configuration.md](./docs/configuration.md) for the server config. [`schemabot onboard`](./docs/github-app-setup.md#6-add-schemabotyaml-config-to-your-repository) pulls a live database's schema into a new declarative schema directory against that server, so you start from your real tables rather than writing them out by hand.

## Supported databases

| Database | Status | How changes run |
|---|---|---|
| MySQL | Generally available | [Spirit](https://github.com/block/spirit), with instant DDL where supported and online copying when needed |
| Vitess on PlanetScale | Generally available | [Deploy requests](https://planetscale.com/docs/vitess/schema-changes/deploy-requests), with per-shard progress |
| PostgreSQL | Early alpha | [pg-sprite](https://github.com/block/pg-sprite); see the [support envelope](./docs/postgresql.md) |

Follow progress, choose cutover timing, and pause, resume, cancel, or roll back where the engine supports it. Execution methods, throttling, and recovery differ by engine; the [capability matrix](./docs/engines.md) explains the choices.

## Docs

Guides and reference:

- [Vision](./docs/vision.md): See what we’re building toward
- [Quick start](#quick-start): Try it on your machine
- [Initialize a database](./docs/init.md): Connect a database you already have and start from its live schema
- [Local examples](./examples/README.md): Run the demo and [connect to its MySQL databases](./examples/README.md#connect-to-mysql)
- [Pre-merge workflow](./docs/pre-merge-workflow.md): Take a schema change from your first edit to a merged PR
- [CLI guide](./docs/cli.md): Set up the CLI, inspect your databases, and run changes
- [Schema intelligence](./docs/schema-intelligence.md): Get to know your fleet and what’s changing
- [Engines](./docs/engines.md): See how changes run on your database engine
- [PostgreSQL](./docs/postgresql.md): Find out what’s supported today
- [Configuration](./docs/configuration.md): Set things up for your environment
- [Storage schema](./docs/storage-schema.md): Keep SchemaBot’s own bookkeeping database converged across deploys
- [Authentication](./docs/auth.md): Choose who can read and change your databases
- [AI agents](./docs/ai-agents.md): Set clear boundaries for your assistants
- [Safety invariants](./docs/invariants.md): Understand the guardrails behind each change
- [Architecture](./docs/architecture.md): Follow a change from start to finish
- [Target credential self-heal](./docs/target-credential-self-heal.md): Understand target probes and credential rotation recovery
- [Partition-aware index builds](./docs/partitioned-parent-index-flow.md): See the decision for online indexes on partitioned PostgreSQL tables
- [Ideas behind SchemaBot](./docs/inspiration.md): The projects that helped shape it
- [Contributing](./CONTRIBUTING.md): Come build with us

## Releases

Releases are published as binaries on the [GitHub Releases page](https://github.com/block/schemabot/releases), as container images at `ghcr.io/block/schemabot`, and, if you run Kubernetes, as a Helm chart at `oci://ghcr.io/block/charts/schemabot`.

Every release tag is deployed to production at Block. SchemaBot is pre-1.0, so read the release notes before upgrading: they describe compatibility changes.

See [docs/release.md](./docs/release.md) for how releases are cut and what is checked before a tag is published.

## Contributing

Contributors are welcome. See [CONTRIBUTING.md](./CONTRIBUTING.md).

For feature requests and bugs, [open an issue](https://github.com/block/schemabot/issues).
