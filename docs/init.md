# Initialize a database

Run `schemabot init` in a terminal to connect a database and start with a verified schema.
The wizard and explicit CLI flags use the same setup workflow.

![SchemaBot setup, a schema edit, and the first plan](../assets/init-demo.gif)

## Before you start

Use MySQL or PostgreSQL, with an existing application database and a separate database for
SchemaBot's state. They can share a server. Set `DATABASE_URL` and `SCHEMABOT_STORAGE_DSN` to
their connection strings; the wizard saves references to those variables, never their values
in your schema files. Keep those variables available for later CLI invocations.

Setup initializes SchemaBot's metadata tables in the state database. Baseline planning also
needs the engine's scratch privileges. Setup never applies application schema changes.

## Follow the wizard

The wizard asks for the engine, database name, environment, connection references, namespace
scope, schema directory, and profile. Review the summary and confirm to continue.

SchemaBot imports into a temporary directory and verifies a no-change plan before publishing
the files. A successful setup ends with:

```text
Schema ready in schema.
Baseline plan: no changes.
Profile "default" is ready. Edit the schema, then run a plan.
```

An empty namespace gets a comment-only `schema.sql` file that preserves its scope. Add your
first table declaration there when you are ready; setup does not invent a sample table.

If schema files already exist, the wizard offers to verify and reuse them. It checks their
database, engine, and namespace scope, and preserves their contents. Differences from the live
database stop setup with the plan details; they are not applied automatically.

## Use an agent or script

Supply the same decisions as flags. `--non-interactive` never prompts, and `--json` returns a
structured result. For example, against a supported database with one table:

```console
$ schemabot init --non-interactive --json --type mysql \
    --database shop --environment development --namespace shop \
    --dsn env:DATABASE_URL --storage-dsn env:SCHEMABOT_STORAGE_DSN \
    --schema-dir schema
{"database":"shop","environment":"development","profile":"default","schema_dir":"/project/schema","plan_id":"plan-example","tables":1,"verified":true}
```

Paths and plan IDs vary. Add `--reuse-schema` to verify existing desired files. Select a named
connection with `--profile`; existing profiles and the default connection are preserved.

Missing inputs are explicit and exit unsuccessfully:

```console
$ schemabot init --non-interactive --json
{"error":"missing_inputs","missing":["database","environment","type","dsn","storage-dsn","namespace"]}
```

A failed setup preserves existing files and retains any runtime registration and state already
created. Correct the reported issue and retry with the same inputs. An identical import can be
reused; conflicting files or connections are never overwritten.
