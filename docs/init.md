# Initialize a database

Run `schemabot init` in a terminal to connect a database and start with a verified schema.
The wizard and explicit CLI flags use the same setup workflow.

![SchemaBot setup, a schema edit, and the first plan](../assets/init-demo.gif)

The demo shows PostgreSQL: choose an engine, name the database, select namespaces, review
the setup, and verify the baseline before making a first edit.

## Before you start

Use MySQL or PostgreSQL, with an existing application database and a separate database for
SchemaBot's state. Vitess databases are not offered by the wizard yet; register them in the
[server configuration](configuration.md) instead. They can share a server. Set `DATABASE_URL` and `SCHEMABOT_STORAGE_DSN` to
their connection strings; the wizard saves references to those variables, never their values
in your schema files. Keep those variables available for later CLI invocations.

Setup initializes SchemaBot's metadata tables in the state database. Baseline planning also
needs the engine's scratch privileges. Setup never applies application schema changes.

Private databases do not need a public endpoint. Run SchemaBot somewhere that can reach
them: on your VPN, through an existing tunnel, or on a machine inside the private network.
If a connection check fails, keep the wizard open, restore access, and retry.

## Follow the wizard

The wizard confirms both connections before discovering namespaces, even when their variables
are already set. Each connection step shows the host and database without credentials; edit
the variable reference to use a different connection. Press Enter to test access, then continue
after “Connected” appears. The check runs a read-only query and creates no metadata. It keeps `development`, `schema`, and your default profile as editable defaults,
and includes everything in the final review.

Before setting up a runtime, SchemaBot reads the target catalog to discover namespaces. One
result is selected automatically; multiple results appear in a searchable list. Use Space to
select namespaces and Enter to continue. MySQL stays within the database named in the DSN;
PostgreSQL lists accessible application schemas. No results or a failed connection stops here
with a chance to retry, edit the connection, or press `m` to enter namespaces manually.
Explicit `--namespace` flags also bypass discovery. Discovery uses only the application connection. State metadata is initialized only after the final review.

Use Shift+Tab from the review to edit any decision, or Escape to cancel. Explicit
`--namespace` flags keep their supplied scope and bypass discovery.

SchemaBot imports into a temporary directory and verifies a no-change plan before publishing
the files. A successful setup ends with:

```text
  ✓ Your schema is ready

  1 table · schema
  Baseline plan: no changes.

  Make your first edit, then review the plan:

    schemabot plan -s 'schema' -e 'development'
```

The command names the profile with `--profile` only when the connection was saved under a
profile that is not your default, so the next step works as printed.

An empty namespace gets an explicit marker in `schema.sql` that preserves its scope. Keep that
marker until you add your
first table declaration there when you are ready; setup does not invent a sample table.

If schema files already exist, the review screen explains that they will be verified and reused. It checks their
database, engine, and namespace scope, and preserves their contents. Differences from the live
database stop setup with the plan details; they are not applied automatically.

## Use an agent or script

Supply the same decisions as flags. Both `--non-interactive` and `--json` suppress prompts;
`--json` also returns a structured result. For example, against a supported database with one table:

```console
$ schemabot init --non-interactive --json --type mysql \
    --database shop --environment development --namespace shop \
    --dsn env:DATABASE_URL --storage-dsn env:SCHEMABOT_STORAGE_DSN \
    --schema-dir schema
{"database":"shop","environment":"development","profile":"default","schema_dir":"/project/schema","plan_id":"plan-example","tables":1,"verified":true}
```

Paths and plan IDs vary. Existing schema directories with a valid `schemabot.yaml` are verified and
reused automatically, including with flags. `--reuse-schema` is also accepted. Select a named
connection with `--profile`; existing profiles and the default connection are preserved.

Missing inputs are explicit and exit unsuccessfully:

```console
$ schemabot init --non-interactive --json
{"error":{"code":"missing_inputs","message":"Provide the missing flags or run init interactively."},"missing":["database","environment","type","dsn","storage-dsn","namespace"]}
```

A failed setup preserves existing files and retains any runtime registration and state already
created. Correct the reported issue and retry with the same inputs. An identical import can be
reused; conflicting files or connections are never overwritten.
