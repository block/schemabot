# Initialize a database

Run `schemabot init` in a terminal to bring an existing database into SchemaBot.
Setup reads its live schema into files and verifies they match, without changing application
tables or rewriting existing SQL scripts. An empty database follows the same flow.
The wizard and explicit CLI flags use the same setup workflow.

![SchemaBot setup, a schema edit, and the first plan](../assets/init-demo.gif)

## Before you start

Use MySQL, PostgreSQL, or Vitess with an existing application database. You can paste a connection string or enter host, port, database, username, and password
inside the wizard. Passwords and pasted strings are hidden. The final review asks you to
confirm saving entered credentials in private, **unencrypted** files under
`~/.schemabot/credentials`, outside the project. These files persist across terminal and
laptop restarts; keep them private and include them in your credential-management practices.
Cancelling before final confirmation writes no credentials.

If `DATABASE_URL` is already set, the wizard shows its destination and offers to use it.
Environment-variable and absolute file references remain available as an advanced option;
a referenced file must contain only the connection string. References stay references,
so environment variables must remain available to later CLI invocations. Schema files
never contain credentials. OS credential-store integration is not implemented.
Vitess uses PlanetScale deploy requests and needs a separate MySQL database for SchemaBot’s state.

The wizard asks **Where should SchemaBot store its own data?**

- **Integrated:** a simple setup for a single database project. SchemaBot creates a new
  `schemabot` database on your application's server, using the same credentials. Those
  credentials need permission to create a database. Your application's tables stay separate.
- **Standalone:** provide `SCHEMABOT_STORAGE_DSN` for an existing, dedicated state database.
  A separate server is a good fit for teams managing multiple databases.

Integrated setup refuses to adopt an existing `schemabot` database silently. If you already
prepared one for SchemaBot, choose Standalone and provide its connection explicitly.

You can move state to another server later, but this is an operator-managed transfer, not a
wizard toggle: drain in-flight work, stop the runtime, transfer the complete state database,
update its connection configuration, and verify it before restarting. Keep the original state
until the new connection is verified. Re-running `init` never replaces existing state storage.

For Vitess, the application connection is your vtgate address, and the wizard also asks for the
PlanetScale organization and a [service token](#configure-the-planetscale-service-token).
SchemaBot opens deploy requests with that token and reads keyspaces from the `main` branch.
Its own state lives in a MySQL database outside Vitess.

Setup initializes SchemaBot's metadata tables in the state database. Baseline planning also
needs the engine's scratch privileges. Setup never applies application schema changes.

Private databases do not need a public endpoint. Run SchemaBot somewhere that can reach
them: on your VPN, through an existing tunnel, or on a machine inside the private network.
If a connection check fails, keep the wizard open, restore access, and retry.

## Vitess and PlanetScale

Connect your vtgate endpoint, choose your PlanetScale organization, and verify a service-token
reference. SchemaBot discovers keyspaces from the `main` branch and keeps its own data in a
separate MySQL database.

![Connect a Vitess database, verify its schema, and preview a change](../assets/init-vitess-demo.gif)

### Configure the PlanetScale service token

The service token lets SchemaBot manage branches and deploy requests through the PlanetScale
API. It is separate from the username and password in your database connection string.

1. In your PlanetScale organization's **Settings → Service tokens**, create a token for
   SchemaBot. Copy its **ID** and **secret**; the display name is only a label.
2. Choose **Edit token permissions → Add database access** and select the database you are
   connecting. Grant the permissions below, then save. Scope access to this database.

| Permission | What SchemaBot uses it for |
| --- | --- |
| `read_branch` | Discover keyspaces and read branch schemas and VSchemas. |
| `create_branch` | Prepare a development branch for a schema change. |
| `connect_branch` | Create credentials to run the proposed DDL on that development branch. |
| `delete_branch` | Clean up development branches. |
| `read_deploy_request` | Read deploy request state and progress. |
| `create_deploy_request` | Open, queue, cut over, cancel, and revert deploy requests. |
| `write_branch_vschema` | Stage VSchema changes on the development branch. |

These permissions follow the [PlanetScale API reference](https://planetscale.com/docs/api/reference/service-tokens)
and the operations SchemaBot performs. Organization-wide permissions and permission to delete
production branches are not needed for this setup. If your database requires deploy-request
approval, an eligible reviewer still needs to approve in PlanetScale; the token cannot approve
its own requests. See [PlanetScale's approval rules](https://planetscale.com/docs/api/service-tokens#service-tokens-and-deploy-requests-approvals).

3. Store the ID and secret together, separated by a colon. For example, a private file outside
   your project at `/Users/alex/.config/schemabot/planetscale-token` contains just:

   ```text
   TOKEN_ID:TOKEN_SECRET
   ```

   Restrict the file to your user. On the **Connect the PlanetScale API** screen, enter:

   ```text
   file:/Users/alex/.config/schemabot/planetscale-token
   ```

   Or use `env:PLANETSCALE_TOKEN` if that variable already contains the same value. Set it
   before launching the wizard, and keep it available to later SchemaBot commands. The
   wizard's `name:value` wording refers to the token ID and secret, not its display name.

The wizard checks the token by listing keyspaces on `main`. This confirms read access, not
all of the write permissions above. If setup succeeds but an apply is denied, check the
permissions on this token for this database. Setup does not create a deploy request to test them.

## Follow the wizard

The wizard detects an available connection and offers **Use this connection**, or lets you
paste a connection string, enter details, or use a reference. It shows the host and database
without credentials and checks access before continuing. Failed checks can be retried;
Shift+Tab returns to connection choices without leaving the wizard.

Standalone setup uses the same adaptive connection flow. Integrated creates its database
only after final confirmation. Connection checks run a read-only query and create no metadata.
Environment, schema directory, and profile defaults remain editable in the final review.

Before setting up a runtime, SchemaBot reads the target catalog to discover namespaces. One
result is selected automatically; multiple results appear in a searchable list. Use Space to
select namespaces and Enter to continue. MySQL stays within the database named in the DSN;
PostgreSQL lists accessible application schemas; Vitess lists the keyspaces of the `main`
branch. No results or a failed connection stops here
with a chance to retry, edit the connection, or press `m` to enter namespaces manually.
Explicit `--namespace` flags also bypass discovery. Discovery uses only the application connection. State metadata is initialized only after the final review.

Press Shift+Tab to go back and change your choices, or Escape to cancel.

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

Use `--integrated` instead of `--storage-dsn` to create SchemaBot’s own database on the application server. The flags are mutually exclusive.

For Vitess, also pass `--organization` and `--api-token env:PLANETSCALE_TOKEN`.
Use `--api-url` only for a PlanetScale-compatible private endpoint.

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
