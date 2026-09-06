# Local MySQL runtime

Run SchemaBot's API and MySQL execution engine in the same native process. Plans,
consent checks, progress, and recovery use the same implementation as a shared
service. Docker is only needed if you choose it to provide MySQL.

```text
CLI commands
     |
     | authenticated loopback HTTP
     v
schemabot local serve
     |                 |
     v                 v
SchemaBot storage    Target MySQL
plans and applies    application data and copy checkpoints
```

The foreground process owns execution until you shut it down.

## Configure storage and targets

Provide an existing, dedicated MySQL database for SchemaBot's state and a MySQL
DSN for each application database. SchemaBot creates its bookkeeping tables in
that state database on startup. It does not create or reset the database itself.
Use a different database name from every target, even when they live on separate
servers; host aliases cannot prove that two connections reach different databases.

Save this as `runtime.yaml`, with permissions `0600`. Set `LOCAL_STATE_DSN` and
`LOCAL_APP_DSN` to the respective Go MySQL driver DSNs before starting the process.
Explicit `env:` and `file:` secret references are supported; unrelated storage
variables and service configuration are not selected implicitly.

```yaml
storage:
  dsn: env:LOCAL_STATE_DSN

databases:
  app:
    type: mysql
    environments:
      development:
        dsn: env:LOCAL_APP_DSN
```

Create `runtime.token`, also with permissions `0600`, containing a randomly
generated secret: at least 32 cryptographically random bytes encoded as hex.
Keep both files outside version control. The token authorizes access to the local
runtime, including writes; it is never printed in the ready record or logs.

## Start the runtime

```sh
schemabot local serve --config runtime.yaml --token-file runtime.token
```

```json
{"state":"ready","endpoint":"http://127.0.0.1:49152"}
```

The port is allocated by the operating system and will vary. `--listen` can select
a fixed numeric loopback address and port. Every route, including health probes,
requires the token. Diagnostics go to stderr; stdout contains the ready record.
The local host does not open gRPC or metrics listeners or configure GitHub Apps.

Use the reported endpoint with the existing CLI's `--endpoint` option and supply
the token through `SCHEMABOT_TOKEN`. The [schema intelligence guide](schema-intelligence.md)
shows commands and their output.
Local hosting does not require logging into a remote profile.

## Shut down and recover

Ctrl+C or SIGTERM drains requests, checkpoints running copies where possible,
stops the engines, and releases the operator's claims. Active applies remain
active for recovery; shutting down the process does not issue a schema change
stop or cancel command. Start it again with the same configuration and storage
to let the operator recover the work.

After a hard crash, recovery waits for the existing leases to expire and uses the
available engine checkpoints. Preserve both the state database and the target's
checkpoint data. A storage outage affects readiness and supervision; it does not
trigger a local process restart.

This command runs in the foreground. Keep its terminal open while it works.
Background startup, an onboarding wizard, Docker provisioning, and automatic
upgrades are not part of this command. Keep configuration and the binary version
stable while an apply is active.

The local credential identifies `local-runtime`, not a person. An agent running
as the same OS user can read the same credential. Plan validation and explicit
unsafe-change consent still apply; the token is not independent human approval.
