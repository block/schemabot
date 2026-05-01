# LocalScale

LocalScale is a fake PlanetScale HTTP API server that translates PlanetScale API calls into Vitess operations against a managed [vtcombo](https://vitess.io/docs/get-started/vttestserver/) cluster. It enables integration testing and local development of the PlanetScale engine without a real PlanetScale account.

Each database in the config gets its own `vttest.LocalCluster` (the same code path as `vttestserver`) started in-process. Clusters start in parallel. Ports are randomly assigned by vttest. Metadata tables are stored on the managed cluster's embedded mysqld.

## Architecture

```
+-----------------------------------------------------------+
|  SchemaBot / Tern                                         |
|                                                           |
|  PlanetScale Engine                                       |
|    |                                                      |
|    v                                                      |
|  PSClient (PlanetScale Go SDK + custom endpoints)         |
+---|-------------------------------------------------------+
    | HTTP
    v
+---|-------------------------------------------------------+
|  LocalScale HTTP Server (pkg/localscale)                  |
|                                                           |
|  Endpoints:                                               |
|    /branches             create / get (metadata only)     |
|    /branches/.../schema  apply DDL or SHOW CREATE TABLE   |
|    /branches/.../vschema VSchema via vtctld gRPC          |
|    /admin/...            test helpers (seed, exec, reset) |
|                                                           |
|  Backends:                                                |
|  +-------------+  +----------------+  +-----------------+ |
|  | vtctldclient|  | vtgate MySQL   |  | Metadata MySQL  | |
|  | gRPC        |  | (per keyspace) |  | (localscale DB) | |
|  +------+------+  +-------+--------+  +--------+--------+ |
|         |                 |                     |          |
+---------|-----------------|-----------+---------+----------+
          |                 |           |
          v                 v           v
+-----------------------------------------------------------+
|  vttest.LocalCluster (vtcombo, in-process)                |
|                                                           |
|  vtctld gRPC (random port)   vtgate MySQL (random port)   |
|  mysqld (Unix socket + TCP)                               |
+-----------------------------------------------------------+
```

## Usage

### Docker Image

Self-contained all-in-one container (~5s startup):

```bash
docker build -f deploy/local/Dockerfile.localscale -t localscale:latest .
docker run -p 8080:8080 -v ./config.json:/etc/localscale/config.json localscale:latest
```

Config:
```json
{
  "organizations": {
    "my-org": {
      "databases": {
        "my-db": {
          "keyspaces": [
            {"name": "app", "shards": 1},
            {"name": "app_sharded", "shards": 2}
          ]
        }
      }
    }
  },
  "listen_addr": ":8080"
}
```

### Testcontainers (Go)

#### Schema directory layout

Create a `testdata/schema/` directory with one subdirectory per keyspace. Each keyspace
can contain `.sql` files (DDL) and an optional `vschema.json`:

```
testdata/schema/
  app/
    users.sql           # CREATE TABLE users (...)
    orders.sql          # CREATE TABLE orders (...)
  app_sharded/
    products.sql        # CREATE TABLE products (...)
    vschema.json        # {"sharded": true, "vindexes": {...}, "tables": {...}}
```

#### TestMain setup

```go
var (
    testContainer *localscale.LocalScaleContainer
    testClient    psclient.PSClient
)

func TestMain(m *testing.M) {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

    var err error
    testContainer, err = localscale.RunContainer(ctx, localscale.ContainerConfig{
        Orgs: map[string]localscale.ContainerOrgConfig{
            "my-org": {Databases: map[string]localscale.ContainerDatabaseConfig{
                "my-db": {Keyspaces: []localscale.ContainerKeyspaceConfig{
                    {Name: "app", Shards: 1},
                    {Name: "app_sharded", Shards: 2},
                }},
            }},
        },
    })
    if err != nil {
        fmt.Fprintf(os.Stderr, "Failed to start container: %v\n", err)
        cancel()
        os.Exit(1)
    }

    // Create PSClient pointing at LocalScale (same client used against real PlanetScale)
    testClient, err = psclient.NewPSClientWithBaseURL("test", "test", testContainer.URL())
    if err != nil {
        fmt.Fprintf(os.Stderr, "Failed to create client: %v\n", err)
        cancel()
        os.Exit(1)
    }

    // Seed schema from directory (applies VSchema first, then DDL per keyspace)
    if err := testContainer.SchemaDir(ctx, "my-org", "my-db", "testdata/schema"); err != nil {
        fmt.Fprintf(os.Stderr, "Failed to seed schema: %v\n", err)
        cancel()
        os.Exit(1)
    }

    code := m.Run()
    cancel()
    os.Exit(code)
}
```

#### Writing tests

Tests use the standard PlanetScale Go SDK — the same code that runs against real PlanetScale:

```go
func TestBranchAndDDL(t *testing.T) {
    ctx := t.Context()

    // Create a branch (snapshots current schema)
    _, err := testClient.CreateBranch(ctx, &ps.CreateDatabaseBranchRequest{
        Organization: "my-org",
        Database:     "my-db",
        Name:         "feature-branch",
        ParentBranch: "main",
    })
    require.NoError(t, err)

    // Get branch credentials (starts a MySQL proxy for this branch)
    pw, err := testClient.CreateBranchPassword(ctx, &ps.DatabaseBranchPasswordRequest{
        Organization: "my-org",
        Database:     "my-db",
        Branch:       "feature-branch",
    })
    require.NoError(t, err)

    // Connect via MySQL and apply DDL (same as real PlanetScale)
    dsn := fmt.Sprintf("%s:%s@tcp(%s)/app_sharded", pw.Username, pw.PlainText, pw.Hostname)
    db, err := sql.Open("mysql", dsn)
    require.NoError(t, err)
    defer db.Close()

    _, err = db.ExecContext(ctx, "ALTER TABLE products ADD COLUMN weight_kg DECIMAL(10,2)")
    require.NoError(t, err)

    // Update VSchema on the branch
    _, err = testClient.UpdateKeyspaceVSchema(ctx, &ps.UpdateKeyspaceVSchemaRequest{
        Organization: "my-org",
        Database:     "my-db",
        Branch:       "feature-branch",
        Keyspace:     "app_sharded",
        VSchema:      `{"sharded": true, ...}`,
    })
    require.NoError(t, err)
}
```

#### Container helpers

| Method | Purpose |
|--------|---------|
| `container.SchemaDir(ctx, org, db, dir)` | Seed schema from directory (VSchema first, then DDL) |
| `container.SeedDDL(ctx, org, db, ks, stmts...)` | Execute DDL directly on vtgate |
| `container.SeedVSchema(ctx, org, db, ks, json)` | Apply VSchema via vtctld |
| `container.VtgateExec(ctx, org, db, ks, sql)` | Execute SQL against vtgate |
| `container.MetadataQuery(ctx, sql)` | Execute SQL against metadata DB |
| `container.BranchDBQuery(ctx, branch, ks, sql)` | Execute SQL against a branch database |
| `container.ResetState(ctx)` | Truncate metadata, cancel migrations, drop branch databases |
| `container.URL()` | Base URL for PSClient (`http://localhost:PORT`) |

#### Container reuse for fast tests

By default each test run starts a fresh container (~5s startup + schema seeding).
Set `Reuse: true` on the config to keep the container running between `go test`
invocations — subsequent runs connect instantly and state is cleaned up automatically:

```go
container, err := localscale.RunContainer(ctx, localscale.ContainerConfig{
    Orgs: map[string]localscale.ContainerOrgConfig{
        "my-org": {Databases: map[string]localscale.ContainerDatabaseConfig{
            "my-db": {Keyspaces: []localscale.ContainerKeyspaceConfig{
                {Name: "app", Shards: 1},
                {Name: "app_sharded", Shards: 2},
            }},
        }},
    },
    Reuse: os.Getenv("DEBUG") == "1", // Reuse locally, fresh in CI
})
```

Run with reuse: `DEBUG=1 go test -tags=integration ./pkg/localscale/...`

**How it works:**
- First run: starts the container, seeds schema (~5s)
- Subsequent runs: connects to existing container, auto-resets state (~1s)
- Container persists until manually stopped (`docker rm -f localscale-test`)

When `Reuse` is true, `RunContainer` automatically:
1. Uses a fixed container name (`localscale-test`, or `ContainerName` if set)
2. Calls `ResetState()` to clean up stale data (cancel migrations, truncate metadata, drop branch databases)

Don't call `container.Terminate()` when reusing — the container stays for the next run.

**CI vs local:** Use an env var (e.g. `DEBUG=1`) to control reuse. CI should use
fresh containers for isolation. Local dev should reuse for speed.

#### Complete integration test example

Here's a full example of testing a PlanetScale engine feature against LocalScale:

```go
//go:build integration

package mypackage_test

import (
    "context"
    "database/sql"
    "fmt"
    "os"
    "testing"
    "time"

    _ "github.com/go-sql-driver/mysql"
    ps "github.com/planetscale/planetscale-go/planetscale"
    "github.com/stretchr/testify/require"

    "github.com/block/schemabot/pkg/localscale"
    "github.com/block/schemabot/pkg/psclient"
)

var (
    testContainer *localscale.LocalScaleContainer
    testClient    psclient.PSClient
)

func TestMain(m *testing.M) {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

    var err error
    testContainer, err = localscale.RunContainer(ctx, localscale.ContainerConfig{
        Orgs: map[string]localscale.ContainerOrgConfig{
            "my-org": {Databases: map[string]localscale.ContainerDatabaseConfig{
                "my-db": {Keyspaces: []localscale.ContainerKeyspaceConfig{
                    {Name: "app", Shards: 1},
                    {Name: "app_sharded", Shards: 2},
                }},
            }},
        },
        Reuse: os.Getenv("DEBUG") == "1",
    })
    if err != nil {
        fmt.Fprintf(os.Stderr, "container: %v\n", err)
        cancel()
        os.Exit(1)
    }

    testClient, _ = psclient.NewPSClientWithBaseURL("test", "test", testContainer.URL())

    // Seed schema once — all tests share the same container
    testContainer.SchemaDir(ctx, "my-org", "my-db", "testdata/schema")

    code := m.Run()
    cancel()
    os.Exit(code)
}

func TestSchemaChangeOnBranch(t *testing.T) {
    ctx := t.Context()

    // 1. Create a branch
    _, err := testClient.CreateBranch(ctx, &ps.CreateDatabaseBranchRequest{
        Organization: "my-org", Database: "my-db",
        Name: fmt.Sprintf("test-%d", time.Now().UnixNano()),
        ParentBranch: "main",
    })
    require.NoError(t, err)

    // 2. Get MySQL credentials for the branch
    pw, err := testClient.CreateBranchPassword(ctx, &ps.DatabaseBranchPasswordRequest{
        Organization: "my-org", Database: "my-db", Branch: branchName,
    })
    require.NoError(t, err)

    // 3. Apply DDL via MySQL (same as real PlanetScale)
    db, _ := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/app_sharded",
        pw.Username, pw.PlainText, pw.Hostname))
    defer db.Close()
    _, err = db.ExecContext(ctx, "ALTER TABLE products ADD COLUMN weight DECIMAL(10,2)")
    require.NoError(t, err)

    // 4. Verify the schema change via the API
    schema, err := testClient.GetBranchSchema(ctx, &ps.BranchSchemaRequest{
        Organization: "my-org", Database: "my-db",
        Branch: branchName, Keyspace: "app_sharded",
    })
    require.NoError(t, err)
    // schema contains the diff between branch and main
}
```

Run: `go test -tags=integration ./mypackage/...`
Fast mode: `DEBUG=1 go test -tags=integration ./mypackage/...`

## API Endpoints

### Health
- `GET /health` — 200 OK

### Keyspaces & Schema
- `GET /v1/.../branches/{branch}/keyspaces` — Lists keyspaces from vtctld
- `GET /v1/.../branches/{branch}/schema?keyspace={ks}` — SHOW CREATE TABLE for each table

### Branches
- `GET /v1/.../branches/{branch}` — Get branch metadata
- `POST /v1/.../branches` — Create branch (snapshots schema into branch database)
- `POST /v1/.../branches/{branch}/passwords` — Get branch credentials (starts TCP proxy)
- `POST /v1/.../branches/{branch}/schema` — Apply DDL/VSchema to branch
- `GET/PATCH /v1/.../branches/{branch}/keyspaces/{keyspace}/vschema` — Get/update VSchema

### Catch-all
- `* /v1/...` — Returns 501 Not Implemented for unimplemented endpoints

### Admin Endpoints (Test Helpers)

| Endpoint | Purpose |
|----------|---------|
| `POST /admin/seed-ddl` | Execute DDL on vtgate (optional `strategy` and `migration_context` for online DDL) |
| `POST /admin/seed-vschema` | Apply VSchema via vtctld gRPC |
| `POST /admin/vtgate-exec` | Execute SQL against vtgate |
| `POST /admin/metadata-query` | Execute SQL against metadata DB |
| `POST /admin/branch-db-query` | Execute SQL against a branch database |
| `POST /admin/reset-state` | Truncate metadata, close proxies |

Deploy request endpoints (create, deploy, cancel, cutover, revert, throttle) and the background state machine processor are coming in a follow-up PR.

## Online DDL Strategy

LocalScale submits online DDL with the `vitess` strategy and `--singleton-context` flag. This means Vitess rejects new DDL submissions if there is a pending migration from a **different** migration context (each deploy request gets a unique context like `localscale:7`).

This is important for test cleanup: if a test's online DDL migration is still running when the next test starts, the new deploy request will fail with `singleton-context migration rejected`. To avoid this, call `POST /admin/reset-state` between tests — it cancels all pending migrations and waits for them to reach terminal state before returning.

See `buildDDLStrategy()` in `helpers.go` for the full strategy string, and the [Vitess online DDL documentation](https://vitess.io/docs/user-guides/schema-changes/managed-online-schema-changes/) for details on singleton strategies.

## Differences from Real PlanetScale

1. **Single vtgate**: All branches share the same vtgate. Branch isolation via per-branch databases + TCP proxy
2. **Configurable cluster size**: Defaults to 2 shards; configurable per keyspace
