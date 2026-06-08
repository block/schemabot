# Strata

Strata is a database type for schema changes that span many MySQL shards behind
a shared topology. It is declared with `type: strata` and executed remotely by
Tern over gRPC: SchemaBot sends the plan and apply requests and tracks the
result, while the per-shard execution and cross-shard coordination happen in the
deployment. (An in-process mode could be added later by porting that execution
into SchemaBot, but it is not needed today.)

A schema change to a strata database is one SchemaBot apply that the deployment
fans out across the shards of each keyspace, running them in parallel. Once
every shard has finished, the keyspace's VSchema is re-applied. Coordinating
that final VSchema update across all shards is why strata runs remotely rather
than through the in-process client.

## Responsibilities

What lives in the OSS repo (SchemaBot):

- The `strata` database type and its validation (server config, repo
  `schemabot.yaml`, and the plan handler).
- The proto surface for the per-shard view: `ENGINE_STRATA`, `ShardPlan` +
  `PlanResponse.shards`, `ApplyRequest.target_shards`, and the existing
  `ShardProgress` on `TableProgress`.
- Sending the full declarative schema (`schema_files`, including `vschema.json`)
  on the apply gRPC call, so the deployment can plan per shard.

What lives in the deployment (Tern):

- Resolving the shard topology, enumerating shards, diffing each, and hydrating
  the VSchema.
- Applying the change to every shard in parallel and coordinating the
  cross-shard VSchema update.
- Aggregating per-shard progress and applying control operations across all
  shards.

A strata database is registered with an ordinary cluster identifier — the same
as any other remotely-executed database — and the deployment resolves the shard
topology from it. No separate topology endpoint is needed.

## Plan

`Plan` runs a declarative diff across every shard of each keyspace:

1. Build the desired per-keyspace state from the request's schema files
   (`.sql` files are table definitions; `vschema.json` is the desired VSchema).
2. Diff every shard against the desired state (enumerate shards, diff each,
   hydrate the VSchema, and lint).
3. Aggregate into one `PlanResponse`: the canonical change set comes from a
   representative shard (the schema is identical across a keyspace's shards),
   and `PlanResponse.shards` reports each shard's membership and drift
   (`needs_change`).

## Apply

`Apply` plans per shard (using the `schema_files` carried on the request) and
applies the change:

- Each shard that needs changes is applied in parallel.
- Once every shard has finished, the keyspace's VSchema is re-applied. A
  keyspace whose only change is the VSchema (no shard DDL) has it applied
  directly.
- `defer_cutover` is honored per shard, so all shards hold at cutover until
  signaled.

The apply returns an identifier that lets progress and control operations
address all the per-shard work for the change. A database may declare more than
one keyspace; the identifier covers every keyspace in the apply. Most strata
databases map to one keyspace.

### Shard selection (phased applies)

The `ApplyRequest.target_shards` proto field is reserved for phased/canary
rollouts — applying to a subset of shards first, then the rest. It is not yet
sent by SchemaBot or honored by the apply; wiring it and sequencing waves is
future work.

## Progress and control

`Progress` gathers progress from every shard and merges the results: one entry
per table with a per-shard breakdown (`ShardProgress`), and an overall state
that is `completed` only when every shard is complete and `failed` if any shard
fails.

`Stop`, `Start`, and `Cutover` apply across all shards of the change. `Volume`,
`Revert`, and `SkipRevert` are not supported for strata, matching MySQL/Spirit.

## Onboarding

A strata database is declared like any other, with `type: strata`:

```yaml
databases:
  resolute:
    type: strata
    allowed_repos: [org/repo]
    allowed_dirs: [path/to/schema]
    environments:
      production:
        deployment: <deployment>
        target: <identifier>
```

Flipping an existing `type: mysql` database to `type: strata` requires no other
SchemaBot config change: the database name, schema files, allowed dirs/repos,
deployment, and target are unchanged. The deployment resolves the shard set for
that target from the topology.

## Non-goals (for now)

- Phased/canary wave sequencing. The `ApplyRequest.target_shards` proto field is
  reserved as the hook, but it is not yet sent or honored.
- `Volume`, `Revert`, and `SkipRevert` for strata (unsupported, as for
  MySQL/Spirit).
- In-process (local-mode) execution. Strata runs remotely today; local mode
  could be added later.
