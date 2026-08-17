# Releasing SchemaBot

A release is a `vX.Y.Z` tag. This document covers what a tag does and does not
guarantee, what gets checked before one is pushed, and how the release then rolls
out.

It is written for maintainers, but published on purpose. If you run SchemaBot,
the checks below are the ones that decide whether moving from one tag to the next
is safe for you too.

**Upgrading?** Read the release notes for every tag you are skipping past, not
just the one you are landing on. They are the only place a required config edit,
a changed default, or a deploy ordering constraint is written down.

Two terms recur below. SchemaBot can run **everything in one process**, which is
the simplest way to deploy it and where most people start: one binary talks to
GitHub, owns its storage, and connects to your databases itself. That is *local
mode*. It can also be split in two, which is *gRPC mode*: a **control plane**
serves the API, receives GitHub webhooks, and owns storage, while one or more
**data planes** connect to the databases and drive the schema change, reached by
the control plane over gRPC. The modes can be mixed per database environment, and
[configuration.md](./configuration.md) covers both.

If you run local mode there is one process to upgrade, so
[Two planes, and deploy ordering](#two-planes-and-deploy-ordering) does not apply
to you. Everything else here does.

## Every release is deployed to production

SchemaBot is developed by Block and runs Block's own schema changes, so every tag
in this repository is a tag Block deploys. Releases are not cut on a calendar. A
tag goes out when merged work is ready to roll, reaches a staging control plane
and its data planes first, then production, and anything found along the way is
fixed forward in the next tag.

Note the order, because it is easy to read too much into it: **the tag comes
first and the rollout follows it.** Publishing a tag says nothing about how the
version behaves in production, since at that moment it has not run there. The
[pre-tag checks](#pre-tag-checks) are all a fresh tag has behind it, and they are
static checks rather than evidence from a running system.

What the arrangement gives you is a project under continuous production load. The
maintainers depend on the same code you do, on the same day you can get it, so
regressions surface against real schema changes fast and the fix ships as the next
tag rather than waiting on a report from the field.

The one practical caveat: production exposure builds up with a tag's age, so the
newest tag has the least of it. Take the newest tag for the newest work, and an
older one when you would rather have the mileage.

### What Block deploys

Kubernetes, with Helm, from a GitOps repository where the deployed version is a
checked-in value and a rollout is a pull request that changes it.

**The same container image this repository publishes.** No internal fork, no
separately built binary. The image comes from `ghcr.io/block/schemabot` at the
released tag, by way of a registry mirror that changes where the bytes are
fetched from but not which image runs. A bug you hit in the published image is a
bug in the image Block is running.

That covers the control planes and most of the data planes, including a
multi-region deployment that runs one control plane driving three regional data
planes entirely on the published image. A few data planes are instead existing
in-house services that embed SchemaBot as a Go module, which is the second shape
described under [deploy ordering](#two-planes-and-deploy-ordering).

**The same Helm chart this repository publishes**, via a thin wrapper chart that
declares the published chart as a dependency. The templates that render the
workload are the ones in [`charts/schemabot`](../charts/schemabot). The wrapper
only supplies deployment-specific values and the surrounding cluster resources
(networking and service mesh policy) that are specific to Block's environment and
have no business in a general-purpose chart.

**The same CLI.** Block engineers reach SchemaBot through an internal developer
CLI that embeds the published SchemaBot CLI rather than reimplementing it, so the
commands in these docs are the commands people actually run. Bumping that wrapper
is the last step of a rollout.

Control planes go first, then the data planes, except when the release is an
engine fix. See [Two planes, and deploy ordering](#two-planes-and-deploy-ordering)
for why.

## What a release produces

Pushing a `vX.Y.Z` tag triggers two workflows:

| Workflow | Artifacts |
| --- | --- |
| `Release` (GoReleaser) | `schemabot` binaries for linux/amd64, linux/arm64, darwin/arm64, plus checksums, attached to a GitHub Release |
| `Docker & Helm` | `ghcr.io/block/schemabot:vX.Y.Z` and the Helm chart at `oci://ghcr.io/block/charts/schemabot` |

Nothing else is required. There is no manual artifact step.

## What the version number means

Very little, on its own. SchemaBot is pre-1.0, and patch releases carry behavior
changes.

Compatibility is carried by the [pre-tag checks](#pre-tag-checks) and by the
release notes, which call out every change that needs an operator to do
something: a config edit, a changed default, a deploy ordering constraint, a
storage schema change. Treat the notes as the contract and the version number as
a label.

## Cadence and release boundaries

A release usually carries several merged commits. Every tag has to be rolled out
through staging and then production, so batching amortizes that time rather than
paying it per change.

There is no minimum, though. A single commit is a perfectly good release, and
reaching for one is the right call whenever waiting would cost more than the extra
rollout. If the next rollout step needs code that is only on `main`, cut the next
patch release instead of deploying an unreleased commit or sitting on the fix until
more work piles up.

What a batch should not do is put changes together that constrain each other. A
release cannot be rolled back in parts, so trouble in one change forces a decision
about all of them, and a change that needs its own rollout ordering drags the rest
of the batch along with it.

**Prefer the tip of `main`.** It is the default and usually the right release
commit. Everything merged since the last tag goes out together.

**When not to choose `main`:** when the range since the last tag holds a commit
that needs its rollout done a particular way. Bundling one of those makes every
other change in the tag inherit its constraint. So read the commits in the range
before tagging, and look for:

- Storage schema column renames or removals, which need the expand/contract
  sequence below
- Protobuf or API changes that require the data planes to roll before the control
  plane image
- Config shape changes, where the new binary rejects the deployed config or the
  running binary rejects the new one
- Data-plane changes that are not backward compatible with the deployed control
  plane

Found one? Tag the commit before it and let that release roll out, then give the
constrained change a tag of its own.

Column renames deserve special care: do them as expand/contract across several
releases rather than one. Add the new column and teach the code to tolerate both
shapes, release and deploy that everywhere, switch reads and writes in a later
release, and only drop the old column once every binary that touches it has
rolled. Keep the contraction step away from unrelated fixes, so an operator can
choose a pre-contraction release without giving up the fix they came for.

## Two planes, and deploy ordering

The two shapes, side by side:

```
local mode
  GitHub ──► schemabot ──► your databases
             API, webhooks, storage, engine: all one process

gRPC mode
  GitHub ──► control plane ──┬─gRPC─► data plane ──► databases
             API, webhooks,  ├─gRPC─► data plane ──► databases
             storage         └─gRPC─► data plane ──► databases
```

A data plane owns the connection to the databases it is responsible for and runs
the engine that drives the schema change. The control plane never touches those
databases; it decides what should happen and asks a data plane to do it. That is
what makes a fleet possible: one control plane, and a data plane per region,
account, or network boundary that the control plane could not reach itself.

This section is about gRPC mode. In local mode there is one process and one
artifact, so there is no ordering question: upgrade it and you are done.

In the usual gRPC-mode deployment both boxes above are **the same published
image**, started with different config. Nothing is embedded and nothing is
custom-built.

```
tag vX.Y.Z
  ├─ container image ──► control plane
  └─ container image ──► data planes     (same image, gRPC server mode)
```

Both therefore wait on the same artifact. They are still separate deployments
that you roll one at a time, so between the first roll and the last the two
planes are running different versions. Call that the skew window. When both
planes are the same image you control both rolls, so you can make the skew window
as short as back-to-back deploys allow.

A data plane does not have to be that image, though. A service can embed
SchemaBot as a Go module and provide its own data plane instead. The upgrade then
reaches it differently: rather than pulling a new image, its owners have to bump
the dependency, rebuild, and ship on whatever release schedule that service
follows. You control when your control plane rolls; you do not control that. The
skew window can be much longer, and you should assume it will be.

The ordering rules below are the same either way. What changes is how long you
live inside them.

**Control plane first, normally.** The gRPC contract is additive only (new
fields, never removed or renumbered), so a newer control plane and an older data
plane interoperate: unknown fields are ignored on the way in, absent fields read
as zero values on the way out.

**Data plane first when the change is in the engine.** In gRPC mode the plan and
its diff are computed by the data plane, not the control plane, so a release whose
point is a diffing or engine fix does nothing until a data plane runs it. The
control plane's own copy of the engine does not affect plan output in gRPC mode at
all. Upgrade a data plane first for those, and often it is the only plane that
needs the release.

Anything else that changes the order for a particular release is called out in its
release notes.

All of which is why mixed versions have to be genuinely tolerable, and the
additive-only rule is what buys that. A skew window is still a rollout state
rather than a resting place, so close it rather than letting the planes drift
several releases apart.

## Pre-tag checks

Run these against the commit range since the previous tag
(`git diff <previous-tag>..<release-commit>`, where the release commit is normally
the tip of `main`, or whichever earlier commit was chosen under
[Cadence](#cadence-and-release-boundaries)). Each one is here because it has caught
a real problem.

### 1. Removed or renamed config keys

The server config is decoded with `KnownFields(true)`, so an unknown key is a
hard startup error. Remove or rename a key here and every deployed config that
still sets it becomes a crashloop at boot, before the process can report anything
useful.

Diff the config structs. Additions are safe. Removals and renames need a
deprecation path, and the release notes have to name the exact key so operators
can grep their own configs.

### 2. Storage schema changes

SchemaBot bootstraps its own storage schema at startup (`EnsureSchema`), diffing
the embedded schema files against the live database and applying what is missing.
It **refuses destructive statements by default**: the offending statement is
skipped with a warning and a metric, startup continues, and the live schema keeps
the old shape until an operator opts in with
`storage.allow_destructive_schema_changes` (see
[configuration.md](./configuration.md)). That means a release that drops a column
still starts, running against a database where the drop never happened, so the
code in that release has to tolerate the old shape too.

Diff the embedded schema files. Additive changes need no action. A destructive
change is a coordinated operation and belongs in the release notes with
instructions, not in a routine patch release.

### 3. The public Go API

SchemaBot is importable as a Go library, not only runnable as a binary, so its
exported declarations are part of what a release ships. Removing one, or changing
its signature, breaks an importer's build at compile time, and almost nothing in
this repository will notice: the consumer-module tests (`e2e/consumermodule`)
build against the public startup surface the way a host binary does, but they
cover only that surface.

Diff the exported declarations across the range, and read the diff carefully: a
symbol can look removed when it was only moved between files, regenerated
(protobuf), or given a type alias that leaves the signature identical for
callers. Check what it looks like on the new tag before concluding anything.

A green `go build ./...` proves nothing on this point, since it only compiles
this module's own call sites. Something that builds cleanly here can still break
a call site we never see. When a break is unavoidable, name the package and
symbol in the release notes so anyone importing SchemaBot can find their call
sites quickly.

### 4. Resume compatibility for in-flight schema changes

**This one is specific to the MySQL engine**, which uses
[Spirit](https://github.com/block/spirit). Skip it when a release does not move
the Spirit dependency. The other engines do not have this exposure: a Vitess
change is a PlanetScale deploy request that the Vitess side owns and tracks, so
there is no checkpoint on SchemaBot's side that has to stay compatible.

How much an app restart costs depends on the change. Adding a column is usually
instant DDL and completes in milliseconds, so there is nothing to pick up. (Spirit
asserts `ALGORITHM=INSTANT` and lets MySQL decide, so when a table cannot take the
change instantly it falls back to the copy path below.) A change that needs
a table copy, such as adding an index, can run for hours to weeks on a large table.
To prevent an app restart from losing progress, Spirit keeps a small checkpoint
table alongside the table being changed and rewrites a single row in it as the copy
progresses: how far the copy has reached, how far verification has reached, the
binlog position, and the statement being applied. On restart it reads that row and
picks up from there instead of copying the table again.

This is typically safe, but when the Spirit dependency upgrades, it is important
to verify the checkpoint format is the same between the old and new versions:
diff Spirit's `pkg/checkpoint` between the two pins. A checkpoint format change
means the checkpoint table gets rebuilt from scratch, losing all table copy
progress and starting the copy over from the beginning. The best mitigation is to
ensure all long-running schema changes finish before doing the upgrade (e.g. via
the `schemabot status` CLI command).

### 5. Wire protocol

Only relevant to gRPC mode, but checked every release, since the two planes run
different versions during a rollout. Protobuf changes must be additive: new
fields with new numbers, no removals, no renumbering, no type changes. Diff the
`.proto` sources rather than the generated code, where regeneration noise buries
the field-level change.

### 6. Error baseline

Record the deployed version's error rate before the tag goes out. Without a
baseline, pre-existing noise reads as a regression and a real regression hides
inside it.

## Cutting the tag

1. Confirm the exact commit, per
   [Cadence](#cadence-and-release-boundaries). Re-read the SHA immediately before
   tagging, since `main` may have moved while you were running the checks.
2. Tag and push:
   ```bash
   git tag -a vX.Y.Z <sha> -m "vX.Y.Z"
   git push origin vX.Y.Z
   ```
3. Watch both workflows finish. `Release` and `Docker & Helm` run independently,
   so one can pass while the other fails.
4. Confirm the image actually published:
   ```bash
   docker manifest inspect ghcr.io/block/schemabot:vX.Y.Z
   ```
5. Write the release notes, operator actions first: config keys added or removed,
   changed defaults, deploy ordering constraints, storage schema changes.

## Fixing a bad release

Never delete or move a published tag. By the time you notice, image caches, Go
module proxies, and deployment tooling have already pulled it, and moving it
leaves two different builds answering to one version.

Ship the fix as the next patch release instead, and say in its notes what was
wrong with its predecessor so operators know whether they need to skip it.
