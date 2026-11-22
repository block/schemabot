# Sharded databases

"Sharded" means different things to SchemaBot depending on the engine, and the commands and failure modes differ. Work out which one the database is before giving advice.

## Tell them apart

Read `type` from `schemabot.yaml` first.

| | MySQL rollout of several targets | Vitess on PlanetScale |
|---|---|---|
| `type` | `mysql` | `vitess` |
| Schema directory | `.sql` table files, one subdirectory per schema when targets hold different schemas | keyspace subdirectories, each with `.sql` files and a `vschema.json` |
| Server configuration | a `targets:` list (or a `deployments:` map) for the environment | one PlanetScale database |
| Who runs the change on each shard | SchemaBot, once per target | PlanetScale, through one deploy request |
| `--target` | names one member of the rollout | not used |

`type: mysql` alone doesn't tell you. Most MySQL databases have one target. It's a rollout of several targets when the plan comment says it rolls out to several, or lists target names. The server configuration that defines this lives outside the repo; see [Multi-target environment](https://github.com/block/schemabot/blob/main/docs/configuration.md#multi-target-environment).

## MySQL rollout of several targets

A MySQL database spread across several servers is still one SchemaBot database: one schema directory, one PR, and one check per environment.

A **target** is one server in the rollout. SchemaBot plans, applies, stops, and logs each target on its own, and the plan comment and `--target` use target names. A target can hold more than one shard of the data, so talk to the user in targets when the subject is something SchemaBot does.

### Writing the change

Write the change once, in the same files, whatever the number of targets. Never make a copy of a table file per target. When targets hold different schemas, each schema is one subdirectory of the schema directory, and the server configuration says which target gets which. See [Namespaces](https://github.com/block/schemabot/blob/main/docs/namespaces.md).

### Reading the plan

Each target is planned against its own live schema, so targets can need different work: a target missing an index gets a longer plan than the others. The plan shows the change once, shows what applies where, and lists any target that needs attention along with the `--target` that applies it on its own. An apply of the whole environment is refused while any target needs attention.

### How the rollout runs

By default:

- **Table by table.** Each table finishes on every target before any target starts the next one.
- **Targets copy in parallel, up to a server-wide cap.** The rest wait in list order. The cap can't be raised for one database.
- **Each target's cutover runs as soon as its own copy is ready.** A small target can finish while a large one is still copying.

The server can configure a different order; [Deployment order](https://github.com/block/schemabot/blob/main/docs/configuration.md#deployment-order) describes the options.

### Commands

The [command policy](command-policy.md) applies to every command below.

| To | Command | Notes |
|---|---|---|
| Apply every target | `schemabot apply -e <env>` | The check passes once every target has the change. |
| Try one target first | `schemabot apply -e <env> --target <target>` | Use a target name from the plan, or `deployment/target` when two deployments share a name. An unknown name is refused with the list of valid ones. The check keeps blocking merge until an apply of the whole environment finishes the rest; that apply skips targets that already have the change. |
| Confirm an apply | `schemabot apply-confirm -e <env>` | Takes no `--target`; it confirms the target its apply named. |
| Hold a cutover | `schemabot apply -e <env> --target <target> --defer-cutover` | `--defer-cutover` is refused on an apply of the whole environment, because each target's cutover runs as soon as its copy is ready. |
| Stop or resume | `schemabot stop <apply-id> -e <env>`, `schemabot start <apply-id> -e <env>` | Act on the whole rollout: copying targets stop at their checkpoint, queued targets don't start, finished targets keep the change. To stop one target alone, it must have been applied alone with `--target`, which gives it its own apply ID. |
| Undo | apply the schema files without the change | `rollback` is refused for a rollout of several targets, even for an apply narrowed with `--target`. To undo one target, apply the previous schema with that `--target`. |

### When a target fails

Under the default failure policy, a failed target stops the rollout from spreading: queued targets don't start, no target starts the next table, and targets already copying finish and then wait at cutover until someone stops or cancels the apply. Targets that already finished keep the change. Don't call this a partial rollback: the finished targets have the new schema.

The failure comment names the failed target. Read its logs:

```bash
schemabot logs <apply-id> --target <target>
schemabot logs <apply-id> --target <target> --deployment <deployment>
```

`--target` for a target the apply never ran on is refused, and the error lists the targets it did run on. [Read the logs](https://github.com/block/schemabot/blob/main/docs/cli.md#read-the-logs) has more.

## Vitess on PlanetScale

PlanetScale spreads the database across shards itself. SchemaBot applies the whole PR as one deploy request, so there is one target per environment and no `--target`. Load [Authoring](authoring.md#repo-layout) for the keyspace layout, [Sharded Vitess tables](vitess-sharded-tables.md) to add or drop a sharded table, and [Recovery](recovery.md) for the revert window.
