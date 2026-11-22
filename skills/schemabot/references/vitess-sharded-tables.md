# Adding a sharded Vitess table

A sharded Vitess table takes its ids from a sequence table in an unsharded keyspace. Using the [keyspace layout](authoring.md#repo-layout), adding a `payments` table sharded by `user_id`, with ids from a sequence, takes four edits in one PR.

**1. The table**, a new file `shop_sharded/payments.sql`. No `AUTO_INCREMENT` on a sharded table: ids come from the sequence, which step 2 wires up.

```sql
CREATE TABLE `payments` (
  `id` bigint unsigned NOT NULL,
  `user_id` bigint unsigned NOT NULL,
  `amount_cents` bigint NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
```

**2. Its VSchema entry**, added to the `tables` map in `shop_sharded/vschema.json`. Add the entry and leave the rest of the file alone. The `column_vindexes` entry is the sharding decision: rows land on shards by `user_id`, so pick the column the application queries by. `"name": "hash"` refers to a vindex in the keyspace's top-level `vindexes` map; reuse one that is already there.

```json
"payments": {
  "column_vindexes": [
    { "column": "user_id", "name": "hash" }
  ],
  "auto_increment": { "column": "id", "sequence": "payments_seq" }
}
```

**3. The sequence table**, a new file `shop/payments_seq.sql` in the **unsharded** keyspace. `COMMENT='vitess_sequence'` is required: it is what makes Vitess treat the table as a sequence.

```sql
CREATE TABLE `payments_seq` (
  `id` int unsigned NOT NULL DEFAULT '0',
  `next_id` bigint unsigned DEFAULT NULL,
  `cache` bigint unsigned DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='vitess_sequence'
```

**4. Its VSchema entry**, added to the `tables` map in `shop/vschema.json`:

```json
"payments_seq": { "type": "sequence" }
```

Open the PR and the plan shows all four pieces; the deploy request applies them as one unit.

To **drop** a sharded table, reverse it: delete both `.sql` files and both VSchema entries in one PR. Drops are unsafe and follow the application-first deploy order in [Unsafe changes and drops](change-behavior.md#unsafe-changes-and-drops).

The repository's [Vitess example](https://github.com/block/schemabot/tree/main/examples/vitess/schema) has a complete sharded and unsharded keyspace pair.
