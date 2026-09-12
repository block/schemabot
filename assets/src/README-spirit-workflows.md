# Spirit workflow illustrations

These animations use the primary key GIF's 1100 × 690 canvas, system fonts, GitHub light
palette, fixed layout, and shared-palette encoding. Timing and progress values are
illustrative, not performance measurements.

- `spirit-ddl-selection.html` uses three independent MySQL 8.4 / InnoDB DDL examples:
  an eligible nullable column addition, an existing index rename, and an index addition.
  Each example holds for ten seconds. The native subset matches Spirit’s statement classifier;
  MySQL must still accept the operation on the actual table.
- `spirit-change-lifecycle.html` shows the table-copy path, including an optional scheduled
  cutover. The application route changes only at the swap. A full copy bar does not mean
  verification or cutover has finished. Continuous checks during a deferred wait are periodic.
  Its 22-second source timeline plays over 33.1 seconds: short phases receive four to five
  seconds each through per-frame delays in the renderer.
- `spirit-checkpoint-resume.html` shows logical responsibilities: SchemaBot coordinates the
  change, Spirit copies and reports progress, and MySQL stores the tables and checkpoints.
  These boxes are not separate required deployments. Rows pass through the Spirit process.
  A compatible restart repeats work beyond the saved checkpoint and replays retained logs.
  This loop remains 22 seconds.

From the repository root, with Node.js, Playwright, Chrome, and ImageMagick installed:

```sh
node scripts/render-spirit-workflows.cjs
```

The renderer reports frame progress and the final size for each GIF, then writes
`assets/spirit-ddl-selection.gif`, `assets/spirit-change-lifecycle.gif`, and
`assets/spirit-checkpoint-resume.gif`.
Set `NODE_PATH` if needed to resolve Playwright. Chrome is discovered through its `chrome`
channel; `CHROME` can override the executable. Temporary frames are cleaned up on exit.

To render only the decision and lifecycle illustrations:

```sh
node scripts/render-spirit-workflows.cjs spirit-ddl-selection spirit-change-lifecycle
```

The same progress and size output appears for those two GIFs; the checkpoint GIF is unchanged.
Consecutive identical frames share one image with a longer delay to keep reading pauses small.

After editing, inspect the encoded GIFs at the copy, interruption, recovery, and cutover
boundaries. Keep stationary elements fixed; use one palette to avoid text shimmering.
Check the explanation against Spirit's upstream copier, checkpoint, and deferred cutover
references linked in `docs/mysql.md`. Resume across Spirit binary versions is unsupported;
missing logs or an unusable checkpoint can require a fresh copy.

For the DDL examples, also check [MySQL 8.4 online DDL operations](https://dev.mysql.com/doc/refman/8.4/en/innodb-online-ddl-operations.html).
The examples assume an eligible table; they do not claim every column addition can run instantly.
