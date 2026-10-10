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
- `spirit-throttling.html` shows an application and Spirit's copy writing to one Aurora writer.
  Application traffic grows, average commit latency climbs past Spirit's 100ms threshold, and the
  copy waits before each chunk until latency recovers. Traffic, latency, and timing are
  illustrative.
- `spirit-write-thread-scaling.html` steps Spirit's write-thread controller once per tick: below
  the band it adds a thread, inside it holds, above it sheds one, and at the limit it halves the
  pool while the copy pauses. Load values are illustrative; the zone order matches Spirit's
  `pkg/autoscale`. The renderer plays it at 1.6x its 24-second timeline.
- `spirit-aurora-upsize.html` retells a staging schema change on a 107M-row table that moved from
  `db.r6g.large` to `db.r6g.2xlarge`. The ETAs, CPU, and commit latency are the observed values;
  the elapsed timeline before the upsize is illustrative, and the copy progress follows the ETAs;
  the thread pools follow Spirit's sizing for each instance class (small-instance mode below
  4 vCPUs, write threads from vCPUs minus two up to twice that).

From the repository root, with Node.js, Playwright, Chrome, and ImageMagick installed:

```sh
node scripts/render-spirit-workflows.cjs
```

The renderer reports frame progress and the final size for each GIF, then writes
one GIF per source in `assets/`, named after it (for example `assets/spirit-throttling.gif`).
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

For the throttling, scaling, and upsize illustrations, check the explanation against Spirit's
[throttler reference](https://github.com/block/spirit/blob/main/pkg/throttler/README.md) and the
thresholds in `pkg/autoscale`; update the animation when either changes.
