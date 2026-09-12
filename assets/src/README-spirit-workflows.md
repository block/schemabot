# Spirit workflow illustrations

These animations use the primary key GIF's 1100 × 690 canvas, system fonts, GitHub light
palette, fixed layout, and shared-palette encoding. Each loop lasts 22 seconds; timing and
progress values are illustrative, not performance measurements.

- `spirit-change-lifecycle.html` shows the table-copy path, including an optional scheduled
  cutover. The application route changes only at the swap. A full copy bar does not mean
  verification or cutover has finished. Continuous checks during a deferred wait are periodic.
- `spirit-checkpoint-resume.html` shows logical responsibilities: SchemaBot coordinates the
  change, Spirit copies and reports progress, and MySQL stores the tables and checkpoints.
  These boxes are not separate required deployments. Rows pass through the Spirit process.
  A compatible restart repeats work beyond the saved checkpoint and replays retained logs.

From the repository root, with Node.js, Playwright, Chrome, and ImageMagick installed:

```sh
node scripts/render-spirit-workflows.cjs
```

The renderer reports frame progress and the final size for each GIF, then writes
`assets/spirit-change-lifecycle.gif` and `assets/spirit-checkpoint-resume.gif`.
Set `NODE_PATH` if needed to resolve Playwright. Chrome is discovered through its `chrome`
channel; `CHROME` can override the executable. Temporary frames are cleaned up on exit.

After editing, inspect the encoded GIFs at the copy, interruption, recovery, and cutover
boundaries. Keep stationary elements fixed; use one palette to avoid text shimmering.
Check the explanation against Spirit's upstream copier, checkpoint, and deferred cutover
references linked in `docs/mysql.md`. Resume across Spirit binary versions is unsupported;
missing logs or an unusable checkpoint can require a fresh copy.
