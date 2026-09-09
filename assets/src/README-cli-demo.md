# CLI guide animations

These GIFs illustrate operator workflows using fictional data passed
through the production Go templates and interactive `commands.WatchModel`:

- `cli-plan-apply.gif`: inspect an index plan, confirm with yes, and follow an apply to completion
- `cli-ops.gif`: list databases, pull a live schema with lint findings, show the latest 20 of 500 changes, then follow engine logs through completion
- `cli-cutover.gif`: finish copying, wait for the swap, and press Enter to cut over
- `cli-throttle.gif`: see commit-latency pause copying, read the labeled docs link, and watch it resume
- `cli-rollback.gif`: review the DDL to restore an index, confirm with yes, then follow the new apply to completion
- `cli-stop.gif`: press s through the real handler, await Stopped, then start and follow the resumed change to completion
- `cli-vitess.gif`: create a deploy request, deploy with Enter, follow four shards, and close the revert window

They are template-based illustrations, not recordings of a live database.
Commands are typed by the renderer, long operations are compressed, and the
frames show selected output views. The guide also includes text examples so
readers can copy commands and read output without watching an animation.

## Regenerate

From the repository root, generate the output with the current templates:

```sh
go run ./pkg/cmd/docdemo/main.go > assets/src/cli-demo.json
```

The throttle scenario enables the production terminal hyperlink helper, matching
the labeled links in `list-plans` and `status`. Plain output prints the full URL.

The generator writes a JSON array of scenarios to that file. It uses a
loopback-only HTTP fixture to feed typed progress responses through
the real API client and interactive model. It does not connect to a database
or an external service. The fixture verifies the Enter, Esc, and stop handlers,
including the target of control requests and disabled controls during cutover.
The apply confirmation text comes from the command source. The Go file has an
`ignore` build tag so it is only compiled when explicitly run.

With Playwright, its Chromium browser (or local Chrome), and ImageMagick
available, render the GIFs:

```sh
node scripts/render-cli-demo.cjs
```

The renderer reports the duration, byte size, and temporary frame directory
for each GIF. It rejects frames with overflowing output. Set `CHROME` to a
browser executable if needed; `NODE_PATH` can point to an existing Playwright
installation. The HTML, JSON, and generator are all kept here so the output
can be refreshed when CLI templates change.

Review a plan frame, the fleet table, the throttle reason and ETA, the waiting
state, and the completed state after rendering. Check the GIFs at README
width as well as their native size (1100 × 670, 1100 × 740 for the shard and rollback views, or 1100 × 960 for the fleet view). Keep the rendering method and timing choices documented here.

Pass a scenario name, such as `cli-vitess`, as the renderer’s first argument
to refresh only that GIF. The PlanetScale fixture checks Enter against start
and skip-revert requests, and checks that **c** sends the stop request used
by the PlanetScale engine to cancel the deploy request.

The operations animation runs `LogsCmd` against a loopback logs response, including its real follow prompt and colored formatter. The fixture cancels after the initial window; no external logs are fetched. Log messages match the queue event, Spirit runner output, and replica throttler. Selected batches arrive at compressed intervals, with the most recent 14 terminal lines kept in view as the tail scrolls.

The inventory uses the real databases command against a loopback fixture. The status fixture totals 500 changes and renders the default 20 rows, shown together in a stationary view.

The fleet pull view enables the production interactive color setting and shows the entire result in one frame. Each fleet command finishes typing, then pauses briefly before output appears.

All animations render at 12 frames per second. The cutover copy advances in five-point steps, with brief holds for interactive controls.

The rollback illustration restores a removed index, which can require a row copy; the index drop in the text-only declined example can finish through native MySQL DDL. The rollback scenario runs `RollbackCmd` with both no and yes against a loopback fixture. It checks the source apply, target environment, request order, lock target, reviewed plan ID, and unsafe acknowledgment. Declining must issue no lock or apply request. The accepted command runs with `Watch: true`, opens its watcher automatically, and polls the new apply ID through 25%, 100%, and completion. Production watcher views provide the selected animation frames. No database is contacted. The preview and submitted output are separate selected terminal views, with input echo added for the animation.

The rollback command runs in a child process with terminal input supplied by `scripts/cli-demo-tty.py` (Python 3 standard library). This exercises the actual automatic Bubble Tea startup after confirmation, including its completion exit. The fixture rejects a wrong apply ID, unexpected requests, or polling after completion. The helper has a bounded runtime and never opens the user's terminal.

Rollback previews use the production boxed context and dialect-aware SQL formatting. A canonical comparison preserves identifiers and literal values when a display transformation would change them. All remaining scenarios use production SQL highlighting; the operations pull view retains the multiline schema supplied by the engine.
