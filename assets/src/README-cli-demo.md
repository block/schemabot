# CLI guide animations

These GIFs illustrate five operator workflows using fictional data passed
through the production Go templates and interactive `commands.WatchModel`:

- `cli-plan-apply.gif`: inspect an index plan, confirm with yes, and follow an apply to completion
- `cli-ops.gif`: move from fleet status to a MySQL change, watch throttling and copying, detach, and follow logs through completion
- `cli-fleet.gif`: list changes, attach to the live watcher, detach with Esc, and list plans
- `cli-cutover.gif`: see a throttle reason, finish copying, defer the swap, and press Enter to cut over
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
width as well as their native size (1100 × 670, or 1100 × 740 for the shard view). Keep the rendering method and timing choices documented here.

Pass a scenario name, such as `cli-vitess`, as the renderer’s first argument
to refresh only that GIF. The PlanetScale fixture checks Enter against start
and skip-revert requests, and checks that **c** sends the stop request used
by the PlanetScale engine to cancel the deploy request.

The operations animation runs `LogsCmd` against a loopback logs response, including its real follow prompt and colored formatter. The fixture cancels after the initial window; no external logs are fetched. Log messages match the queue and derived-state events in the server.
