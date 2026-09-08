# CLI guide animations

These GIFs illustrate three operator workflows using fictional data passed
through the production Go templates in `pkg/cmd/internal/templates`:

- `cli-plan-apply.gif`: inspect an index plan and follow an apply to completion
- `cli-fleet.gif`: list changes across databases, inspect one apply, and list plans
- `cli-cutover.gif`: see a throttle reason, finish copying, defer the swap, and cut over

They are template-based illustrations, not recordings of a live database.
Commands are typed by the renderer, long operations are compressed, and the
frames show selected output views. The guide also includes text examples so
readers can copy commands and read output without watching an animation.

## Regenerate

From the repository root, generate the output with the current templates:

```sh
go run ./pkg/cmd/docdemo/main.go > assets/src/cli-demo.json
```

The generator writes a JSON array of scenarios to that file. It makes no
network requests and does not connect to a database. The Go file has an
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
width as well as their native 1100 × 670 size. Keep the fictional-data label
visible when changing the presentation.
