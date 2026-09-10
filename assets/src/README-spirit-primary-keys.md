# Spirit primary key illustration

`spirit-primary-keys.html` is the source for `../spirit-primary-keys.gif`, embedded in
[the MySQL guide](../../docs/mysql.md#choosing-a-primary-key). It shows primary-key
payload in secondary indexes, then numeric and index-query chunk boundaries.

All values are fictional. The animation is not a benchmark: frame timing does not
represent execution time, and work can overlap. The string is 25 ASCII bytes;
BIGINT is 8 bytes. Record overhead is excluded. The SQL is a simplified first-chunk
boundary query; Spirit explicitly forces the selected index in production.

Run from the repository root, with Node.js, Playwright, Chrome, and ImageMagick installed:

```sh
node scripts/render-spirit-primary-keys.cjs
```

The renderer logs frame progress and the final GIF size, then writes
`assets/spirit-primary-keys.gif`. Set `NODE_PATH` if needed to resolve Playwright;
`CHROME` overrides the browser executable. Temporary frames are removed after rendering.
The 26-second loop uses a fixed canvas, reading pauses, and a shared palette to keep
stationary text from shimmering. Inspect the encoded GIF after changing the source.

See the guide's upstream references before changing the explanation. In particular,
integer without auto-increment is not the optimistic chunker, sparse integer keys can
also use lookups, and instant changes do not copy the table. Keep comparisons about
actual key width rather than treating every VARCHAR as a wide key.
