# Contributing to SchemaBot

Thank you for your interest in contributing to SchemaBot! We welcome bug reports, feature requests, and pull requests.

## Filing Issues

- **Bug reports**: Include steps to reproduce, expected behavior, and actual behavior.
- **Feature requests**: Describe the use case and why the feature would be useful.

## Submitting Pull Requests

1. Fork the repository and create a feature branch from `main`.
2. Make your changes. Follow the existing code style and conventions.
3. Add or update tests for your changes. Run the full test suite:
   ```bash
   make test
   ```
4. Ensure your code builds and lints cleanly:
   ```bash
   make build
   make lint
   ```
5. Open a pull request against `main` with a clear description of your changes.

## Code Style

- Use `gofmt` for formatting.
- Use [testify](https://github.com/stretchr/testify) (`require` and `assert`) in tests.
- Wrap errors with context: `fmt.Errorf("what was being done: %w", err)`.
- Never use the word "migration" — use "schema change" instead.

## Development Setup

```bash
make setup         # Configure git hooks
make demo-full     # Start local services, apply schema, seed data
make demo-vitess   # Instead, start a local sharded Vitess cluster
make test          # Run all tests (unit + integration + e2e)
```

`make demo-vitess` runs against [LocalScale](./pkg/localscale/README.md), which
answers the PlanetScale API over an embedded Vitess cluster, so Vitess changes
can be developed and tested without a PlanetScale account. It is an alternative
to `make demo-full`, not a step after it: each target resets the Compose stack
before starting its own services, and `demo-vitess` brings up LocalScale alone,
applying no schema and seeding no data. It builds the LocalScale image first;
`make build-localscale-image` builds it on its own.

See [docs/architecture.md](./docs/architecture.md) for an overview of how SchemaBot works.

## Releases

Maintainers cut releases by pushing a `vX.Y.Z` tag. See [docs/release.md](./docs/release.md) for the process and the compatibility checks that run before a tag is published.

## License

By contributing to SchemaBot, you agree that your contributions will be licensed under the [Apache License 2.0](./LICENSE).
