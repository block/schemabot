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
make setup    # Configure git hooks
make demo     # Start local services, apply schema, seed data
make test     # Run all tests (unit + integration + e2e)
```

See [docs/architecture.md](./docs/architecture.md) for an overview of how SchemaBot works.

## License

By contributing to SchemaBot, you agree that your contributions will be licensed under the [Apache License 2.0](./LICENSE).
