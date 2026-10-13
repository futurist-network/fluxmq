# Contributing to FluxMQ

Bug reports, ideas and pull requests are welcome.

- **Bugs and ideas:** open an [issue](https://github.com/futurist-network/fluxmq/issues/new/choose).
  For a bug, include `fluxmq --version`, how you run it and the steps that reproduce it.
- **Security problems:** report them privately, see [SECURITY.md](SECURITY.md).
- **Larger changes:** open an issue first so we can agree on the approach before you write the code.

## Development setup

Needs Rust 1.88+, Node 20.19+ and pnpm 9.

```sh
scripts/dev.sh          # debug broker + demo traffic + console dev server on http://localhost:5173
```

Layout: `crates/` (the broker and the Rust SDK), `ui/` (the console, Vite + React + TypeScript),
`deploy/` (Docker, systemd, install script), `docs/`. Architecture: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Before you open a pull request

```sh
cargo fmt --all
cargo test --workspace            # unit and integration tests
cd ui && pnpm typecheck && pnpm lint && pnpm test
```

- Keep a pull request to one change, with tests for new behaviour.
- A new flag or REST endpoint also goes into the console docs (`ui/src/docs/content/{en,ru}`);
  `pnpm test` checks that every broker flag is documented.
- User-visible changes get a line in [CHANGELOG.md](CHANGELOG.md).

By contributing you agree that your contribution is licensed under the
[Apache License 2.0](LICENSE).
