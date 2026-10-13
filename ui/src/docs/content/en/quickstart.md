---
title: Quick start
group: start
order: 3
summary: Install FluxMQ with one command, or run it from source or in Docker; check it and start demo traffic in a couple of minutes.
---

## Requirements {#requirements}

| What | Version | Why |
|---|---|---|
| Rust (rustup) | 1.88+ | building the broker and SDK |
| Node.js and pnpm | Node 20.19+, pnpm 9 | building the console only |
| macOS or Linux | — | |
| Docker | any recent | if you run the image instead of building |

## Fastest: a ready release {#install}

```bash
# Docker (amd64 / arm64)
docker run -d --name fluxmq -p 7420:7420 -p 7421:7421 -v fluxmq:/var/lib/fluxmq ghcr.io/futurist-network/fluxmq

# Linux server with systemd (x86_64 / aarch64, glibc 2.36+): binary, /etc/fluxmq/fluxmq.toml,
# a generated admin token (sudo cat /etc/fluxmq/admin-token) and the fluxmq service
curl -fsSL https://raw.githubusercontent.com/futurist-network/fluxmq/master/deploy/install.sh | sudo sh
```

The console is built into the binary: open `http://localhost:7421`. Settings live in `/etc/fluxmq/fluxmq.toml` (keys are the [flag names](/docs/ref-cli)); re-running the script upgrades and keeps the config and data.

## Option 1. From source (production-like) {#from-source}

Run from the repository root.

```bash
cd ui && pnpm install && pnpm build && cd ..        # console -> ui/dist, built into the binary
cargo build --release -p fluxmq-server -p fluxmq-client
./target/release/fluxmq --data-dir ./data          # broker: TCP 7420, HTTP 7421
```

In a second terminal, optionally, demo traffic that creates channels, load and real problems (a slow worker, a consumer that stalls, messages dead-lettered with `Invalid schema`):

```bash
./target/release/fluxmq-demo            # --speed 2 doubles the traffic
```

Open `http://localhost:7421`. If the binary was built before the console, that address shows a "FluxMQ is running" hint page: build the console, then rebuild the broker (or pass `--ui-dir ui/dist`).

## Option 2. Development mode in one command {#dev}

```bash
scripts/dev.sh
```

The script builds the broker and demo (debug), starts the broker on `./data-dev`, waits for `/api/health`, starts `fluxmq-demo` and the console dev server on `http://localhost:5173` with hot reload. `Ctrl-C` stops everything. Variables: `NO_DEMO=1`, `NO_UI=1`, `FLUXMQ_DATA_DIR=…`, `FLUXMQ_ADMIN_TOKEN=…`.

## Option 3. Docker {#docker}

```bash
deploy/build.sh                                                       # image fluxmq:dev (first build 6–10 min)
docker compose -f deploy/docker-compose.yml --profile single up -d    # console: http://localhost:37421
docker compose -f deploy/docker-compose.yml --profile single --profile demo up -d   # + demo traffic
docker compose -f deploy/docker-compose.yml --profile '*' down -v     # stop everything, delete data
```

Or as a single container:

```bash
docker run --rm -p 127.0.0.1:37420:7420 -p 127.0.0.1:37421:7421 \
  -v fluxmq-data:/var/lib/fluxmq fluxmq:dev
```

The image contains `fluxmq`, `fluxmq-bench` and `fluxmq-demo`; data lives in the `/var/lib/fluxmq` volume, the process runs as user `fluxmq` (uid 10001), the healthcheck is `/api/health`. Host ports in compose are in the 3xxxx range and bound to `127.0.0.1` only. A three-node TLS cluster is the `cluster` profile — see [3-node cluster with TLS](/docs/howto-cluster).

## Check it {#verify}

```bash
curl -s http://localhost:7421/api/health
# {"status":"ok","version":"0.7.0","node_id":"...","uptime_secs":12}

curl -s http://localhost:7421/api/channels | head -c 300
```

## Next steps {#next}

1. Look at [console basics](/docs/console-basics) and the [dashboard](/docs/dashboard).
2. [Create a channel](/docs/howto-create-channel) and [connect a client](/docs/howto-connect-client).
3. Before exposing the broker on a network, [secure it](/docs/howto-secure): auth is off by default and anyone can administer it.

> [!WARNING]
> Without `--admin-token` (or a created token) the broker runs in open mode: REST, the console and the client port are open to everyone as an admin. The console shows an "Auth disabled" warning at the top in this mode.
