# Databasa

Databasa is a vector database with a gRPC API for storage and similarity search.

## Proposal

The project exists to offer a simple-to-operate vector foundation, focused on:

- local or self-hosted execution
- direct gRPC API for integration with any stack
- on-disk persistence
- operation with Docker or Linux service (systemd)
- API guardrails and operational limits

## Current scope

- Protocol: gRPC
- Service API: `databasa.Databasa`
- Proto: `proto/databasa.proto`
- Default config: `databasa.toml`
- Default port: `50051`
- Authentication: API key required by default
- Metrics: `COSINE`, `L2`, `DOT_PRODUCT`
- Main operations: collections, insert/get/delete, batch insert, and search

## Ingestion and search architecture

- Decoupled write path: `upsert -> WAL (durable according to policy) -> active partition`.
- Search always queries `active partition` + `sealed partitions`.
- ANN (HNSW) and merge/compaction run in the background per partition, without blocking write ACK.
- After a successful `Insert`/`BatchInsert`, the vector becomes immediately queryable.

## Basic usage

### 1) Run locally

```bash
go run ./cmd/databasa
```

With an explicit configuration file:

```bash
go run ./cmd/databasa -config ./databasa.toml
```

### 2) Build the binary

```bash
go build -o ./bin/databasa ./cmd/databasa
./bin/databasa -config ./databasa.toml
```

### 2.1) Build a release for Ubuntu (linux/amd64)

PowerShell:

```powershell
New-Item -ItemType Directory -Force -Path .\dist | Out-Null
$env:CGO_ENABLED="0"; $env:GOOS="linux"; $env:GOARCH="amd64"; go build -trimpath -ldflags "-s -w" -o ./dist/databasa_linux_amd64 ./cmd/databasa
```

Bash:

```bash
mkdir -p ./dist
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "-s -w" -o ./dist/databasa_linux_amd64 ./cmd/databasa
```

### 3) Run with Docker

```bash
cp .env.example .env
docker compose up --build -d
docker compose down
```

### 4) Interactive CLI quickstart

Start the server:

```bash
./bin/databasa -config ./databasa.toml
```

In another terminal, open the interactive shell:

```bash
./bin/databasa --cli --tls off
```

CLI targets are loopback-only (`127.0.0.1`/`localhost`/`::1`).

On startup, the CLI connects immediately.

- If auth is required and users already exist, it prompts credentials before opening the prompt.
- If the server has no users yet, it enters bootstrap flow and creates the first admin user interactively.

Example bootstrap flow:

```text
No users found. Bootstrap initial admin user.
admin user: admin
admin password:
confirm password:
bootstrap admin admin created and authenticated
key id: 71560500497eb51d2ecc
api key (shown once): dbs1.71560500497eb51d2ecc.<secret>
store this api key securely. Databasa never stores plaintext keys.
```

In CLI user commands, `PASSWORD` always triggers a hidden prompt for the API key secret.
Generated keys follow: `dbs1.<key_id>.<secret>`.
Statements containing `PASSWORD` are not stored in CLI history.

Databasa sessions are connection-bound, so each new channel must run `Login` again.

## Benchmark (fixed 200k)

The insert benchmark now uses a fixed count of `200000` vectors.

```bash
go run ./cmd/benchmark -embedded=true -workers 4 -batch-size 1000
```

Notes:
- Embedded benchmark now runs with production auth interceptors by default and performs channel `Login` automatically.
- To explicitly run insecure embedded mode for local dev only, add `-embedded-dev-no-auth=true`.

PowerShell:

```powershell
go run ./cmd/benchmark -embedded=true -workers 4 -batch-size 1000
```

## Minimal configuration

`databasa.toml` file:

```toml
[server]
port = 50051
grpc_max_recv_mb = 32
grpc_max_send_mb = 32
enable_reflection = false

[storage]
data_dir = ./data
shards = 8
compression = int8
write_mode = strict
wal_sync_mode = auto
wal_sync_interval_ms = 10
wal_batch_wait_ms = 1
wal_batch_max_ops = 1024
wal_queue_size = 8192
active_partition_max_ops = 20000
active_partition_max_bytes_mb = 64
partition_max_count = 12
partition_merge_fan_in = 4
partition_index_min_rows = 512
index_workers = 2
merge_workers = 1
optimizer_queue_cap = 256
index_m = 16
index_ef_construction = 200
index_ef_search = 50

[guardrails]
max_top_k = 256
max_batch_size = 1000
max_ef_search = 4096
max_collection_dim = 8192
max_data_dir_mb = 0
require_rpc_deadline = false

[security]
auth_enabled = true
require_auth = true
api_key_header = authorization
tls_enabled = true
tls_cert_file = ./certs/server.crt
tls_key_file = ./certs/server.key
tls_client_auth = none
```

`write_mode`:
- `strict`: strict WAL durability profile (default `always`).
- `performance`: throughput-oriented WAL durability profile (default `periodic`).

`wal_sync_mode` (`auto|always|periodic|none`) can override the durability policy derived from `write_mode`.

## Linux service (basic)

```bash
# Without Go (uses precompiled release by default):
sudo ./scripts/install.sh

# Optional: pin a specific release tag (default = latest)
# sudo env DATABASA_RELEASE_TAG=v0.1.2 ./scripts/install.sh
#
# Optional: explicit URL for binary/file .tar.gz/.tgz
# export DATABASA_BIN_URL="https://.../databasa_linux_amd64"
# sudo ./scripts/install.sh

# Optional: local compile (development workflow)
go build -o ./bin/databasa ./cmd/databasa
sudo ./scripts/install.sh
databasa status
databasa logs --follow
databasa --cli --insecure
```

In Linux service mode, installer adds the invoking user to group `databasa` for CLI access.
Open a new shell once (or run `newgrp databasa`) after installation.

## Additional documentation

- Detailed API: `docs/README.API.md`
- Security guide: `SECURITY.md`
- Interactive CLI: `docs/cli.md`
- Linux service: `docs/LINUX_SERVICE.md`
- Linux configuration: `docs/README.SERVER.CONFIG.LINUX.md`
- Internal architecture: `docs/ARCHITECTURE.md`
- Additional configuration notes: `docs/CONFIG.md`
- Performance/benchmarks: `docs/PERFORMANCE.md`
