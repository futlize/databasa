# Databasa Configuration Guide

This document explains the recommended production-ready `databasa.toml` (balanced profile) and every configuration field currently supported by the codebase.

Scope:
- Source of truth: `cmd/databasa/config.go` and `cmd/databasa/main.go`.
- Only real fields are used in `databasa.toml`.
- Missing but useful knobs are listed in a separate "Proposed additions" section.

## Recommended baseline

The provided `databasa.toml` is the `balanced` profile:
- Durable enough for production by using periodic WAL fsync (`write_mode=performance` + `wal_sync_mode=auto`).
- Lower tail-latency risk than fsync-on-every-batch.
- Conservative guardrails (`max_batch_size`, `max_ef_search`, deadline requirement).

Global change rule:
- All fields require process restart to take effect.
- This server has no live config reload path today.

## Server

| Field | Balanced default (code fallback) | What it does | Performance vs durability and tuning behavior | Recommended range | Runtime change | Interactions |
|---|---|---|---|---|---|---|
| `server.port` | `50051` (`50051`) | gRPC listen port. | No durability impact. Raise/lower only affects networking and deployment topology. | `1024-65535` in production | Restart required | Clients/load balancers/firewall rules must match this port. |
| `server.enable_reflection` | `false` (`false`) | Enables gRPC server reflection. | Reflection is operationally useful in dev, but exposes API surface and adds minor overhead. Keep off in hardened production. | `false` in prod, `true` in dev/staging | Restart required | No direct interaction with storage. |

## gRPC / HTTP

Databasa currently exposes gRPC only. There is no HTTP API listener config in the current code.

| Field | Balanced default (code fallback) | What it does | Performance vs durability and tuning behavior | Recommended range | Runtime change | Interactions |
|---|---|---|---|---|---|---|
| `server.grpc_max_recv_mb` | `64` (`32`) | Max inbound gRPC message size in MB. | Higher value allows larger inserts but increases worst-case memory per request. Lower value improves safety but can reject large batches. | `32-256` | Restart required | Must be compatible with `guardrails.max_batch_size`, vector dimension, and client message size. |
| `server.grpc_max_send_mb` | `64` (`32`) | Max outbound gRPC message size in MB. | Mostly affects large search responses. Higher allows large payloads; lower reduces memory pressure and response amplification risk. | `32-256` | Restart required | Interacts with `guardrails.max_top_k` and whether embeddings are returned. |

## Logging

No logging level/format/sink fields exist in the current config schema.

Current behavior:
- Logger uses Go standard log defaults from `cmd/databasa/main.go`.
- Startup report is printed at INFO-style level, but log verbosity is not configurable.

## Storage / WAL

| Field | Balanced default (code fallback) | What it does | Performance vs durability and tuning behavior | Recommended range | Runtime change | Interactions |
|---|---|---|---|---|---|---|
| `storage.data_dir` | `./data` (`./data`) | Root directory for persistent data (WAL, partitions, index files). | No direct latency effect unless storage medium changes. Faster disks improve both throughput and p99 latency. | Dedicated persistent volume/path | Restart required | Must have read/write permissions and enough disk. |
| `storage.compression` | `int8` (`int8`) | Vector storage compression mode (`int8` or `none`). | `int8` reduces memory and storage footprint; `none` preserves full precision at higher RAM/disk cost. | `int8` for most prod workloads | Restart required | Collection metadata persists compression at collection creation. |
| `storage.write_mode` | `performance` (`strict`) | Runtime write profile (`strict` or `performance`). | In current implementation, biggest impact is WAL behavior when `wal_sync_mode=auto`: `strict -> always`, `performance -> periodic`. | `performance` (balanced), `strict` (max durability) | Restart required | Interacts with `storage.wal_sync_mode`; `auto` resolves using this field. |
| `storage.wal_sync_mode` | `auto` (`auto`) | WAL fsync policy: `auto`, `always`, `periodic`, `none`. | `always` is strongest durability but can cause fsync latency cliffs. `periodic` smooths latency with bounded loss window. `none` is fastest and riskiest. | `auto` or `periodic` in balanced prod | Restart required | `auto` maps from `write_mode` in `cmd/databasa/main.go`. |
| `storage.wal_sync_interval_ms` | `10` (`10`) | Sync interval when WAL mode is periodic. | Lower values improve durability but increase fsync frequency. Higher values increase throughput and potential data-loss window on crash. | `5-50` ms | Restart required | Effective only for periodic mode (`auto` + `performance`, or explicit `periodic`). |
| `storage.wal_batch_wait_ms` | `1` (`1`) | Max wait to accumulate WAL append batch. | Higher improves batching throughput but increases write ACK latency. Lower reduces latency but may increase write amplification. | `1-5` ms | Restart required | Works with `wal_batch_max_ops` and ingest concurrency. |
| `storage.wal_batch_max_ops` | `1024` (`1024`) | Max operations in one WAL writer flush batch. | Higher can improve throughput on heavy ingest; too high can increase burst latency and memory spikes. Lower gives smoother latency but lower throughput ceiling. | `512-4096` | Restart required | Works with `wal_batch_wait_ms`, `wal_queue_size`, and client batch shape. |

## Compaction / Partition Lifecycle

| Field | Balanced default (code fallback) | What it does | Performance vs durability and tuning behavior | Recommended range | Runtime change | Interactions |
|---|---|---|---|---|---|---|
| `storage.active_partition_max_ops` | `20000` (`20000`) | Seals active partition after this many ops. | Higher reduces seal frequency and background churn, but makes active partition larger (more memory and active-scan cost). Lower does the opposite. | `5000-100000` | Restart required | Interacts with `active_partition_max_bytes_mb` and `partition_index_min_rows`. |
| `storage.active_partition_max_bytes_mb` | `64` (`64`) | Seals active partition after this byte threshold. | Higher favors ingest throughput; lower reduces active memory footprint and can stabilize latency under memory pressure. | `32-512` MB | Restart required | Interacts with `active_partition_max_ops`; first threshold hit seals. |
| `storage.partition_max_count` | `12` (`12`) | Target cap for sealed partition count before merges. | Higher reduces merge frequency but increases query fan-out and metadata overhead. Lower triggers merges sooner and can increase background IO/CPU. | `8-32` | Restart required | Interacts with `partition_merge_fan_in` and `merge_workers`. |
| `storage.partition_merge_fan_in` | `4` (`4`) | Number of partitions merged per merge operation. | Higher merges more aggressively (bigger merge jobs). Lower gives smaller merge steps but more rounds. | `2-8` | Restart required | Interacts with `partition_max_count` and `merge_workers`. |
| `storage.merge_workers` | `1` (`1`) | Number of background merge workers. | Higher can reduce partition backlog but may compete with foreground latency on limited CPUs. | `1-4` | Restart required | Interacts with `partition_max_count`, `partition_merge_fan_in`, and disk bandwidth. |

## Sharding

| Field | Balanced default (code fallback) | What it does | Performance vs durability and tuning behavior | Recommended range | Runtime change | Interactions |
|---|---|---|---|---|---|---|
| `storage.shards` | `4` (`8`) | Number of shards per collection (single node). | More shards can increase parallelism with high concurrency, but also increase coordination overhead. In current server batch path, per-shard writes are processed sequentially within a single batch RPC, so too many shards can hurt per-request latency. | `2-8` typical, `1` for minimal overhead | Restart required | Applies when creating collections; collection metadata persists shard count. |

## Indexing / HNSW

| Field | Balanced default (code fallback) | What it does | Performance vs durability and tuning behavior | Recommended range | Runtime change | Interactions |
|---|---|---|---|---|---|---|
| `storage.partition_index_min_rows` | `1024` (`512`) | Minimum sealed partition size before background ANN index build. | Higher avoids indexing tiny partitions and smooths background load. Lower improves ANN readiness for small datasets but can add churn. | `512-8192` | Restart required | Interacts with `active_partition_*`, `index_workers`, and ingest rate. |
| `storage.index_workers` | `2` (`2`) | Number of background index build workers. | Higher accelerates index catch-up but can steal CPU from request path; lower preserves foreground latency but delays ANN readiness. | `1-8` | Restart required | Interacts with `partition_index_min_rows`, `index_m`, and `index_ef_construction`. |
| `storage.optimizer_queue_cap` | `256` (`256`) | Shared queue capacity used by seal/index/merge scheduling channels. | Higher absorbs bursts but increases buffered background work and memory. Lower applies backpressure earlier and can reduce long tail spikes. | `128-2048` | Restart required | Interacts with `index_workers`, `merge_workers`, and ingest burst shape. |
| `storage.index_m` | `16` (`16`) | HNSW connectivity (`M`) default in storage options. | Higher improves recall potential at memory/build-cost increase. Lower reduces memory/build time but can reduce recall. | `8-48` | Restart required | CreateIndex RPC values can override; this is a default-level knob. |
| `storage.index_ef_construction` | `200` (`200`) | HNSW build-time exploration factor default. | Higher improves graph quality/recall but increases build CPU and latency. Lower builds faster with lower quality. | `100-400` | Restart required | Interacts with `index_m`, `index_workers`, and dataset size. |
| `storage.index_ef_search` | `64` (`50`) | Default search-time ef for ANN. | Higher improves recall but increases search latency/CPU. Lower improves latency but may reduce recall. | `32-256` | Restart required | Interacts with `guardrails.max_ef_search` and per-request ef_search values. |

## Caching

No explicit cache sizing or cache policy fields are currently available.

Current behavior:
- Search and vector states are handled by in-memory structures, but there is no dedicated query-result/vector cache config in `databasa.toml`.

## Resource Limits (CPU / Memory)

There are no direct CPU quota or memory budget fields in the current config schema.

Current behavior:
- Legacy resource throttling paths are intentionally no-op in `internal/resources/manager.go`.
- You can limit disk consumption through `guardrails.max_data_dir_mb`, but that is not a memory or CPU limit.
- For production hard limits, rely on container/runtime controls (cgroups, CPU/memory limits) until native fields are added.

## Queues / Backpressure

| Field | Balanced default (code fallback) | What it does | Performance vs durability and tuning behavior | Recommended range | Runtime change | Interactions |
|---|---|---|---|---|---|---|
| `storage.wal_queue_size` | `4096` (`8192`) | Capacity of WAL append request queue. | Higher tolerates burstier ingest but can increase memory usage and queueing delay under sustained overload. Lower triggers blocking/backpressure earlier. | `2048-16384` | Restart required | Interacts with `wal_batch_*`, client concurrency, and disk speed. |
| `guardrails.max_batch_size` | `512` (`1000`) | Upper bound for vectors per batch insert RPC. | Higher increases throughput potential per RPC but increases request memory and tail risk. Lower stabilizes memory and p99 but may reduce throughput ceiling. | `256-2000` | Restart required | Must fit with gRPC message limits and client batch settings. |

## Timeouts

| Field | Balanced default (code fallback) | What it does | Performance vs durability and tuning behavior | Recommended range | Runtime change | Interactions |
|---|---|---|---|---|---|---|
| `guardrails.require_rpc_deadline` | `true` (`false`) | Rejects RPCs that do not include a deadline. | Improves latency predictability and protects server from unbounded waits. Can break legacy clients that do not set deadlines. | `true` in production | Restart required | Client SDK/caller must set deadline on all requests. |

Notes:
- There is no server-level request timeout field today.
- Internal bounded waits use fixed constants in code (not config-driven).

## Observability / Metrics

No metrics/tracing/exporter fields currently exist in `databasa.toml`.

What exists today:
- Startup initialization report logging.
- Recovery diagnostics in startup logs.
- No configurable Prometheus endpoint in current schema.

## Guardrails and Safety Limits

| Field | Balanced default (code fallback) | What it does | Performance vs durability and tuning behavior | Recommended range | Runtime change | Interactions |
|---|---|---|---|---|---|---|
| `guardrails.max_top_k` | `256` (`256`) | Caps `top_k` in search requests. | Higher allows bigger result sets but increases CPU/response size and tail latency. Lower protects latency and memory. | `100-1000` | Restart required | Interacts with `grpc_max_send_mb` and client response expectations. |
| `guardrails.max_ef_search` | `1024` (`4096`) | Caps per-request ANN `ef_search`. | Higher can improve recall but can dramatically increase search latency/CPU. Lower keeps p95/p99 more predictable. | `256-2048` | Restart required | Must be >= typical requested `ef_search`; also interacts with `storage.index_ef_search`. |
| `guardrails.max_collection_dim` | `8192` (`8192`) | Max allowed vector dimension at collection creation. | Higher allows wider embeddings but increases memory/compute costs everywhere. Lower enforces safer upper bound. | `1024-16384` | Restart required | Interacts with batch sizing and gRPC message size. |
| `guardrails.max_data_dir_mb` | `102400` (`0`) | Max allowed data directory size in MB (`0` means unlimited). | A hard cap prevents disk-full outages. Too low can reject writes unexpectedly when close to limit. | `>= 10240` in production | Restart required | Checked during writes and index operations; interacts with `storage.data_dir` volume size. |

## Profiles

Use `balanced` as default.

### balanced (default)
Already represented by the provided `databasa.toml`.

### durable (strict WAL)
Exact overrides:

```toml
[storage]
write_mode = strict
wal_sync_mode = always
wal_sync_interval_ms = 10
wal_batch_wait_ms = 1
wal_batch_max_ops = 512
wal_queue_size = 2048

[guardrails]
require_rpc_deadline = true
```

Expected behavior:
- Strongest durability guarantees.
- Lowest ingest throughput and higher p95/p99 write latency due to fsync frequency.

### ingest_fast (benchmark/ingest mode)
Exact overrides:

```toml
[server]
grpc_max_recv_mb = 128
grpc_max_send_mb = 128

[storage]
write_mode = performance
wal_sync_mode = none
wal_batch_wait_ms = 3
wal_batch_max_ops = 4096
wal_queue_size = 16384
partition_index_min_rows = 4096
index_workers = 4
merge_workers = 2

[guardrails]
max_batch_size = 1000
require_rpc_deadline = true
```

Expected behavior:
- Maximum ingest throughput.
- Highest risk of data loss on crash (`wal_sync_mode=none`).
- More background catch-up and possible latency instability under mixed load.

## Proposed additions

These are not currently implemented fields. Keep them out of `databasa.toml` until code support is added.

| Proposed field | Type | Suggested default | Why it is needed | Where to wire in code |
|---|---|---|---|---|
| `guardrails.max_inflight_requests` | `int` | `0` (unbounded) | Explicit global cap for overload protection. | Add in `cmd/databasa/config.go`; enforce in `internal/server/server.go` with semaphore around RPC handlers. |
| `guardrails.request_timeout_ms` | `int` | `0` (disabled) | Central server timeout policy for requests missing deadlines. | Parse in `cmd/databasa/config.go`; apply in `internal/server/server.go` `contextWithBoundedTimeout`. |
| `storage.memory_budget_mb` | `int` | `0` (disabled) | Real memory backpressure (current manager is no-op). | Add in config structs; implement in `internal/resources/manager.go`; consume in server wait paths. |
| `storage.cpu_budget_percent` | `int` | `0` (disabled) | Prevent background workers from starving request path. | Add in config and apply to worker pools in `internal/storage/storage.go`. |
| `logging.level` | `string` | `info` | Control verbosity in production incidents. | Add in `cmd/databasa/config.go`; initialize logger in `cmd/databasa/main.go`. |
| `logging.format` | `string` (`text|json`) | `text` | Structured logs for ops pipelines. | Add in config; set logger formatter in `cmd/databasa/main.go`. |
| `metrics.enabled` | `bool` | `true` | First-class observability endpoint. | Add config; expose `/metrics` HTTP server in `cmd/databasa/main.go`. |
| `metrics.port` | `int` | `9090` | Prometheus scrape target control. | Same as above. |
| `http.enabled` | `bool` | `false` | Optional HTTP admin/readiness endpoints. | Add in config and startup path in `cmd/databasa/main.go`. |
| `http.port` | `int` | `8080` | Standardized ops endpoint port. | Same as above. |
| `storage.query_cache_mb` | `int` | `0` | Control and bound query cache memory. | Add in config; implement cache in `internal/storage/storage.go` search path. |

## Legacy keys and compatibility notes

Currently accepted but ignored:
- Section `[resources]` is ignored.
- `guardrails.max_concurrent_search` is ignored.
- `guardrails.max_concurrent_write` is ignored.

These legacy keys do not affect behavior and should not be relied on for production control.
