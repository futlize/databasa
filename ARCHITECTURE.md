# Databasa Storage And Search Architecture

## Goals

- High ingest throughput with predictable latency.
- Immediate query visibility after successful upsert.
- Background ANN optimization that never blocks request ACK.
- Crash safety with WAL + partition persistence.

## Core Model

Each shard store is split into:

- `active partition` (mutable, write target)
- `sealed partitions` (immutable, read-optimized, ANN-eligible)

Write ACK path:

1. append WAL record(s) through a batched WAL writer
2. durability according to configured WAL mode (`always`, `periodic`, `none`)
3. apply record(s) to active partition and key/id visibility state
4. ACK

No ANN queue/index build/compaction step is in the ACK path.

## Why Partitions

- Active partition gives low-latency mutable ingest.
- Sealed partitions isolate heavy optimization from writes.
- ANN persistence is partition-scoped, so normal operation never rewrites a single global graph.
- Merge/compaction reduces partition fan-out over time.

## Background Optimizer

Background loops:

- `seal loop`: rotates active partition when thresholds are reached.
- `index loop`: builds HNSW for sealed partitions asynchronously.
- `merge loop`: merges older sealed partitions to control fragmentation.

All loops use bounded queues and context-aware waits.

## Search Fan-Out

Search always executes:

- active partition scan (always)
- sealed partition ANN search when index exists
- sealed partition brute-force scan when index is unavailable

Results are merged deterministically by `(distance, key)`.

This guarantees immediate visibility for acknowledged upserts because active partition is always searched, regardless of indexed hit count.

## Consistency Model

- `Insert/BatchInsert/Delete` are durable per configured WAL policy.
- Read-after-write for successful operations is guaranteed within a shard because active partition is on the read path.
- ANN is eventually optimized: correctness/visibility comes first, performance improves as index/merge workers catch up.

## WAL And Recovery

- WAL files are replayed on startup (rotated then active).
- Active WAL is checkpointed to a compact latest-active image to bound replay time.
- During seal, rotated WAL is retained until the new sealed partition is persisted.

## Key Configuration Knobs

From `[storage]`:

- WAL:
  - `wal_sync_mode`
  - `wal_sync_interval_ms`
  - `wal_batch_wait_ms`
  - `wal_batch_max_ops`
  - `wal_queue_size`
- Partition lifecycle:
  - `active_partition_max_ops`
  - `active_partition_max_bytes_mb`
  - `partition_max_count`
  - `partition_merge_fan_in`
  - `partition_index_min_rows`
- Optimizer workers:
  - `index_workers`
  - `merge_workers`
  - `optimizer_queue_cap`
- HNSW:
  - `index_m`
  - `index_ef_construction`
  - `index_ef_search`

## Performance Behavior

- Ingest throughput scales with WAL batching and active partition striping.
- Search latency remains stable under mixed load because insert ACK does not wait for ANN work.
- As more sealed partitions gain ANN and are merged, topK query latency decreases.

