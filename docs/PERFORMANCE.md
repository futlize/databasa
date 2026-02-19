# Performance And Validation

## Suggested Acceptance Targets

Targets below are practical baseline goals for a single node (8 vCPU, NVMe, 128-d vectors):

- Insert throughput:
  - `wal_sync_mode=always`: >= 8k vectors/s
  - `wal_sync_mode=periodic`: >= 40k vectors/s
- Search latency under mixed load (`topK=10`):
  - p95 <= 25ms
  - p99 <= 60ms
- Mixed load stability:
  - sustained inserts + searches without deadlock/starvation
  - no blocking from internal resource throttling; monitor process memory under sustained load

## Reproducible Commands

### 1) Insert Throughput

```bash
go run ./cmd/benchmark -embedded=true -workers 4 -batch-size 1000
```

PowerShell:

```powershell
pwsh ./scripts/benchmark.ps1 -Embedded
```

### 2) Storage Microbenchmarks

```bash
go test ./internal/storage -run '^$' -bench BenchmarkVectorStore -benchtime 10s -benchmem
```

PowerShell:

```powershell
pwsh ./scripts/bench-storage.ps1 -Bench BenchmarkVectorStore -Seconds 10
```

### 3) Mixed Search+Insert Latency

`BenchmarkVectorStoreSearchMixedLoad` runs searches while background inserts keep pressure on ingest.

```bash
go test ./internal/storage -run '^$' -bench BenchmarkVectorStoreSearchMixedLoad -benchtime 10s -benchmem
```

### 4) Crash/Restart Validation (Manual)

1. Start server with `wal_sync_mode=always`.
2. Run sustained ingest with benchmark client.
3. Kill process abruptly (`SIGKILL` / taskkill `/F`).
4. Restart server using same `data_dir`.
5. Verify expected keys are queryable and counts are consistent.

## Recall Validation

For recall checks:

1. Run query set with ANN enabled (`CreateIndex`).
2. Run same query set with ANN disabled (`DropIndex`) as exact baseline.
3. Compare overlap@K for `topK=10` and `topK=50`.

This isolates ANN quality from ingestion/optimizer throughput.
