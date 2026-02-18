package storage

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkVectorStoreInsertThroughput(b *testing.B) {
	opts := DefaultOptions()
	opts.WALSyncMode = WALSyncPeriodic
	opts.WALSyncInterval = 5 * time.Millisecond
	store, err := OpenWithOptions(b.TempDir(), "bench", 128, "cosine", "none", opts)
	if err != nil {
		b.Fatalf("open store: %v", err)
	}
	defer store.Close()

	var id atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := id.Add(1)
			key := fmt.Sprintf("k-%d", n)
			vec := buildBenchmarkVec(n, 128)
			if _, err := store.Insert(context.Background(), key, vec); err != nil {
				b.Fatalf("insert: %v", err)
			}
		}
	})
}

func BenchmarkVectorStoreSearchMixedLoad(b *testing.B) {
	opts := DefaultOptions()
	opts.WALSyncMode = WALSyncPeriodic
	opts.WALSyncInterval = 5 * time.Millisecond
	opts.ActivePartitionMaxOps = 5000

	store, err := OpenWithOptions(b.TempDir(), "bench", 128, "cosine", "none", opts)
	if err != nil {
		b.Fatalf("open store: %v", err)
	}
	defer store.Close()

	const seedCount = 10000
	for i := 0; i < seedCount; i++ {
		key := fmt.Sprintf("seed-%d", i)
		if _, err := store.Insert(context.Background(), key, buildBenchmarkVec(uint64(i+1), 128)); err != nil {
			b.Fatalf("seed insert: %v", err)
		}
	}
	store.EnableANN(DefaultOptions().HNSWConfig)

	stopInsert := make(chan struct{})
	var insertID atomic.Uint64
	go func() {
		for {
			select {
			case <-stopInsert:
				return
			default:
			}
			n := insertID.Add(1)
			key := fmt.Sprintf("live-%d", n)
			_, _ = store.Insert(context.Background(), key, buildBenchmarkVec(uint64(n+seedCount), 128))
		}
	}()
	defer close(stopInsert)

	query := buildBenchmarkVec(42, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.SearchTopK(context.Background(), query, 10, 64); err != nil {
			b.Fatalf("search: %v", err)
		}
	}
}

func buildBenchmarkVec(seed uint64, dim int) []float32 {
	out := make([]float32, dim)
	x := seed*2862933555777941757 + 3037000493
	for i := 0; i < dim; i++ {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		out[i] = float32((x%10000)-5000) / 5000.0
	}
	return out
}
