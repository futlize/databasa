package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/futlize/databasa/pkg/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestServerCRUDAndBruteForceSearch(t *testing.T) {
	srv, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()

	ctx := context.Background()
	if _, err := srv.CreateCollection(ctx, &pb.CreateCollectionRequest{
		Name:      "docs",
		Dimension: 2,
		Metric:    pb.Metric_L2,
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}

	if _, err := srv.Insert(ctx, &pb.InsertRequest{
		Collection: "docs",
		Key:        "a",
		Embedding:  []float32{0, 0},
	}); err != nil {
		t.Fatalf("insert a: %v", err)
	}
	if _, err := srv.Insert(ctx, &pb.InsertRequest{
		Collection: "docs",
		Key:        "b",
		Embedding:  []float32{10, 10},
	}); err != nil {
		t.Fatalf("insert b: %v", err)
	}

	getResp, err := srv.Get(ctx, &pb.GetRequest{Collection: "docs", Key: "a"})
	if err != nil {
		t.Fatalf("get a: %v", err)
	}
	if !reflect.DeepEqual(getResp.Embedding, []float32{0, 0}) {
		t.Fatalf("unexpected embedding: got=%v", getResp.Embedding)
	}

	searchResp, err := srv.Search(ctx, &pb.SearchRequest{
		Collection: "docs",
		Embedding:  []float32{1, 1},
		TopK:       1,
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(searchResp.Results) != 1 || searchResp.Results[0].Key != "a" {
		t.Fatalf("unexpected search result: %+v", searchResp.Results)
	}

	if _, err := srv.Delete(ctx, &pb.DeleteRequest{Collection: "docs", Key: "a"}); err != nil {
		t.Fatalf("delete a: %v", err)
	}
	if _, err := srv.Get(ctx, &pb.GetRequest{Collection: "docs", Key: "a"}); status.Code(err) != codes.NotFound {
		t.Fatalf("expected not found after delete, got: %v", err)
	}

	listResp, err := srv.ListCollections(ctx, &pb.ListCollectionsRequest{})
	if err != nil {
		t.Fatalf("list collections: %v", err)
	}
	if len(listResp.Collections) != 1 {
		t.Fatalf("unexpected collections count: %d", len(listResp.Collections))
	}
	if listResp.Collections[0].Count != 1 {
		t.Fatalf("unexpected active vector count: %d", listResp.Collections[0].Count)
	}
}

func TestServerIndexStaysConsistentOnOverwriteAndDelete(t *testing.T) {
	srv, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()

	ctx := context.Background()
	if _, err := srv.CreateCollection(ctx, &pb.CreateCollectionRequest{
		Name:      "docs",
		Dimension: 2,
		Metric:    pb.Metric_L2,
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}

	mustInsert := func(key string, embedding []float32) {
		t.Helper()
		if _, err := srv.Insert(ctx, &pb.InsertRequest{
			Collection: "docs",
			Key:        key,
			Embedding:  embedding,
		}); err != nil {
			t.Fatalf("insert %s: %v", key, err)
		}
	}

	mustInsert("a", []float32{0, 0})
	mustInsert("b", []float32{5, 5})

	if _, err := srv.CreateIndex(ctx, &pb.CreateIndexRequest{Collection: "docs"}); err != nil {
		t.Fatalf("create index: %v", err)
	}

	mustInsert("a", []float32{10, 10}) // overwrite "a"

	resp, err := srv.Search(ctx, &pb.SearchRequest{
		Collection: "docs",
		Embedding:  []float32{0, 0},
		TopK:       1,
	})
	if err != nil {
		t.Fatalf("search after overwrite: %v", err)
	}
	if len(resp.Results) != 1 || resp.Results[0].Key != "b" {
		t.Fatalf("expected b as nearest after overwrite, got: %+v", resp.Results)
	}

	if _, err := srv.Delete(ctx, &pb.DeleteRequest{Collection: "docs", Key: "b"}); err != nil {
		t.Fatalf("delete b: %v", err)
	}

	resp, err = srv.Search(ctx, &pb.SearchRequest{
		Collection: "docs",
		Embedding:  []float32{0, 0},
		TopK:       1,
	})
	if err != nil {
		t.Fatalf("search after delete: %v", err)
	}
	if len(resp.Results) != 1 || resp.Results[0].Key != "a" {
		t.Fatalf("expected only a after deleting b, got: %+v", resp.Results)
	}
}

func TestServerSearchValidation(t *testing.T) {
	srv, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()

	ctx := context.Background()
	if _, err := srv.CreateCollection(ctx, &pb.CreateCollectionRequest{
		Name:      "docs",
		Dimension: 3,
		Metric:    pb.Metric_COSINE,
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}

	if _, err := srv.Search(ctx, &pb.SearchRequest{
		Collection: "docs",
		Embedding:  []float32{1, 2, 3},
		TopK:       0,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for top_k=0, got: %v", err)
	}

	if _, err := srv.Search(ctx, &pb.SearchRequest{
		Collection: "docs",
		Embedding:  []float32{1, 2},
		TopK:       1,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for dimension mismatch, got: %v", err)
	}

	if _, err := srv.Search(ctx, &pb.SearchRequest{
		Collection: "docs",
		Embedding:  []float32{1, float32(math.NaN()), 3},
		TopK:       1,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for non-finite embedding, got: %v", err)
	}
}

func TestServerGuardrailsBatchAndTopK(t *testing.T) {
	srv, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()
	srv.ConfigureGuardrails(GuardrailConfig{
		MaxTopK:      2,
		MaxBatchSize: 2,
	})

	ctx := context.Background()
	if _, err := srv.CreateCollection(ctx, &pb.CreateCollectionRequest{
		Name:      "docs",
		Dimension: 2,
		Metric:    pb.Metric_COSINE,
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}

	if _, err := srv.BatchInsert(ctx, &pb.BatchInsertRequest{
		Collection: "docs",
		Items: []*pb.BatchInsertItem{
			{Key: "a", Embedding: []float32{1, 0}},
			{Key: "b", Embedding: []float32{0, 1}},
			{Key: "c", Embedding: []float32{1, 1}},
		},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for batch size limit, got: %v", err)
	}

	if _, err := srv.Insert(ctx, &pb.InsertRequest{
		Collection: "docs",
		Key:        "x",
		Embedding:  []float32{1, float32(math.Inf(1))},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for non-finite insert embedding, got: %v", err)
	}

	if _, err := srv.Insert(ctx, &pb.InsertRequest{
		Collection: "docs",
		Key:        "a",
		Embedding:  []float32{1, 0},
	}); err != nil {
		t.Fatalf("insert a: %v", err)
	}

	if _, err := srv.Search(ctx, &pb.SearchRequest{
		Collection: "docs",
		Embedding:  []float32{1, 0},
		TopK:       3,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for top_k limit, got: %v", err)
	}
}

func TestServerRequireDeadline(t *testing.T) {
	srv, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()
	srv.ConfigureGuardrails(GuardrailConfig{RequireRPCDeadline: true})

	if _, err := srv.ListCollections(context.Background(), &pb.ListCollectionsRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument when deadline is missing, got: %v", err)
	}
}

func TestServerDataDirLimit(t *testing.T) {
	srv, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()
	srv.ConfigureGuardrails(GuardrailConfig{MaxDataDirBytes: 1})

	ctx := context.Background()
	if _, err := srv.CreateCollection(ctx, &pb.CreateCollectionRequest{
		Name:      "docs",
		Dimension: 2,
		Metric:    pb.Metric_COSINE,
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}

	if _, err := srv.Insert(ctx, &pb.InsertRequest{
		Collection: "docs",
		Key:        "a",
		Embedding:  []float32{1, 0},
	}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected resource exhausted for data dir limit, got: %v", err)
	}
}

func TestServerSearchEmbeddingsAreOptional(t *testing.T) {
	srv, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()

	ctx := context.Background()
	if _, err := srv.CreateCollection(ctx, &pb.CreateCollectionRequest{
		Name:      "docs",
		Dimension: 2,
		Metric:    pb.Metric_L2,
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := srv.Insert(ctx, &pb.InsertRequest{
		Collection: "docs",
		Key:        "a",
		Embedding:  []float32{0, 0},
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	resp, err := srv.Search(ctx, &pb.SearchRequest{
		Collection: "docs",
		Embedding:  []float32{0, 0},
		TopK:       1,
	})
	if err != nil {
		t.Fatalf("search without embedding flag: %v", err)
	}
	if len(resp.Results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(resp.Results))
	}
	if got := extractSearchResultEmbedding(resp.Results[0]); len(got) != 0 {
		t.Fatalf("did not expect embedding by default, got=%v", got)
	}

	withEmbeddingCtx := metadata.NewIncomingContext(ctx, metadata.Pairs("x-return-embedding", "true"))
	resp, err = srv.Search(withEmbeddingCtx, &pb.SearchRequest{
		Collection: "docs",
		Embedding:  []float32{0, 0},
		TopK:       1,
	})
	if err != nil {
		t.Fatalf("search with metadata embedding flag: %v", err)
	}
	got := extractSearchResultEmbedding(resp.Results[0])
	if !reflect.DeepEqual(got, []float32{0, 0}) {
		t.Fatalf("unexpected embedding from metadata flag: got=%v", got)
	}

	req := &pb.SearchRequest{
		Collection: "docs",
		Embedding:  []float32{0, 0},
		TopK:       1,
	}
	unknown := protowire.AppendTag(nil, 5, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, 1)
	req.ProtoReflect().SetUnknown(unknown)

	resp, err = srv.Search(ctx, req)
	if err != nil {
		t.Fatalf("search with proto unknown embedding flag: %v", err)
	}
	got = extractSearchResultEmbedding(resp.Results[0])
	if !reflect.DeepEqual(got, []float32{0, 0}) {
		t.Fatalf("unexpected embedding from proto field flag: got=%v", got)
	}
}

func extractSearchResultEmbedding(result *pb.SearchResult) []float32 {
	if result == nil {
		return nil
	}

	raw := result.ProtoReflect().GetUnknown()
	for len(raw) > 0 {
		fieldNum, wireType, n := protowire.ConsumeTag(raw)
		if n < 0 {
			return nil
		}
		raw = raw[n:]

		if fieldNum == 3 && wireType == protowire.BytesType {
			payload, m := protowire.ConsumeBytes(raw)
			if m < 0 {
				return nil
			}
			if len(payload)%4 != 0 {
				return nil
			}
			out := make([]float32, len(payload)/4)
			for i := 0; i < len(out); i++ {
				bits := binary.LittleEndian.Uint32(payload[i*4 : i*4+4])
				out[i] = math.Float32frombits(bits)
			}
			return out
		}

		m := protowire.ConsumeFieldValue(fieldNum, wireType, raw)
		if m < 0 {
			return nil
		}
		raw = raw[m:]
	}
	return nil
}

func TestServerDistributedSearchMergeTopK(t *testing.T) {
	srv, err := NewWithConfig(t.TempDir(), 4, "none")
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()

	ctx := context.Background()
	if _, err := srv.CreateCollection(ctx, &pb.CreateCollectionRequest{
		Name:      "docs",
		Dimension: 2,
		Metric:    pb.Metric_L2,
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}

	inputs := map[string][]float32{
		"v0":  {0, 0},
		"v1":  {1, 1},
		"v2":  {2, 2},
		"v10": {10, 10},
	}
	for key, vec := range inputs {
		if _, err := srv.Insert(ctx, &pb.InsertRequest{
			Collection: "docs",
			Key:        key,
			Embedding:  vec,
		}); err != nil {
			t.Fatalf("insert %s: %v", key, err)
		}
	}

	if _, err := srv.CreateIndex(ctx, &pb.CreateIndexRequest{Collection: "docs"}); err != nil {
		t.Fatalf("create index: %v", err)
	}

	resp, err := srv.Search(ctx, &pb.SearchRequest{
		Collection: "docs",
		Embedding:  []float32{1.1, 1.1},
		TopK:       2,
		EfSearch:   32,
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(resp.Results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(resp.Results))
	}

	got := []string{resp.Results[0].Key, resp.Results[1].Key}
	ok := (got[0] == "v1" && got[1] == "v2") || (got[0] == "v2" && got[1] == "v1")
	if !ok {
		t.Fatalf("unexpected distributed top-k merge result: %+v", got)
	}
}

func TestServerBatchInsertIndexStaysConsistentOnOverwrite(t *testing.T) {
	srv, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()

	ctx := context.Background()
	if _, err := srv.CreateCollection(ctx, &pb.CreateCollectionRequest{
		Name:      "docs",
		Dimension: 2,
		Metric:    pb.Metric_L2,
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := srv.CreateIndex(ctx, &pb.CreateIndexRequest{Collection: "docs"}); err != nil {
		t.Fatalf("create index: %v", err)
	}

	if _, err := srv.BatchInsert(ctx, &pb.BatchInsertRequest{
		Collection: "docs",
		Items: []*pb.BatchInsertItem{
			{Key: "a", Embedding: []float32{0, 0}},
			{Key: "b", Embedding: []float32{5, 5}},
			{Key: "a", Embedding: []float32{10, 10}}, // overwrite "a"
		},
	}); err != nil {
		t.Fatalf("batch insert: %v", err)
	}

	resp, err := srv.Search(ctx, &pb.SearchRequest{
		Collection: "docs",
		Embedding:  []float32{0, 0},
		TopK:       1,
	})
	if err != nil {
		t.Fatalf("search after batch overwrite: %v", err)
	}
	if len(resp.Results) != 1 || resp.Results[0].Key != "b" {
		t.Fatalf("expected b as nearest after batch overwrite, got: %+v", resp.Results)
	}
}

func TestServerPersistsIndexConfigAcrossRestart(t *testing.T) {
	dataDir := t.TempDir()
	ctx := context.Background()

	srv, err := New(dataDir)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	if _, err := srv.CreateCollection(ctx, &pb.CreateCollectionRequest{
		Name:      "docs",
		Dimension: 2,
		Metric:    pb.Metric_L2,
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := srv.CreateIndex(ctx, &pb.CreateIndexRequest{
		Collection:     "docs",
		M:              24,
		EfConstruction: 90,
	}); err != nil {
		t.Fatalf("create index: %v", err)
	}

	srv.mu.RLock()
	col := srv.collections["docs"]
	cfg := col.indexConfig
	srv.mu.RUnlock()
	if cfg.M != 24 || cfg.EfConstruction != 90 {
		t.Fatalf("unexpected in-memory index config before restart: %+v", cfg)
	}
	srv.Close()

	srv, err = New(dataDir)
	if err != nil {
		t.Fatalf("reopen server: %v", err)
	}
	defer srv.Close()

	listResp, err := srv.ListCollections(ctx, &pb.ListCollectionsRequest{})
	if err != nil {
		t.Fatalf("list collections: %v", err)
	}
	if len(listResp.Collections) != 1 || !listResp.Collections[0].HasIndex {
		t.Fatalf("expected collection index enabled after restart, got: %+v", listResp.Collections)
	}

	srv.mu.RLock()
	col = srv.collections["docs"]
	cfg = col.indexConfig
	srv.mu.RUnlock()
	if cfg.M != 24 || cfg.EfConstruction != 90 {
		t.Fatalf("unexpected index config after restart: %+v", cfg)
	}

	if _, err := srv.DropIndex(ctx, &pb.DropIndexRequest{Collection: "docs"}); err != nil {
		t.Fatalf("drop index: %v", err)
	}
	srv.Close()

	srv, err = New(dataDir)
	if err != nil {
		t.Fatalf("reopen server after drop: %v", err)
	}
	defer srv.Close()

	listResp, err = srv.ListCollections(ctx, &pb.ListCollectionsRequest{})
	if err != nil {
		t.Fatalf("list collections after drop: %v", err)
	}
	if len(listResp.Collections) != 1 || listResp.Collections[0].HasIndex {
		t.Fatalf("expected collection index disabled after restart, got: %+v", listResp.Collections)
	}
}

func TestQueueFullUnderLoadDoesNotDeadlock(t *testing.T) {
	restoreRuntime := withRuntimeConfig(t, RuntimeConfig{})
	defer restoreRuntime()

	srv, err := NewWithConfig(t.TempDir(), 1, "none")
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()

	ctx := context.Background()
	if _, err := srv.CreateCollection(ctx, &pb.CreateCollectionRequest{
		Name:      "docs",
		Dimension: 8,
		Metric:    pb.Metric_L2,
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := srv.CreateIndex(ctx, &pb.CreateIndexRequest{Collection: "docs"}); err != nil {
		t.Fatalf("create index: %v", err)
	}

	const inserts = 200
	var successCount atomic.Int64
	errCh := make(chan error, inserts)
	var wg sync.WaitGroup
	for i := 0; i < inserts; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			insertCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			_, err := srv.Insert(insertCtx, &pb.InsertRequest{
				Collection: "docs",
				Key:        fmt.Sprintf("k-%03d", i),
				Embedding:  []float32{float32(i), 1, 2, 3, 4, 5, 6, 7},
			})
			if err == nil {
				successCount.Add(1)
				return
			}
			code := status.Code(err)
			if code != codes.ResourceExhausted && code != codes.DeadlineExceeded && code != codes.Canceled {
				errCh <- err
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for inserts to complete; possible deadlock")
	}

	select {
	case err := <-errCh:
		t.Fatalf("unexpected insert error under queue pressure: %v", err)
	default:
	}

	if successCount.Load() == 0 {
		t.Fatal("expected at least one successful insert under load")
	}
}

func TestCloseCompletesUnderHeavyIngestAndIndexing(t *testing.T) {
	restoreRuntime := withRuntimeConfig(t, RuntimeConfig{})
	defer restoreRuntime()

	srv, err := NewWithConfig(t.TempDir(), 1, "none")
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	ctx := context.Background()
	if _, err := srv.CreateCollection(ctx, &pb.CreateCollectionRequest{
		Name:      "docs",
		Dimension: 8,
		Metric:    pb.Metric_L2,
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := srv.CreateIndex(ctx, &pb.CreateIndexRequest{Collection: "docs"}); err != nil {
		t.Fatalf("create index: %v", err)
	}

	stopProducers := make(chan struct{})
	var producerWG sync.WaitGroup
	for g := 0; g < 4; g++ {
		g := g
		producerWG.Add(1)
		go func() {
			defer producerWG.Done()
			i := 0
			for {
				select {
				case <-stopProducers:
					return
				default:
				}

				insertCtx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
				_, _ = srv.Insert(insertCtx, &pb.InsertRequest{
					Collection: "docs",
					Key:        fmt.Sprintf("p-%d-%d", g, i),
					Embedding:  []float32{float32(i), 2, 3, 4, 5, 6, 7, 8},
				})
				cancel()
				i++
			}
		}()
	}

	time.Sleep(400 * time.Millisecond)
	close(stopProducers)

	closeDone := make(chan struct{})
	go func() {
		srv.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
	case <-time.After(10 * time.Second):
		t.Fatal("server close timed out under heavy ingest/indexing")
	}

	producerWG.Wait()
}

func withRuntimeConfig(t *testing.T, cfg RuntimeConfig) func() {
	t.Helper()
	prev := DefaultRuntimeConfig()
	merged := normalizeRuntimeConfig(cfg)

	SetDefaultRuntimeConfig(merged)
	return func() {
		SetDefaultRuntimeConfig(prev)
	}
}
