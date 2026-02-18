package server

import (
	"context"
	"encoding/binary"
	"math"
	"reflect"
	"testing"

	pb "github.com/juniodev/kekdb/pkg/pb"
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
	srv, err := NewWithConfig(t.TempDir(), 4, 1, "none")
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

func TestNewWithConfigIndexFlushOpsSupportsZero(t *testing.T) {
	srvDisabled, err := NewWithConfig(t.TempDir(), 2, 0, "none")
	if err != nil {
		t.Fatalf("new server with flush disabled: %v", err)
	}
	defer srvDisabled.Close()
	if srvDisabled.indexFlushOps != 0 {
		t.Fatalf("expected index flush ops to stay 0, got %d", srvDisabled.indexFlushOps)
	}

	srvDefault, err := NewWithConfig(t.TempDir(), 2, -1, "none")
	if err != nil {
		t.Fatalf("new server with default flush: %v", err)
	}
	defer srvDefault.Close()
	if srvDefault.indexFlushOps != defaultIndexFlushOps {
		t.Fatalf("expected default index flush ops %d, got %d", defaultIndexFlushOps, srvDefault.indexFlushOps)
	}
}
