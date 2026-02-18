package storage

import (
	"math"
	"reflect"
	"testing"
)

func normalizedCopy(v []float32) []float32 {
	if len(v) == 0 {
		return nil
	}
	out := make([]float32, len(v))
	copy(out, v)

	var normSq float64
	for _, x := range out {
		normSq += float64(x) * float64(x)
	}
	if normSq == 0 {
		return out
	}

	invNorm := float32(1.0 / math.Sqrt(normSq))
	for i := range out {
		out[i] *= invNorm
	}
	return out
}

func assertVectorClose(t *testing.T, got, want []float32, tol float64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("vector length mismatch: got=%d want=%d", len(got), len(want))
	}
	for i := range got {
		if math.Abs(float64(got[i]-want[i])) > tol {
			t.Fatalf("vector mismatch at dim %d: got=%f want=%f", i, got[i], want[i])
		}
	}
}

func TestVectorStorePersistAndAppendAfterReopen(t *testing.T) {
	dir := t.TempDir()

	store, err := Open(dir, "docs", 3, "cosine")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	v1 := []float32{1, 2, 3}
	v2 := []float32{4, 5, 6}
	id1, err := store.Insert("a", v1)
	if err != nil {
		t.Fatalf("insert a: %v", err)
	}
	id2, err := store.Insert("b", v2)
	if err != nil {
		t.Fatalf("insert b: %v", err)
	}
	if id1 != 0 || id2 != 1 {
		t.Fatalf("unexpected ids: got %d and %d", id1, id2)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	store, err = Open(dir, "docs", 3, "cosine")
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer store.Close()

	gotV1, err := store.GetByID(id1)
	if err != nil {
		t.Fatalf("get by id v1: %v", err)
	}
	assertVectorClose(t, gotV1, normalizedCopy(v1), 1e-6)

	gotV2, err := store.GetByKey("b")
	if err != nil {
		t.Fatalf("get by key b: %v", err)
	}
	assertVectorClose(t, gotV2, normalizedCopy(v2), 1e-6)

	v3 := []float32{7, 8, 9}
	id3, err := store.Insert("c", v3)
	if err != nil {
		t.Fatalf("insert c: %v", err)
	}
	if id3 != 2 {
		t.Fatalf("unexpected id after reopen append: got=%d want=2", id3)
	}

	gotV1Again, err := store.GetByID(id1)
	if err != nil {
		t.Fatalf("get by id v1 after append: %v", err)
	}
	assertVectorClose(t, gotV1Again, normalizedCopy(v1), 1e-6)
}

func TestVectorStoreDeleteAndTombstone(t *testing.T) {
	dir := t.TempDir()

	store, err := Open(dir, "docs", 2, "l2")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	id, err := store.Insert("dead", []float32{1, 1})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := store.Delete("dead"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !store.IsDeleted(id) {
		t.Fatalf("expected tombstone for id=%d", id)
	}
	if store.Count() != 0 {
		t.Fatalf("expected count 0, got %d", store.Count())
	}
}

func TestVectorStoreValidatesDimensionAndMetric(t *testing.T) {
	dir := t.TempDir()

	store, err := Open(dir, "docs", 3, "cosine")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := store.Insert("bad", []float32{1, 2}); err == nil {
		t.Fatalf("expected dimension mismatch error")
	}
	if _, err := store.Insert("ok", []float32{1, 2, 3}); err != nil {
		t.Fatalf("insert valid vector: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	if reopened, err := Open(dir, "docs", 4, "cosine"); err == nil {
		_ = reopened.Close()
		t.Fatalf("expected dimension mismatch on reopen")
	}
	if reopened, err := Open(dir, "docs", 3, "dot_product"); err == nil {
		_ = reopened.Close()
		t.Fatalf("expected metric mismatch on reopen")
	}
}

func TestVectorStoreInt8Compression(t *testing.T) {
	dir := t.TempDir()

	store, err := OpenWithCompression(dir, "docs", 4, "cosine", "int8")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	original := []float32{-1.25, 0.05, 0.88, 3.42}
	if _, err := store.Insert("q", original); err != nil {
		t.Fatalf("insert compressed vector: %v", err)
	}

	got, err := store.GetByKey("q")
	if err != nil {
		t.Fatalf("get compressed vector: %v", err)
	}
	expected := normalizedCopy(original)
	if len(got) != len(expected) {
		t.Fatalf("unexpected vector length: got=%d want=%d", len(got), len(expected))
	}
	for i := range expected {
		if math.Abs(float64(got[i]-expected[i])) > 0.03 {
			t.Fatalf("dimension %d drift too high: got=%f want=%f", i, got[i], expected[i])
		}
	}
}

func TestVectorStoreCosineSearchUsesDotOnNormalizedVectors(t *testing.T) {
	store, err := Open(t.TempDir(), "docs", 2, "cosine")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	if _, err := store.Insert("a", []float32{3, 4}); err != nil {
		t.Fatalf("insert a: %v", err)
	}
	if _, err := store.Insert("b", []float32{4, 3}); err != nil {
		t.Fatalf("insert b: %v", err)
	}

	hits, err := store.SearchBruteForceTopK([]float32{6, 8}, 2)
	if err != nil {
		t.Fatalf("search topk: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("unexpected hit count: got=%d want=2", len(hits))
	}
	if hits[0].Key != "a" {
		t.Fatalf("unexpected nearest key: got=%s want=a", hits[0].Key)
	}

	if math.Abs(float64(hits[0].Distance-(-1.0))) > 1e-5 {
		t.Fatalf("unexpected cosine distance for a: got=%f want=-1.0", hits[0].Distance)
	}
	if math.Abs(float64(hits[1].Distance-(-0.96))) > 1e-4 {
		t.Fatalf("unexpected cosine distance for b: got=%f want=-0.96", hits[1].Distance)
	}
}

func TestVectorStoreSearchBruteForceTopK(t *testing.T) {
	tests := []struct {
		name        string
		compression string
		metric      string
		query       []float32
		wantKeys    []string
	}{
		{
			name:        "none-l2",
			compression: "none",
			metric:      "l2",
			query:       []float32{1, 1},
			wantKeys:    []string{"a", "b"},
		},
		{
			name:        "int8-cosine",
			compression: "int8",
			metric:      "cosine",
			query:       []float32{1, 0, 0, 0},
			wantKeys:    []string{"a", "b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := OpenWithCompression(t.TempDir(), "docs", uint32(len(tt.query)), tt.metric, tt.compression)
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			defer store.Close()

			vectors := map[string][]float32{
				"a": make([]float32, len(tt.query)),
				"b": make([]float32, len(tt.query)),
				"c": make([]float32, len(tt.query)),
			}
			copy(vectors["a"], tt.query)
			for i := range vectors["b"] {
				if i == 0 {
					vectors["b"][i] = tt.query[i] * 0.8
				} else {
					vectors["b"][i] = 0.1
				}
				vectors["c"][i] = 9
			}

			if _, err := store.Insert("a", vectors["a"]); err != nil {
				t.Fatalf("insert a: %v", err)
			}
			if _, err := store.Insert("b", vectors["b"]); err != nil {
				t.Fatalf("insert b: %v", err)
			}
			if _, err := store.Insert("c", vectors["c"]); err != nil {
				t.Fatalf("insert c: %v", err)
			}

			hits, err := store.SearchBruteForceTopK(tt.query, 2)
			if err != nil {
				t.Fatalf("search topk: %v", err)
			}
			if len(hits) != 2 {
				t.Fatalf("unexpected hit count: got=%d want=2", len(hits))
			}
			gotKeys := []string{hits[0].Key, hits[1].Key}
			if !reflect.DeepEqual(gotKeys, tt.wantKeys) {
				t.Fatalf("unexpected top-k keys: got=%v want=%v", gotKeys, tt.wantKeys)
			}
			if hits[0].Distance > hits[1].Distance {
				t.Fatalf("results not sorted by ascending distance: %+v", hits)
			}
		})
	}
}

func TestVectorStoreResolveActiveKeys(t *testing.T) {
	store, err := Open(t.TempDir(), "docs", 2, "l2")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	idA, err := store.Insert("a", []float32{0, 0})
	if err != nil {
		t.Fatalf("insert a: %v", err)
	}
	idB, err := store.Insert("b", []float32{1, 1})
	if err != nil {
		t.Fatalf("insert b: %v", err)
	}
	if err := store.Delete("b"); err != nil {
		t.Fatalf("delete b: %v", err)
	}

	resolved := store.ResolveActiveKeys([]uint64{idA, idB, 999})
	if len(resolved) != 1 {
		t.Fatalf("unexpected resolved size: got=%d want=1", len(resolved))
	}
	if got := resolved[idA]; got != "a" {
		t.Fatalf("unexpected key for idA: got=%q", got)
	}
	if _, ok := resolved[idB]; ok {
		t.Fatalf("did not expect deleted idB to be resolved")
	}
}

func TestVectorStoreBatchInsertOverwriteAndReopen(t *testing.T) {
	dir := t.TempDir()

	store, err := Open(dir, "docs", 2, "l2")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	results, err := store.BatchInsert([]BatchInsertItem{
		{Key: "a", Embedding: []float32{0, 0}},
		{Key: "b", Embedding: []float32{1, 1}},
		{Key: "a", Embedding: []float32{2, 2}},
	})
	if err != nil {
		t.Fatalf("batch insert: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("unexpected result size: got=%d want=3", len(results))
	}
	if results[0].ID != 0 || results[0].HadOld {
		t.Fatalf("unexpected first result: %+v", results[0])
	}
	if results[1].ID != 1 || results[1].HadOld {
		t.Fatalf("unexpected second result: %+v", results[1])
	}
	if results[2].ID != 2 || !results[2].HadOld || results[2].OldID != 0 {
		t.Fatalf("unexpected overwrite result: %+v", results[2])
	}
	if store.Count() != 2 {
		t.Fatalf("unexpected count after batch insert: got=%d want=2", store.Count())
	}

	gotA, err := store.GetByKey("a")
	if err != nil {
		t.Fatalf("get a: %v", err)
	}
	if !reflect.DeepEqual(gotA, []float32{2, 2}) {
		t.Fatalf("unexpected final value for a: got=%v", gotA)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := Open(dir, "docs", 2, "l2")
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopened.Close()

	if reopened.Count() != 2 {
		t.Fatalf("unexpected count after reopen: got=%d want=2", reopened.Count())
	}
	gotB, err := reopened.GetByKey("b")
	if err != nil {
		t.Fatalf("get b after reopen: %v", err)
	}
	if !reflect.DeepEqual(gotB, []float32{1, 1}) {
		t.Fatalf("unexpected value for b after reopen: got=%v", gotB)
	}
}
