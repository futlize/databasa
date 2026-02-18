package hnsw

import (
	"fmt"
	"math"
	"testing"
)

func TestIndexInsertAndSearch(t *testing.T) {
	vectors := map[uint64][]float32{
		0: {0, 0},
		1: {1, 1},
		2: {10, 10},
		3: {-5, -5},
	}

	getVector := func(id uint64) ([]float32, error) {
		v, ok := vectors[id]
		if !ok {
			return nil, fmt.Errorf("missing id %d", id)
		}
		return v, nil
	}

	cfg := DefaultConfig()
	cfg.EfConstruction = 64
	cfg.EfSearch = 32

	idx := New(cfg, L2Distance, getVector)
	for _, id := range []uint64{0, 1, 2, 3} {
		if err := idx.Insert(id); err != nil {
			t.Fatalf("insert id %d: %v", id, err)
		}
	}

	results, err := idx.Search([]float32{0.9, 1.1}, 2, 0)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) == 0 {
		t.Fatalf("expected at least one result")
	}
	if results[0].ID != 1 {
		t.Fatalf("unexpected nearest id: got=%d want=1", results[0].ID)
	}
}

func TestIndexRemove(t *testing.T) {
	vectors := map[uint64][]float32{
		0: {0, 0},
		1: {1, 1},
	}
	getVector := func(id uint64) ([]float32, error) {
		v, ok := vectors[id]
		if !ok {
			return nil, fmt.Errorf("missing id %d", id)
		}
		return v, nil
	}

	idx := New(DefaultConfig(), L2Distance, getVector)
	if err := idx.Insert(0); err != nil {
		t.Fatalf("insert 0: %v", err)
	}
	if err := idx.Insert(1); err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	idx.Remove(1)

	results, err := idx.Search([]float32{1, 1}, 1, 0)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 || results[0].ID != 0 {
		t.Fatalf("unexpected results after remove: %+v", results)
	}
}

func TestIndexSaveLoad(t *testing.T) {
	vectors := map[uint64][]float32{
		0: {0, 0},
		1: {2, 2},
		2: {4, 4},
	}
	getVector := func(id uint64) ([]float32, error) {
		v, ok := vectors[id]
		if !ok {
			return nil, fmt.Errorf("missing id %d", id)
		}
		return v, nil
	}

	idx := New(DefaultConfig(), L2Distance, getVector)
	for _, id := range []uint64{0, 1, 2} {
		if err := idx.Insert(id); err != nil {
			t.Fatalf("insert id %d: %v", id, err)
		}
	}

	dir := t.TempDir()
	if err := idx.Save(dir); err != nil {
		t.Fatalf("save index: %v", err)
	}

	loaded, err := Load(dir, L2Distance, getVector)
	if err != nil {
		t.Fatalf("load index: %v", err)
	}

	results, err := loaded.Search([]float32{2.1, 1.9}, 1, 0)
	if err != nil {
		t.Fatalf("search loaded index: %v", err)
	}
	if len(results) != 1 || results[0].ID != 1 {
		t.Fatalf("unexpected loaded search result: %+v", results)
	}
}

func TestPreparedQueryDistanceProducesSameSearch(t *testing.T) {
	vectors := map[uint64][]float32{
		0: {1, 0, 0},
		1: {0.9, 0.1, 0},
		2: {0, 1, 0},
		3: {0, 0, 1},
	}
	getVector := func(id uint64) ([]float32, error) {
		v, ok := vectors[id]
		if !ok {
			return nil, fmt.Errorf("missing id %d", id)
		}
		return v, nil
	}

	cfg := DefaultConfig()
	cfg.EfConstruction = 64
	cfg.EfSearch = 32

	base := New(cfg, CosineDistance, getVector)
	prepared := New(cfg, CosineDistance, getVector)
	prepared.SetQueryDistanceFactory(PrepareCosineDistance)

	for _, id := range []uint64{0, 1, 2, 3} {
		if err := base.Insert(id); err != nil {
			t.Fatalf("base insert id %d: %v", id, err)
		}
		if err := prepared.Insert(id); err != nil {
			t.Fatalf("prepared insert id %d: %v", id, err)
		}
	}

	query := []float32{0.95, 0.05, 0}
	want, err := base.Search(query, 3, 24)
	if err != nil {
		t.Fatalf("base search: %v", err)
	}
	got, err := prepared.Search(query, 3, 24)
	if err != nil {
		t.Fatalf("prepared search: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("result length mismatch: got=%d want=%d", len(got), len(want))
	}
	for i := range want {
		if got[i].ID != want[i].ID {
			t.Fatalf("result[%d] id mismatch: got=%d want=%d", i, got[i].ID, want[i].ID)
		}
		if math.Abs(float64(got[i].Distance-want[i].Distance)) > 1e-6 {
			t.Fatalf("result[%d] distance mismatch: got=%f want=%f", i, got[i].Distance, want[i].Distance)
		}
	}
}
