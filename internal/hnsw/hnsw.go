package hnsw

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Config holds HNSW hyperparameters.
type Config struct {
	M              int `json:"m"`               // Max connections per node per layer
	MMax0          int `json:"m_max0"`          // Max connections at layer 0 (usually 2*M)
	EfConstruction int `json:"ef_construction"` // Beam width during construction
	EfSearch       int `json:"ef_search"`       // Default beam width during search
}

// DefaultConfig returns sensible defaults for HNSW.
func DefaultConfig() Config {
	return Config{
		M:              16,
		MMax0:          32,
		EfConstruction: 200,
		EfSearch:       50,
	}
}

// node represents a single vector in the HNSW graph.
type node struct {
	ID         uint64
	Layer      int
	Neighbours [][]uint64 // neighbours[layer] = list of neighbour IDs
}

// Index is a Hierarchical Navigable Small World graph for ANN search.
type Index struct {
	mu           sync.RWMutex
	config       Config
	distFunc     DistanceFunc
	queryFactory QueryDistanceFactory
	getVector    func(id uint64) ([]float32, error) // callback to retrieve vectors
	nodes        map[uint64]*node
	entryPointID uint64
	maxLayer     int
	rng          *rand.Rand
}

// VectorGetter is a function that retrieves a vector by its internal ID.
type VectorGetter func(id uint64) ([]float32, error)

// New creates a new HNSW index.
func New(cfg Config, distFunc DistanceFunc, getVector VectorGetter) *Index {
	if cfg.M == 0 {
		cfg = DefaultConfig()
	}
	if cfg.MMax0 == 0 {
		cfg.MMax0 = cfg.M * 2
	}
	return &Index{
		config:       cfg,
		distFunc:     distFunc,
		queryFactory: defaultQueryDistanceFactory(distFunc),
		getVector:    getVector,
		nodes:        make(map[uint64]*node),
		maxLayer:     -1,
		rng:          rand.New(rand.NewSource(42)),
	}
}

// SetQueryDistanceFactory installs a query-specialized distance evaluator factory.
func (idx *Index) SetQueryDistanceFactory(factory QueryDistanceFactory) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if factory == nil {
		factory = defaultQueryDistanceFactory(idx.distFunc)
	}
	idx.queryFactory = factory
}

// Config returns a copy of the current HNSW settings.
func (idx *Index) Config() Config {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.config
}

func defaultQueryDistanceFactory(dist DistanceFunc) QueryDistanceFactory {
	return func(query []float32) QueryDistanceFunc {
		return func(candidate []float32) float32 {
			return dist(query, candidate)
		}
	}
}

func (idx *Index) prepareQueryDistance(query []float32) QueryDistanceFunc {
	if idx.queryFactory == nil {
		return defaultQueryDistanceFactory(idx.distFunc)(query)
	}
	return idx.queryFactory(query)
}

// Len returns the number of nodes in the index.
func (idx *Index) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.nodes)
}

// ─── Insert ──────────────────────────────────────────────

// Insert adds a vector (by its storage ID) to the HNSW graph.
func (idx *Index) Insert(id uint64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Insert takes an exclusive lock for the full graph mutation.
	// When callers invoke this in the foreground of each write request, index-enabled
	// ingest becomes effectively serialized per index instance.
	if _, exists := idx.nodes[id]; exists {
		return nil // already in the graph
	}

	// Determine random layer for this node
	level := idx.randomLevel()

	n := &node{
		ID:         id,
		Layer:      level,
		Neighbours: make([][]uint64, level+1),
	}
	for i := range n.Neighbours {
		n.Neighbours[i] = make([]uint64, 0)
	}
	idx.nodes[id] = n

	// First node — set as entry point
	if idx.maxLayer == -1 {
		idx.entryPointID = id
		idx.maxLayer = level
		return nil
	}

	queryVec, err := idx.getVector(id)
	if err != nil {
		return fmt.Errorf("get vector for insert: %w", err)
	}
	queryDist := idx.prepareQueryDistance(queryVec)

	ep := idx.entryPointID

	// Phase 1: Traverse from top layer to level+1 (greedy search)
	for lc := idx.maxLayer; lc > level; lc-- {
		ep = idx.greedyClosest(queryDist, ep, lc)
	}

	// Phase 2: Insert at each layer from min(level, maxLayer) down to 0
	topInsertLayer := level
	if topInsertLayer > idx.maxLayer {
		topInsertLayer = idx.maxLayer
	}

	for lc := topInsertLayer; lc >= 0; lc-- {
		candidates := idx.searchLayer(queryDist, ep, idx.config.EfConstruction, lc)

		// Select M best neighbours
		maxConn := idx.config.M
		if lc == 0 {
			maxConn = idx.config.MMax0
		}
		neighbours := idx.selectNeighboursSimple(candidates, maxConn)

		// Connect new node to neighbours
		n.Neighbours[lc] = neighbours

		// Add reverse connections
		for _, nbrID := range neighbours {
			nbr := idx.nodes[nbrID]
			if nbr == nil || lc >= len(nbr.Neighbours) {
				continue
			}
			nbr.Neighbours[lc] = append(nbr.Neighbours[lc], id)

			// Prune if too many connections
			maxC := idx.config.M
			if lc == 0 {
				maxC = idx.config.MMax0
			}
			if len(nbr.Neighbours[lc]) > maxC {
				// Pruning adds extra per-insert work proportional to local graph density.
				// Under synchronous maintenance this contributes significant CPU cost.
				nbr.Neighbours[lc] = idx.pruneConnections(queryVec, nbr.Neighbours[lc], maxC, nbrID)
			}
		}

		if len(candidates) > 0 {
			ep = candidates[0].id
		}
	}

	// Update entry point if this node has a higher layer
	if level > idx.maxLayer {
		idx.entryPointID = id
		idx.maxLayer = level
	}

	return nil
}

// ─── Search ──────────────────────────────────────────────

// Search returns the top-K nearest neighbours to the query vector.
func (idx *Index) Search(query []float32, k int, efSearch int) ([]SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.nodes) == 0 {
		return nil, nil
	}

	if efSearch == 0 {
		efSearch = idx.config.EfSearch
	}
	if efSearch < k {
		efSearch = k
	}

	queryDist := idx.prepareQueryDistance(query)
	ep := idx.entryPointID

	// Phase 1: Greedy descend from top to layer 1
	for lc := idx.maxLayer; lc > 0; lc-- {
		ep = idx.greedyClosest(queryDist, ep, lc)
	}

	// Phase 2: Search layer 0 with beam search
	candidates := idx.searchLayer(queryDist, ep, efSearch, 0)

	// Return top-K
	if len(candidates) > k {
		candidates = candidates[:k]
	}

	results := make([]SearchResult, len(candidates))
	for i, c := range candidates {
		results[i] = SearchResult{ID: c.id, Distance: c.dist}
	}
	return results, nil
}

// SearchResult holds one result from a nearest neighbour search.
type SearchResult struct {
	ID       uint64
	Distance float32
}

// ─── Delete ──────────────────────────────────────────────

// Remove marks a node as deleted. The node is disconnected from the graph.
func (idx *Index) Remove(id uint64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	n, exists := idx.nodes[id]
	if !exists {
		return
	}

	// Remove references to this node from all neighbours
	for lc := 0; lc < len(n.Neighbours); lc++ {
		for _, nbrID := range n.Neighbours[lc] {
			nbr := idx.nodes[nbrID]
			if nbr == nil || lc >= len(nbr.Neighbours) {
				continue
			}
			filtered := make([]uint64, 0, len(nbr.Neighbours[lc]))
			for _, connID := range nbr.Neighbours[lc] {
				if connID != id {
					filtered = append(filtered, connID)
				}
			}
			nbr.Neighbours[lc] = filtered
		}
	}

	delete(idx.nodes, id)

	// If we deleted the entry point, pick a new one
	if id == idx.entryPointID {
		idx.maxLayer = -1
		for _, nn := range idx.nodes {
			if nn.Layer > idx.maxLayer {
				idx.maxLayer = nn.Layer
				idx.entryPointID = nn.ID
			}
		}
	}
}

// ─── Persistence ─────────────────────────────────────────

const hnswPersistVersion uint16 = 1

var hnswMagic = [8]byte{'K', 'H', 'N', 'S', 'W', 'B', 'I', 'N'}

// Save persists the HNSW graph to disk.
func (idx *Index) Save(dir string) error {
	type persistedNode struct {
		id         uint64
		layer      int32
		neighbours [][]uint64
	}

	idx.mu.RLock()
	cfg := idx.config
	entryPointID := idx.entryPointID
	maxLayer := idx.maxLayer
	nodes := make([]persistedNode, 0, len(idx.nodes))
	for _, n := range idx.nodes {
		neighbours := make([][]uint64, len(n.Neighbours))
		for level, nbrs := range n.Neighbours {
			cp := make([]uint64, len(nbrs))
			copy(cp, nbrs)
			neighbours[level] = cp
		}
		nodes = append(nodes, persistedNode{
			id:         n.ID,
			layer:      int32(n.Layer),
			neighbours: neighbours,
		})
	}
	idx.mu.RUnlock()

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].id < nodes[j].id
	})

	graphPath := filepath.Join(dir, "hnsw.graph")
	tmpPath := graphPath + ".tmp"

	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create hnsw tmp: %w", err)
	}
	defer f.Close()

	if _, err := f.Write(hnswMagic[:]); err != nil {
		return fmt.Errorf("write hnsw magic: %w", err)
	}
	if err := binary.Write(f, binary.LittleEndian, hnswPersistVersion); err != nil {
		return fmt.Errorf("write hnsw version: %w", err)
	}
	if err := binary.Write(f, binary.LittleEndian, int32(cfg.M)); err != nil {
		return fmt.Errorf("write config m: %w", err)
	}
	if err := binary.Write(f, binary.LittleEndian, int32(cfg.MMax0)); err != nil {
		return fmt.Errorf("write config mmax0: %w", err)
	}
	if err := binary.Write(f, binary.LittleEndian, int32(cfg.EfConstruction)); err != nil {
		return fmt.Errorf("write config efConstruction: %w", err)
	}
	if err := binary.Write(f, binary.LittleEndian, int32(cfg.EfSearch)); err != nil {
		return fmt.Errorf("write config efSearch: %w", err)
	}
	if err := binary.Write(f, binary.LittleEndian, entryPointID); err != nil {
		return fmt.Errorf("write entry point: %w", err)
	}
	if err := binary.Write(f, binary.LittleEndian, int32(maxLayer)); err != nil {
		return fmt.Errorf("write max layer: %w", err)
	}

	if err := binary.Write(f, binary.LittleEndian, uint64(len(nodes))); err != nil {
		return fmt.Errorf("write node count: %w", err)
	}

	for _, n := range nodes {
		if err := binary.Write(f, binary.LittleEndian, n.id); err != nil {
			return fmt.Errorf("write node id: %w", err)
		}
		if err := binary.Write(f, binary.LittleEndian, n.layer); err != nil {
			return fmt.Errorf("write node layer: %w", err)
		}
		if err := binary.Write(f, binary.LittleEndian, uint32(len(n.neighbours))); err != nil {
			return fmt.Errorf("write node level count: %w", err)
		}
		for _, neighbours := range n.neighbours {
			if err := binary.Write(f, binary.LittleEndian, uint32(len(neighbours))); err != nil {
				return fmt.Errorf("write node degree: %w", err)
			}
			for _, nbrID := range neighbours {
				if err := binary.Write(f, binary.LittleEndian, nbrID); err != nil {
					return fmt.Errorf("write neighbour id: %w", err)
				}
			}
		}
	}

	if err := f.Sync(); err != nil {
		// Durability here is correct, but when Save is triggered from foreground writes
		// this fsync turns into latency spikes and visible throughput drops.
		return fmt.Errorf("sync hnsw file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close hnsw file: %w", err)
	}
	if err := os.Rename(tmpPath, graphPath); err != nil {
		return fmt.Errorf("commit hnsw file: %w", err)
	}
	return nil
}

// Load restores an HNSW graph from disk.
func Load(dir string, distFunc DistanceFunc, getVector VectorGetter) (*Index, error) {
	f, err := os.Open(filepath.Join(dir, "hnsw.graph"))
	if err != nil {
		return nil, fmt.Errorf("read hnsw: %w", err)
	}
	defer f.Close()

	var magic [8]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		return nil, fmt.Errorf("read hnsw magic: %w", err)
	}
	if magic != hnswMagic {
		return nil, fmt.Errorf("invalid hnsw format")
	}

	var version uint16
	if err := binary.Read(f, binary.LittleEndian, &version); err != nil {
		return nil, fmt.Errorf("read hnsw version: %w", err)
	}
	if version != hnswPersistVersion {
		return nil, fmt.Errorf("unsupported hnsw version %d", version)
	}

	var m, mMax0, efConstruction, efSearch int32
	if err := binary.Read(f, binary.LittleEndian, &m); err != nil {
		return nil, fmt.Errorf("read config m: %w", err)
	}
	if err := binary.Read(f, binary.LittleEndian, &mMax0); err != nil {
		return nil, fmt.Errorf("read config mmax0: %w", err)
	}
	if err := binary.Read(f, binary.LittleEndian, &efConstruction); err != nil {
		return nil, fmt.Errorf("read config efConstruction: %w", err)
	}
	if err := binary.Read(f, binary.LittleEndian, &efSearch); err != nil {
		return nil, fmt.Errorf("read config efSearch: %w", err)
	}

	var entryPointID uint64
	var maxLayer int32
	if err := binary.Read(f, binary.LittleEndian, &entryPointID); err != nil {
		return nil, fmt.Errorf("read entry point: %w", err)
	}
	if err := binary.Read(f, binary.LittleEndian, &maxLayer); err != nil {
		return nil, fmt.Errorf("read max layer: %w", err)
	}

	var nodeCount uint64
	if err := binary.Read(f, binary.LittleEndian, &nodeCount); err != nil {
		return nil, fmt.Errorf("read node count: %w", err)
	}

	cfg := Config{
		M:              int(m),
		MMax0:          int(mMax0),
		EfConstruction: int(efConstruction),
		EfSearch:       int(efSearch),
	}
	idx := New(cfg, distFunc, getVector)
	idx.entryPointID = entryPointID
	idx.maxLayer = int(maxLayer)

	for i := uint64(0); i < nodeCount; i++ {
		var id uint64
		var layer int32
		var levelCount uint32
		if err := binary.Read(f, binary.LittleEndian, &id); err != nil {
			return nil, fmt.Errorf("read node id: %w", err)
		}
		if err := binary.Read(f, binary.LittleEndian, &layer); err != nil {
			return nil, fmt.Errorf("read node layer: %w", err)
		}
		if err := binary.Read(f, binary.LittleEndian, &levelCount); err != nil {
			return nil, fmt.Errorf("read node level count: %w", err)
		}

		neighbours := make([][]uint64, levelCount)
		for level := 0; level < int(levelCount); level++ {
			var degree uint32
			if err := binary.Read(f, binary.LittleEndian, &degree); err != nil {
				return nil, fmt.Errorf("read node degree: %w", err)
			}
			neighbours[level] = make([]uint64, degree)
			for j := 0; j < int(degree); j++ {
				if err := binary.Read(f, binary.LittleEndian, &neighbours[level][j]); err != nil {
					return nil, fmt.Errorf("read neighbour id: %w", err)
				}
			}
		}

		idx.nodes[id] = &node{
			ID:         id,
			Layer:      int(layer),
			Neighbours: neighbours,
		}
	}

	return idx, nil
}

// ─── Internal algorithms ─────────────────────────────────

// randomLevel generates a random layer for a new node.
// P(layer=l) = (1/M)^l, giving an exponential distribution.
func (idx *Index) randomLevel() int {
	ml := 1.0 / math.Log(float64(idx.config.M))
	r := idx.rng.Float64()
	if r == 0 {
		r = 1e-10
	}
	return int(math.Floor(-math.Log(r) * ml))
}

// greedyClosest finds the closest node to query at a given layer by greedy traversal.
func (idx *Index) greedyClosest(queryDist QueryDistanceFunc, entryID uint64, layer int) uint64 {
	currentID := entryID
	currentVec, err := idx.getVector(currentID)
	if err != nil {
		return currentID
	}
	currentDist := queryDist(currentVec)

	for {
		changed := false
		n := idx.nodes[currentID]
		if n == nil || layer >= len(n.Neighbours) {
			break
		}
		for _, nbrID := range n.Neighbours[layer] {
			if idx.nodes[nbrID] == nil {
				continue
			}
			nbrVec, err := idx.getVector(nbrID)
			if err != nil {
				continue
			}
			d := queryDist(nbrVec)
			if d < currentDist {
				currentID = nbrID
				currentDist = d
				changed = true
			}
		}
		if !changed {
			break
		}
	}
	return currentID
}

// candidate is an (id, distance) pair for priority queues.
type candidate struct {
	id   uint64
	dist float32
}

// searchLayer performs beam search at a specific layer.
// Returns up to ef closest candidates sorted by distance (ascending).
func (idx *Index) searchLayer(queryDist QueryDistanceFunc, entryID uint64, ef int, layer int) []candidate {
	entryVec, err := idx.getVector(entryID)
	if err != nil {
		return nil
	}
	entryDist := queryDist(entryVec)

	visited := map[uint64]bool{entryID: true}

	// candidates: min-heap (closest first) for expansion
	cands := &minHeap{{id: entryID, dist: entryDist}}
	heap.Init(cands)

	// results: max-heap (farthest first) for keeping top-ef
	results := &maxHeap{{id: entryID, dist: entryDist}}
	heap.Init(results)

	for cands.Len() > 0 {
		c := heap.Pop(cands).(candidate)

		// If this candidate is farther than the farthest result, stop
		if results.Len() >= ef && c.dist > (*results)[0].dist {
			break
		}

		n := idx.nodes[c.id]
		if n == nil || layer >= len(n.Neighbours) {
			continue
		}

		for _, nbrID := range n.Neighbours[layer] {
			if visited[nbrID] || idx.nodes[nbrID] == nil {
				continue
			}
			visited[nbrID] = true

			nbrVec, err := idx.getVector(nbrID)
			if err != nil {
				continue
			}
			d := queryDist(nbrVec)

			if results.Len() < ef || d < (*results)[0].dist {
				heap.Push(cands, candidate{id: nbrID, dist: d})
				heap.Push(results, candidate{id: nbrID, dist: d})
				if results.Len() > ef {
					heap.Pop(results)
				}
			}
		}
	}

	// Collect results sorted by distance (ascending)
	sorted := make([]candidate, results.Len())
	for i := len(sorted) - 1; i >= 0; i-- {
		sorted[i] = heap.Pop(results).(candidate)
	}
	return sorted
}

// selectNeighboursSimple returns the closest maxConn candidates.
func (idx *Index) selectNeighboursSimple(candidates []candidate, maxConn int) []uint64 {
	if len(candidates) <= maxConn {
		ids := make([]uint64, len(candidates))
		for i, c := range candidates {
			ids[i] = c.id
		}
		return ids
	}
	ids := make([]uint64, maxConn)
	for i := 0; i < maxConn; i++ {
		ids[i] = candidates[i].id
	}
	return ids
}

// pruneConnections selects the best maxConn connections for a node.
func (idx *Index) pruneConnections(query []float32, neighbours []uint64, maxConn int, nodeID uint64) []uint64 {
	nodeVec, err := idx.getVector(nodeID)
	if err != nil {
		if len(neighbours) > maxConn {
			return neighbours[:maxConn]
		}
		return neighbours
	}

	cands := make([]candidate, 0, len(neighbours))
	for _, nbrID := range neighbours {
		nbrVec, err := idx.getVector(nbrID)
		if err != nil {
			continue
		}
		d := idx.distFunc(nodeVec, nbrVec)
		cands = append(cands, candidate{id: nbrID, dist: d})
	}

	// Sort by distance
	h := &minHeap{}
	*h = cands
	heap.Init(h)

	result := make([]uint64, 0, maxConn)
	for h.Len() > 0 && len(result) < maxConn {
		c := heap.Pop(h).(candidate)
		result = append(result, c.id)
	}
	return result
}

// ─── Heap implementations ────────────────────────────────

// minHeap is a min-heap of candidates (closest first).
type minHeap []candidate

func (h minHeap) Len() int            { return len(h) }
func (h minHeap) Less(i, j int) bool  { return h[i].dist < h[j].dist }
func (h minHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x interface{}) { *h = append(*h, x.(candidate)) }
func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// maxHeap is a max-heap of candidates (farthest first).
type maxHeap []candidate

func (h maxHeap) Len() int            { return len(h) }
func (h maxHeap) Less(i, j int) bool  { return h[i].dist > h[j].dist }
func (h maxHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *maxHeap) Push(x interface{}) { *h = append(*h, x.(candidate)) }
func (h *maxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
