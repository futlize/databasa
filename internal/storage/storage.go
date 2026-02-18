package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/juniodev/kekdb/internal/resources"
)

const (
	metaFileName      = "meta.bin"
	walActiveFileName = "wal-active.log"
	segmentDirName    = "segments"

	metaVersion    uint16 = 3
	walVersion     uint16 = 1
	segmentVersion uint16 = 1

	opUpsert byte = 1
	opDelete byte = 2

	defaultMemtableFlushOps   = 2048
	defaultCompactionSegments = 4
)

var (
	metaMagic    = [8]byte{'K', 'D', 'B', 'M', 'E', 'T', 'A', '3'}
	walMagic     = [8]byte{'K', 'D', 'B', 'W', 'A', 'L', '0', '1'}
	segmentMagic = [8]byte{'K', 'D', 'B', 'S', 'E', 'G', '0', '1'}
)

// Meta stores collection-level settings persisted in meta.bin.
type Meta struct {
	Name        string `json:"name"`
	Dimension   uint32 `json:"dimension"`
	Metric      string `json:"metric"`      // "cosine", "l2", "dot_product"
	Compression string `json:"compression"` // "none", "int8"
	Count       uint64 `json:"count"`
}

// VectorStore implements a WAL + memtable + immutable segment engine.
//
// Design notes:
// - Acknowledged writes only require WAL append + WAL fsync + in-memory apply.
// - Inserts do not wait for segment flushes or ANN index maintenance.
// - Segment flush and compaction run in background workers.
//
// This removes synchronous write amplification from the foreground insert path.
type VectorStore struct {
	mu   sync.RWMutex
	dir  string
	meta Meta

	keyToID map[string]uint64
	idToKey map[uint64]string
	vectors map[uint64][]float32
	deleted map[uint64]bool
	nextID  uint64

	// Memtable keeps the latest operation per key since last WAL rotation.
	memtable map[string]memRecord

	flushThresholdOps int
	pendingFlush      []flushTask
	resourceManager   *resources.Manager

	walFile       *os.File
	walActivePath string
	nextWALSeq    uint64

	segments      []segmentMeta
	nextSegmentID uint64

	flushCh chan struct{}
	// compaction is decoupled from writes and runs opportunistically.
	compactCh chan struct{}
	closeCh   chan struct{}
	closeOnce sync.Once
	bgWG      sync.WaitGroup
}

type memRecord struct {
	op        byte
	id        uint64
	key       string
	embedding []float32
}

type flushTask struct {
	segmentID uint64
	walPath   string
	records   []memRecord
}

type segmentMeta struct {
	id          uint64
	path        string
	recordCount int
}

// SearchHit is one brute-force search result from a shard.
type SearchHit struct {
	ID       uint64
	Key      string
	Distance float32
}

// BatchInsertItem is one upsert request handled in a single storage batch.
type BatchInsertItem struct {
	Key       string
	Embedding []float32
}

// BatchInsertResult reports one upsert that was applied during batch insertion.
type BatchInsertResult struct {
	Key    string
	ID     uint64
	HadOld bool
	OldID  uint64
}

// Open opens or creates a vector store with default compression ("none").
func Open(dir string, name string, dimension uint32, metric string) (*VectorStore, error) {
	return OpenWithCompression(dir, name, dimension, metric, "none")
}

// OpenWithCompression opens or creates a vector store with configurable compression.
func OpenWithCompression(dir string, name string, dimension uint32, metric, compression string) (*VectorStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create dir: %w", err)
	}

	metric = normalizeMetric(metric)
	compression = normalizeCompression(compression)

	vs := &VectorStore{
		dir:               dir,
		keyToID:           make(map[string]uint64),
		idToKey:           make(map[uint64]string),
		vectors:           make(map[uint64][]float32),
		deleted:           make(map[uint64]bool),
		memtable:          make(map[string]memRecord),
		flushThresholdOps: defaultMemtableFlushOps,
		walActivePath:     filepath.Join(dir, walActiveFileName),
		resourceManager:   resources.Global(),
		flushCh:           make(chan struct{}, 1),
		compactCh:         make(chan struct{}, 1),
		closeCh:           make(chan struct{}),
		meta: Meta{
			Name:        name,
			Dimension:   dimension,
			Metric:      metric,
			Compression: compression,
		},
	}

	if err := vs.loadOrInitializeMeta(name, dimension, metric, compression); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dir, segmentDirName), 0o755); err != nil {
		return nil, fmt.Errorf("create segment dir: %w", err)
	}
	if err := vs.loadSegments(); err != nil {
		return nil, err
	}

	rotatedWalPaths, maxWalSeq, err := vs.replayWALFiles()
	if err != nil {
		return nil, err
	}
	vs.nextWALSeq = maxWalSeq + 1

	if err := vs.checkpointActiveWAL(rotatedWalPaths); err != nil {
		return nil, err
	}

	vs.meta.Count = uint64(len(vs.keyToID))
	if err := vs.persistMeta(); err != nil {
		_ = vs.walFile.Close()
		return nil, err
	}

	vs.startBackgroundWorkers()
	if len(vs.memtable) >= vs.flushThresholdOps {
		vs.signalFlush()
	}
	if len(vs.segments) >= defaultCompactionSegments {
		vs.signalCompaction()
	}

	return vs, nil
}

func (vs *VectorStore) startBackgroundWorkers() {
	vs.bgWG.Add(2)
	go vs.flushLoop()
	go vs.compactionLoop()
}

func (vs *VectorStore) withWorker(op string, fn func() error) error {
	if vs.resourceManager != nil {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			err := vs.resourceManager.AcquireWorker(ctx, op)
			cancel()
			if err == nil {
				break
			}
			select {
			case <-vs.closeCh:
				return context.Canceled
			default:
			}
		}
		defer vs.resourceManager.ReleaseWorker()
	}
	return fn()
}

func (vs *VectorStore) flushLoop() {
	defer vs.bgWG.Done()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-vs.closeCh:
			_ = vs.withWorker("storage_flush", func() error { return vs.flushCycle(true) })
			return
		case <-vs.flushCh:
			_ = vs.withWorker("storage_flush", func() error { return vs.flushCycle(false) })
		case <-ticker.C:
			_ = vs.withWorker("storage_flush", func() error { return vs.flushCycle(false) })
		}
	}
}

func (vs *VectorStore) compactionLoop() {
	defer vs.bgWG.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-vs.closeCh:
			return
		case <-vs.compactCh:
			_ = vs.withWorker("storage_compaction", func() error { return vs.compactIfNeeded() })
		case <-ticker.C:
			_ = vs.withWorker("storage_compaction", func() error { return vs.compactIfNeeded() })
		}
	}
}

func (vs *VectorStore) flushCycle(force bool) error {
	if err := vs.rotateMemtableForFlush(force); err != nil {
		return err
	}
	if err := vs.processPendingFlushTasks(); err != nil {
		return err
	}
	return nil
}

func (vs *VectorStore) signalFlush() {
	select {
	case vs.flushCh <- struct{}{}:
	default:
	}
}

func (vs *VectorStore) signalFlushLocked() {
	select {
	case vs.flushCh <- struct{}{}:
	default:
	}
}

func (vs *VectorStore) signalCompaction() {
	select {
	case vs.compactCh <- struct{}{}:
	default:
	}
}

func (vs *VectorStore) rotateMemtableForFlush(force bool) error {
	for {
		vs.mu.Lock()
		shouldRotate := len(vs.memtable) > 0 && (force || len(vs.memtable) >= vs.flushThresholdOps)
		if !shouldRotate {
			vs.mu.Unlock()
			return nil
		}
		if err := vs.rotateMemtableLocked(); err != nil {
			vs.mu.Unlock()
			return err
		}
		vs.mu.Unlock()
	}
}

func (vs *VectorStore) rotateMemtableLocked() error {
	if len(vs.memtable) == 0 {
		return nil
	}
	if vs.walFile == nil {
		return fmt.Errorf("wal not open")
	}

	records := vs.memtableRecordsLocked()
	rotatedWalPath := filepath.Join(vs.dir, fmt.Sprintf("wal-%020d.log", vs.nextWALSeq))
	vs.nextWALSeq++

	if err := vs.walFile.Sync(); err != nil {
		return fmt.Errorf("sync active wal before rotation: %w", err)
	}
	if err := vs.walFile.Close(); err != nil {
		return fmt.Errorf("close active wal before rotation: %w", err)
	}
	vs.walFile = nil

	if err := os.Rename(vs.walActivePath, rotatedWalPath); err != nil {
		return fmt.Errorf("rotate wal: %w", err)
	}

	newWal, err := createFreshWAL(vs.walActivePath)
	if err != nil {
		_ = os.Rename(rotatedWalPath, vs.walActivePath)
		reopened, openErr := openWALForAppend(vs.walActivePath)
		if openErr == nil {
			vs.walFile = reopened
		}
		return fmt.Errorf("create new active wal: %w", err)
	}
	vs.walFile = newWal

	vs.memtable = make(map[string]memRecord)
	vs.pendingFlush = append(vs.pendingFlush, flushTask{
		segmentID: vs.nextSegmentID,
		walPath:   rotatedWalPath,
		records:   records,
	})
	vs.nextSegmentID++

	return nil
}

func (vs *VectorStore) processPendingFlushTasks() error {
	for {
		vs.mu.Lock()
		if len(vs.pendingFlush) == 0 {
			vs.mu.Unlock()
			return nil
		}
		task := vs.pendingFlush[0]
		vs.mu.Unlock()

		segmentPath, err := vs.writeSegment(task.segmentID, task.records)
		if err != nil {
			return err
		}

		_ = os.Remove(task.walPath)

		vs.mu.Lock()
		if len(vs.pendingFlush) > 0 && vs.pendingFlush[0].segmentID == task.segmentID {
			vs.pendingFlush = vs.pendingFlush[1:]
		} else {
			for i := range vs.pendingFlush {
				if vs.pendingFlush[i].segmentID == task.segmentID {
					vs.pendingFlush = append(vs.pendingFlush[:i], vs.pendingFlush[i+1:]...)
					break
				}
			}
		}
		vs.segments = append(vs.segments, segmentMeta{
			id:          task.segmentID,
			path:        segmentPath,
			recordCount: len(task.records),
		})
		shouldCompact := len(vs.segments) >= defaultCompactionSegments
		vs.mu.Unlock()

		if shouldCompact {
			vs.signalCompaction()
		}
	}
}

func (vs *VectorStore) compactIfNeeded() error {
	vs.mu.Lock()
	if len(vs.segments) < defaultCompactionSegments {
		vs.mu.Unlock()
		return nil
	}

	oldSegments := make([]segmentMeta, len(vs.segments))
	copy(oldSegments, vs.segments)
	maxOldSegmentID := oldSegments[len(oldSegments)-1].id

	compactSegmentID := vs.nextSegmentID
	vs.nextSegmentID++
	vs.mu.Unlock()

	vs.mu.RLock()
	activeCount := len(vs.keyToID)
	dimension := vs.meta.Dimension
	vs.mu.RUnlock()
	reserveBytes := uint64(activeCount) * uint64(dimension) * 8
	if err := vs.resourceManager.WaitForMemory(context.Background(), reserveBytes, "storage_compaction_snapshot"); err != nil {
		return err
	}

	type activeRecord struct {
		id        uint64
		key       string
		embedding []float32
	}

	vs.mu.RLock()
	active := make([]activeRecord, 0, len(vs.keyToID))
	for key, id := range vs.keyToID {
		vec, ok := vs.vectors[id]
		if !ok {
			continue
		}
		active = append(active, activeRecord{
			id:        id,
			key:       key,
			embedding: cloneEmbedding(vec),
		})
	}
	vs.mu.RUnlock()

	sort.Slice(active, func(i, j int) bool {
		return active[i].id < active[j].id
	})

	records := make([]memRecord, 0, len(active))
	for _, item := range active {
		records = append(records, memRecord{
			op:        opUpsert,
			id:        item.id,
			key:       item.key,
			embedding: item.embedding,
		})
	}

	segmentPath, err := vs.writeSegment(compactSegmentID, records)
	if err != nil {
		return err
	}

	vs.mu.Lock()
	newerSegments := make([]segmentMeta, 0, len(vs.segments))
	for _, seg := range vs.segments {
		if seg.id > maxOldSegmentID {
			newerSegments = append(newerSegments, seg)
		}
	}
	vs.segments = append([]segmentMeta{{
		id:          compactSegmentID,
		path:        segmentPath,
		recordCount: len(records),
	}}, newerSegments...)
	vs.mu.Unlock()

	for _, seg := range oldSegments {
		_ = os.Remove(seg.path)
	}

	return nil
}

// Insert adds a key+embedding pair and returns the internal ID.
func (vs *VectorStore) Insert(key string, embedding []float32) (uint64, error) {
	if err := vs.resourceManager.WaitInsertAllowance(context.Background()); err != nil {
		return 0, err
	}
	if err := vs.resourceManager.WaitForMemory(context.Background(), uint64(len(embedding))*8, "storage_insert"); err != nil {
		return 0, err
	}

	vs.mu.Lock()
	defer vs.mu.Unlock()

	if key == "" {
		return 0, fmt.Errorf("key is required")
	}
	if uint32(len(embedding)) != vs.meta.Dimension {
		return 0, fmt.Errorf("dimension mismatch: got %d, want %d", len(embedding), vs.meta.Dimension)
	}

	id := vs.nextID
	rec := memRecord{
		op:        opUpsert,
		id:        id,
		key:       key,
		embedding: prepareEmbeddingForWrite(vs.meta.Metric, embedding),
	}

	// Foreground write path in the new engine:
	// 1) append WAL record
	// 2) fsync WAL for durability
	// 3) apply to in-memory state
	// No index mutation is performed here.
	if err := vs.appendWALRecordsLocked([]memRecord{rec}); err != nil {
		return 0, err
	}

	vs.applyRecordToState(rec)
	vs.applyRecordToMemtable(rec)
	if len(vs.memtable) >= vs.flushThresholdOps {
		vs.signalFlushLocked()
	}

	return id, nil
}

// BatchInsert inserts/upserts multiple vectors.
func (vs *VectorStore) BatchInsert(items []BatchInsertItem) ([]BatchInsertResult, error) {
	if err := vs.resourceManager.WaitInsertAllowance(context.Background()); err != nil {
		return nil, err
	}
	var reserveBytes uint64
	for _, item := range items {
		reserveBytes += uint64(len(item.Embedding)) * 8
	}
	if err := vs.resourceManager.WaitForMemory(context.Background(), reserveBytes, "storage_batch_insert"); err != nil {
		return nil, err
	}

	vs.mu.Lock()
	defer vs.mu.Unlock()

	if len(items) == 0 {
		return nil, nil
	}

	nextID := vs.nextID
	shadowKeyToID := make(map[string]uint64, len(items))
	records := make([]memRecord, 0, len(items))
	results := make([]BatchInsertResult, 0, len(items))

	for _, item := range items {
		if item.Key == "" {
			return nil, fmt.Errorf("key is required")
		}
		if uint32(len(item.Embedding)) != vs.meta.Dimension {
			return nil, fmt.Errorf("dimension mismatch: got %d, want %d", len(item.Embedding), vs.meta.Dimension)
		}

		oldID, exists := shadowKeyToID[item.Key]
		if !exists {
			oldID, exists = vs.keyToID[item.Key]
		}

		id := nextID
		nextID++
		shadowKeyToID[item.Key] = id

		result := BatchInsertResult{Key: item.Key, ID: id}
		if exists {
			result.HadOld = true
			result.OldID = oldID
		}
		results = append(results, result)
		records = append(records, memRecord{
			op:        opUpsert,
			id:        id,
			key:       item.Key,
			embedding: prepareEmbeddingForWrite(vs.meta.Metric, item.Embedding),
		})
	}

	if err := vs.appendWALRecordsLocked(records); err != nil {
		return nil, err
	}

	for _, rec := range records {
		vs.applyRecordToState(rec)
		vs.applyRecordToMemtable(rec)
	}
	if len(vs.memtable) >= vs.flushThresholdOps {
		vs.signalFlushLocked()
	}

	return results, nil
}

// Delete removes a key from the store.
func (vs *VectorStore) Delete(key string) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	id, exists := vs.keyToID[key]
	if !exists {
		return fmt.Errorf("key not found: %s", key)
	}

	rec := memRecord{op: opDelete, id: id, key: key}
	if err := vs.appendWALRecordsLocked([]memRecord{rec}); err != nil {
		return err
	}

	vs.applyRecordToState(rec)
	vs.applyRecordToMemtable(rec)
	if len(vs.memtable) >= vs.flushThresholdOps {
		vs.signalFlushLocked()
	}

	return nil
}

// GetByKey returns the embedding for a given key.
func (vs *VectorStore) GetByKey(key string) ([]float32, error) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	id, ok := vs.keyToID[key]
	if !ok {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	vec, ok := vs.vectors[id]
	if !ok {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	return cloneEmbedding(vec), nil
}

// GetByID returns the embedding for a given internal ID.
func (vs *VectorStore) GetByID(id uint64) ([]float32, error) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	if _, ok := vs.idToKey[id]; !ok {
		return nil, fmt.Errorf("id not found: %d", id)
	}
	if vs.deleted[id] {
		return nil, fmt.Errorf("id not found: %d", id)
	}
	vec, ok := vs.vectors[id]
	if !ok {
		return nil, fmt.Errorf("id not found: %d", id)
	}
	return cloneEmbedding(vec), nil
}

// GetID returns the internal ID for the given key.
func (vs *VectorStore) GetID(key string) (uint64, bool) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	id, ok := vs.keyToID[key]
	return id, ok
}

// GetKey returns the key for a given internal ID.
func (vs *VectorStore) GetKey(id uint64) (string, bool) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	key, ok := vs.idToKey[id]
	return key, ok
}

// ResolveActiveKeys resolves keys for candidate IDs in one lock acquisition.
func (vs *VectorStore) ResolveActiveKeys(ids []uint64) map[uint64]string {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	out := make(map[uint64]string, len(ids))
	for _, id := range ids {
		key, ok := vs.idToKey[id]
		if !ok {
			continue
		}
		if vs.deleted[id] {
			continue
		}
		out[id] = key
	}
	return out
}

// IsDeleted checks if an internal ID has been tombstoned.
func (vs *VectorStore) IsDeleted(id uint64) bool {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.deleted[id]
}

// Count returns the number of active (non-deleted) vectors.
func (vs *VectorStore) Count() uint64 {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return uint64(len(vs.keyToID))
}

// Dimension returns the vector dimension for this collection.
func (vs *VectorStore) Dimension() uint32 {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.meta.Dimension
}

// MetricName returns the metric name.
func (vs *VectorStore) MetricName() string {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.meta.Metric
}

// CompressionName returns the compression mode for this shard.
func (vs *VectorStore) CompressionName() string {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.meta.Compression
}

// AllIDs returns all active internal IDs.
func (vs *VectorStore) AllIDs() []uint64 {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	ids := make([]uint64, 0, len(vs.idToKey))
	for id := range vs.idToKey {
		ids = append(ids, id)
	}
	return ids
}

// SearchBruteForceTopK scans active vectors in memory.
// Active state already includes WAL-backed memtable writes and segment-backed state.
func (vs *VectorStore) SearchBruteForceTopK(query []float32, topK int) ([]SearchHit, error) {
	if topK <= 0 {
		return nil, nil
	}

	type activeEntry struct {
		id        uint64
		key       string
		embedding []float32
	}

	vs.mu.RLock()
	dimension := vs.meta.Dimension
	metric := vs.meta.Metric
	activeCount := len(vs.keyToID)
	vs.mu.RUnlock()

	if err := vs.resourceManager.WaitForMemory(context.Background(), uint64(activeCount)*uint64(dimension)*4, "storage_search_snapshot"); err != nil {
		return nil, err
	}

	vs.mu.RLock()
	active := make([]activeEntry, 0, len(vs.keyToID))
	for key, id := range vs.keyToID {
		vec, ok := vs.vectors[id]
		if !ok {
			continue
		}
		active = append(active, activeEntry{id: id, key: key, embedding: vec})
	}
	vs.mu.RUnlock()

	if uint32(len(query)) != dimension {
		return nil, fmt.Errorf("dimension mismatch: got %d, want %d", len(query), dimension)
	}
	if metric == "cosine" {
		query = prepareEmbeddingForWrite(metric, query)
	}
	if len(active) == 0 {
		return nil, nil
	}

	sort.Slice(active, func(i, j int) bool {
		return active[i].id < active[j].id
	})

	distance := buildDistanceEvaluator(metric, query)
	limit := topK
	if len(active) < limit {
		limit = len(active)
	}
	best := make([]SearchHit, 0, limit)

	for _, item := range active {
		dist := distance(item.embedding)
		candidate := SearchHit{
			ID:       item.id,
			Key:      item.key,
			Distance: dist,
		}
		pushSearchHitTopK(&best, candidate, topK)
	}

	sort.Slice(best, func(i, j int) bool {
		if best[i].Distance == best[j].Distance {
			return best[i].Key < best[j].Key
		}
		return best[i].Distance < best[j].Distance
	})

	return best, nil
}

func pushSearchHitTopK(best *[]SearchHit, candidate SearchHit, limit int) {
	if limit <= 0 {
		return
	}
	if len(*best) < limit {
		*best = append(*best, candidate)
		return
	}

	worstIdx := 0
	worstDist := (*best)[0].Distance
	for i := 1; i < len(*best); i++ {
		if (*best)[i].Distance > worstDist {
			worstDist = (*best)[i].Distance
			worstIdx = i
		}
	}

	if candidate.Distance >= worstDist {
		return
	}
	(*best)[worstIdx] = candidate
}

func buildDistanceEvaluator(metric string, query []float32) func(candidate []float32) float32 {
	switch metric {
	case "l2":
		return func(candidate []float32) float32 {
			var sum float32
			for i, q := range query {
				d := q - candidate[i]
				sum += d * d
			}
			return sum
		}
	case "dot_product", "cosine":
		return func(candidate []float32) float32 {
			var dot float32
			for i, q := range query {
				dot += q * candidate[i]
			}
			return -dot
		}
	default:
		var queryNormSq float32
		for _, q := range query {
			queryNormSq += q * q
		}
		queryNorm := float32(math.Sqrt(float64(queryNormSq)))
		if queryNorm == 0 {
			return func(_ []float32) float32 { return 1.0 }
		}

		return func(candidate []float32) float32 {
			var dot, normB float32
			for i, q := range query {
				v := candidate[i]
				dot += q * v
				normB += v * v
			}
			if normB == 0 {
				return 1.0
			}
			sim := dot / (queryNorm * float32(math.Sqrt(float64(normB))))
			if sim > 1 {
				sim = 1
			} else if sim < -1 {
				sim = -1
			}
			return 1.0 - sim
		}
	}
}

func (vs *VectorStore) applyRecordToState(rec memRecord) {
	switch rec.op {
	case opUpsert:
		if oldID, exists := vs.keyToID[rec.key]; exists && oldID != rec.id {
			vs.deleted[oldID] = true
			delete(vs.idToKey, oldID)
			delete(vs.vectors, oldID)
		}
		vs.keyToID[rec.key] = rec.id
		vs.idToKey[rec.id] = rec.key
		vs.vectors[rec.id] = cloneEmbedding(rec.embedding)
		delete(vs.deleted, rec.id)
		if rec.id >= vs.nextID {
			vs.nextID = rec.id + 1
		}
	case opDelete:
		vs.deleted[rec.id] = true
		if rec.key != "" {
			if currentID, exists := vs.keyToID[rec.key]; exists && currentID == rec.id {
				delete(vs.keyToID, rec.key)
			}
		}
		if existingKey, exists := vs.idToKey[rec.id]; exists {
			delete(vs.idToKey, rec.id)
			delete(vs.vectors, rec.id)
			if currentID, ok := vs.keyToID[existingKey]; ok && currentID == rec.id {
				delete(vs.keyToID, existingKey)
			}
		}
	}
	vs.meta.Count = uint64(len(vs.keyToID))
}

func (vs *VectorStore) applyRecordToMemtable(rec memRecord) {
	switch rec.op {
	case opUpsert:
		vs.memtable[rec.key] = memRecord{
			op:        opUpsert,
			id:        rec.id,
			key:       rec.key,
			embedding: cloneEmbedding(rec.embedding),
		}
	case opDelete:
		vs.memtable[rec.key] = memRecord{
			op:  opDelete,
			id:  rec.id,
			key: rec.key,
		}
	}
}

func cloneEmbedding(in []float32) []float32 {
	if len(in) == 0 {
		return nil
	}
	out := make([]float32, len(in))
	copy(out, in)
	return out
}

func prepareEmbeddingForWrite(metric string, embedding []float32) []float32 {
	out := cloneEmbedding(embedding)
	if metric != "cosine" || len(out) == 0 {
		return out
	}

	var normSq float64
	for _, v := range out {
		normSq += float64(v) * float64(v)
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

func (vs *VectorStore) appendWALRecordsLocked(records []memRecord) error {
	if len(records) == 0 {
		return nil
	}
	if vs.walFile == nil {
		return fmt.Errorf("wal not open")
	}

	if _, err := vs.walFile.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek wal: %w", err)
	}
	for _, rec := range records {
		if err := writeRecord(vs.walFile, rec); err != nil {
			return fmt.Errorf("append wal record: %w", err)
		}
	}
	if err := vs.walFile.Sync(); err != nil {
		return fmt.Errorf("sync wal: %w", err)
	}
	return nil
}

func (vs *VectorStore) writeSegment(segmentID uint64, records []memRecord) (string, error) {
	segDir := filepath.Join(vs.dir, segmentDirName)
	if err := os.MkdirAll(segDir, 0o755); err != nil {
		return "", fmt.Errorf("ensure segment dir: %w", err)
	}

	segmentPath := filepath.Join(segDir, fmt.Sprintf("segment-%020d.seg", segmentID))
	tmpPath := segmentPath + ".tmp"

	f, err := os.Create(tmpPath)
	if err != nil {
		return "", fmt.Errorf("create segment temp: %w", err)
	}
	defer f.Close()

	if err := writeLogHeader(f, segmentMagic, segmentVersion); err != nil {
		return "", fmt.Errorf("write segment header: %w", err)
	}
	for _, rec := range records {
		if err := writeRecord(f, rec); err != nil {
			return "", fmt.Errorf("write segment record: %w", err)
		}
	}
	if err := f.Sync(); err != nil {
		return "", fmt.Errorf("sync segment: %w", err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("close segment temp: %w", err)
	}
	if err := os.Rename(tmpPath, segmentPath); err != nil {
		return "", fmt.Errorf("commit segment: %w", err)
	}

	return segmentPath, nil
}

func writeLogHeader(w io.Writer, magic [8]byte, version uint16) error {
	if _, err := w.Write(magic[:]); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, version)
}

func readLogHeader(r io.Reader, expectedMagic [8]byte, expectedVersion uint16) error {
	var magic [8]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return err
	}
	if magic != expectedMagic {
		return fmt.Errorf("invalid log magic")
	}
	var version uint16
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return err
	}
	if version != expectedVersion {
		return fmt.Errorf("unsupported log version %d", version)
	}
	return nil
}

func writeRecord(w io.Writer, rec memRecord) error {
	var payload bytes.Buffer
	if err := payload.WriteByte(rec.op); err != nil {
		return err
	}
	if err := binary.Write(&payload, binary.LittleEndian, rec.id); err != nil {
		return err
	}
	keyBytes := []byte(rec.key)
	if err := binary.Write(&payload, binary.LittleEndian, uint32(len(keyBytes))); err != nil {
		return err
	}
	if _, err := payload.Write(keyBytes); err != nil {
		return err
	}

	if rec.op == opUpsert {
		if err := binary.Write(&payload, binary.LittleEndian, uint32(len(rec.embedding))); err != nil {
			return err
		}
		for _, v := range rec.embedding {
			if err := binary.Write(&payload, binary.LittleEndian, math.Float32bits(v)); err != nil {
				return err
			}
		}
	}

	if err := binary.Write(w, binary.LittleEndian, uint32(payload.Len())); err != nil {
		return err
	}
	_, err := w.Write(payload.Bytes())
	return err
}

func readRecord(r io.Reader, dimension uint32, toleratePartial bool) (memRecord, error) {
	var recLen uint32
	if err := binary.Read(r, binary.LittleEndian, &recLen); err != nil {
		if errors.Is(err, io.EOF) {
			return memRecord{}, io.EOF
		}
		if errors.Is(err, io.ErrUnexpectedEOF) && toleratePartial {
			return memRecord{}, io.EOF
		}
		return memRecord{}, err
	}

	payload := make([]byte, recLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		if (errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)) && toleratePartial {
			return memRecord{}, io.EOF
		}
		return memRecord{}, err
	}

	reader := bytes.NewReader(payload)
	var op [1]byte
	if _, err := io.ReadFull(reader, op[:]); err != nil {
		return memRecord{}, err
	}

	var id uint64
	if err := binary.Read(reader, binary.LittleEndian, &id); err != nil {
		return memRecord{}, err
	}

	var keyLen uint32
	if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
		return memRecord{}, err
	}
	keyBytes := make([]byte, keyLen)
	if _, err := io.ReadFull(reader, keyBytes); err != nil {
		return memRecord{}, err
	}

	rec := memRecord{op: op[0], id: id, key: string(keyBytes)}
	switch rec.op {
	case opUpsert:
		var vectorLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &vectorLen); err != nil {
			return memRecord{}, err
		}
		if dimension > 0 && vectorLen != dimension {
			return memRecord{}, fmt.Errorf("vector dimension mismatch in log record: got %d want %d", vectorLen, dimension)
		}
		rec.embedding = make([]float32, vectorLen)
		for i := range rec.embedding {
			var bits uint32
			if err := binary.Read(reader, binary.LittleEndian, &bits); err != nil {
				return memRecord{}, err
			}
			rec.embedding[i] = math.Float32frombits(bits)
		}
	case opDelete:
		// no vector payload
	default:
		return memRecord{}, fmt.Errorf("unknown record op %d", rec.op)
	}

	if reader.Len() != 0 {
		return memRecord{}, fmt.Errorf("extra bytes at end of record")
	}

	return rec, nil
}

func openWALForAppend(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("stat wal: %w", err)
	}
	if info.Size() == 0 {
		if err := writeLogHeader(f, walMagic, walVersion); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("write wal header: %w", err)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("sync wal header: %w", err)
		}
	} else {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("seek wal header: %w", err)
		}
		if err := readLogHeader(f, walMagic, walVersion); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("read wal header: %w", err)
		}
	}

	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("seek wal end: %w", err)
	}
	return f, nil
}

func createFreshWAL(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("create wal: %w", err)
	}
	if err := writeLogHeader(f, walMagic, walVersion); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("write wal header: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("sync wal header: %w", err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("seek wal end: %w", err)
	}
	return f, nil
}

func (vs *VectorStore) loadSegments() error {
	segDir := filepath.Join(vs.dir, segmentDirName)
	entries, err := os.ReadDir(segDir)
	if err != nil {
		return fmt.Errorf("read segment dir: %w", err)
	}

	segments := make([]segmentMeta, 0, len(entries))
	var maxID uint64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		id, ok := parseSegmentID(entry.Name())
		if !ok {
			continue
		}
		path := filepath.Join(segDir, entry.Name())
		segments = append(segments, segmentMeta{id: id, path: path})
		if id > maxID {
			maxID = id
		}
	}

	sort.Slice(segments, func(i, j int) bool { return segments[i].id < segments[j].id })
	for i := range segments {
		count, err := vs.replaySegmentFile(segments[i].path)
		if err != nil {
			return fmt.Errorf("replay segment %s: %w", segments[i].path, err)
		}
		segments[i].recordCount = count
	}

	vs.segments = segments
	if len(segments) == 0 {
		vs.nextSegmentID = 1
	} else {
		vs.nextSegmentID = maxID + 1
	}
	return nil
}

func parseSegmentID(name string) (uint64, bool) {
	if !strings.HasPrefix(name, "segment-") || !strings.HasSuffix(name, ".seg") {
		return 0, false
	}
	idPart := strings.TrimSuffix(strings.TrimPrefix(name, "segment-"), ".seg")
	id, err := strconv.ParseUint(idPart, 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

func (vs *VectorStore) replaySegmentFile(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	if err := readLogHeader(f, segmentMagic, segmentVersion); err != nil {
		return 0, err
	}

	count := 0
	for {
		rec, err := readRecord(f, vs.meta.Dimension, false)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return count, err
		}
		vs.applyRecordToState(rec)
		count++
	}
	return count, nil
}

func parseRotatedWALSeq(name string) (uint64, bool) {
	if !strings.HasPrefix(name, "wal-") || !strings.HasSuffix(name, ".log") {
		return 0, false
	}
	if name == walActiveFileName {
		return 0, false
	}
	seqPart := strings.TrimSuffix(strings.TrimPrefix(name, "wal-"), ".log")
	seq, err := strconv.ParseUint(seqPart, 10, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
}

func (vs *VectorStore) replayWALFiles() ([]string, uint64, error) {
	type walRef struct {
		seq  uint64
		path string
	}

	entries, err := os.ReadDir(vs.dir)
	if err != nil {
		return nil, 0, fmt.Errorf("read data dir for wal replay: %w", err)
	}

	rotated := make([]walRef, 0)
	activeExists := false
	var maxSeq uint64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == walActiveFileName {
			activeExists = true
			continue
		}
		seq, ok := parseRotatedWALSeq(name)
		if !ok {
			continue
		}
		path := filepath.Join(vs.dir, name)
		rotated = append(rotated, walRef{seq: seq, path: path})
		if seq > maxSeq {
			maxSeq = seq
		}
	}

	sort.Slice(rotated, func(i, j int) bool {
		return rotated[i].seq < rotated[j].seq
	})

	pathsToRemove := make([]string, 0, len(rotated))
	for _, wal := range rotated {
		if _, err := vs.replayWALFile(wal.path); err != nil {
			return nil, maxSeq, fmt.Errorf("replay rotated wal %s: %w", wal.path, err)
		}
		pathsToRemove = append(pathsToRemove, wal.path)
	}
	if activeExists {
		if _, err := vs.replayWALFile(vs.walActivePath); err != nil {
			return nil, maxSeq, fmt.Errorf("replay active wal: %w", err)
		}
	}
	return pathsToRemove, maxSeq, nil
}

func (vs *VectorStore) replayWALFile(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if info.Size() == 0 {
		return 0, nil
	}

	if err := readLogHeader(f, walMagic, walVersion); err != nil {
		return 0, err
	}

	count := 0
	for {
		rec, err := readRecord(f, vs.meta.Dimension, true)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return count, err
		}
		vs.applyRecordToState(rec)
		vs.applyRecordToMemtable(rec)
		count++
	}
	return count, nil
}

func (vs *VectorStore) checkpointActiveWAL(rotatedWalPaths []string) error {
	f, err := createFreshWAL(vs.walActivePath)
	if err != nil {
		return err
	}

	records := vs.memtableRecordsLocked()
	for _, rec := range records {
		if err := writeRecord(f, rec); err != nil {
			_ = f.Close()
			return fmt.Errorf("checkpoint wal record: %w", err)
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync checkpoint wal: %w", err)
	}

	vs.walFile = f
	for _, path := range rotatedWalPaths {
		if path == vs.walActivePath {
			continue
		}
		_ = os.Remove(path)
	}
	return nil
}

func (vs *VectorStore) memtableRecordsLocked() []memRecord {
	keys := make([]string, 0, len(vs.memtable))
	for key := range vs.memtable {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	records := make([]memRecord, 0, len(keys))
	for _, key := range keys {
		rec := vs.memtable[key]
		records = append(records, memRecord{
			op:        rec.op,
			id:        rec.id,
			key:       rec.key,
			embedding: cloneEmbedding(rec.embedding),
		})
	}
	return records
}

// Close closes open resources and flushes pending background work.
func (vs *VectorStore) Close() error {
	var closeErr error
	vs.closeOnce.Do(func() {
		close(vs.closeCh)
		vs.bgWG.Wait()

		vs.mu.Lock()
		defer vs.mu.Unlock()

		var errs []error
		vs.meta.Count = uint64(len(vs.keyToID))
		if err := vs.persistMeta(); err != nil {
			errs = append(errs, err)
		}
		if vs.walFile != nil {
			if err := vs.walFile.Close(); err != nil {
				errs = append(errs, err)
			}
			vs.walFile = nil
		}
		closeErr = errors.Join(errs...)
	})
	return closeErr
}

func (vs *VectorStore) loadOrInitializeMeta(name string, dimension uint32, metric, compression string) error {
	metaPath := filepath.Join(vs.dir, metaFileName)
	m, err := readMeta(metaPath)
	if err == nil {
		vs.meta = m
	} else if errors.Is(err, os.ErrNotExist) {
		vs.meta = Meta{
			Name:        name,
			Dimension:   dimension,
			Metric:      metric,
			Compression: compression,
			Count:       0,
		}
		if vs.meta.Dimension == 0 {
			return fmt.Errorf("dimension must be > 0")
		}
		if vs.meta.Metric == "" {
			return fmt.Errorf("metric is required")
		}
		if err := vs.persistMeta(); err != nil {
			return err
		}
	} else {
		return err
	}

	if vs.meta.Name == "" {
		vs.meta.Name = name
	}
	if vs.meta.Compression == "" {
		vs.meta.Compression = "none"
	}
	vs.meta.Metric = normalizeMetric(vs.meta.Metric)
	vs.meta.Compression = normalizeCompression(vs.meta.Compression)

	if dimension > 0 && vs.meta.Dimension != dimension {
		return fmt.Errorf("dimension mismatch for collection %q: existing=%d requested=%d", name, vs.meta.Dimension, dimension)
	}
	if metric != "" && vs.meta.Metric != normalizeMetric(metric) {
		return fmt.Errorf("metric mismatch for collection %q: existing=%s requested=%s", name, vs.meta.Metric, normalizeMetric(metric))
	}
	if compression != "" && vs.meta.Compression != normalizeCompression(compression) {
		return fmt.Errorf("compression mismatch for collection %q: existing=%s requested=%s", name, vs.meta.Compression, normalizeCompression(compression))
	}
	if vs.meta.Dimension == 0 {
		return fmt.Errorf("dimension must be > 0")
	}

	return nil
}

func (vs *VectorStore) persistMeta() error {
	return writeMeta(filepath.Join(vs.dir, metaFileName), vs.meta)
}

func writeMeta(path string, meta Meta) error {
	metricCode, ok := metricToCode(meta.Metric)
	if !ok {
		return fmt.Errorf("unsupported metric %q", meta.Metric)
	}
	compressionCode, ok := compressionToCode(meta.Compression)
	if !ok {
		return fmt.Errorf("unsupported compression %q", meta.Compression)
	}
	nameBytes := []byte(meta.Name)
	if len(nameBytes) > math.MaxUint16 {
		return fmt.Errorf("name too long")
	}

	var buf bytes.Buffer
	buf.Write(metaMagic[:])
	if err := binary.Write(&buf, binary.LittleEndian, metaVersion); err != nil {
		return err
	}
	if err := binary.Write(&buf, binary.LittleEndian, meta.Dimension); err != nil {
		return err
	}
	if err := binary.Write(&buf, binary.LittleEndian, meta.Count); err != nil {
		return err
	}
	if err := buf.WriteByte(metricCode); err != nil {
		return err
	}
	if err := buf.WriteByte(compressionCode); err != nil {
		return err
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint16(len(nameBytes))); err != nil {
		return err
	}
	if _, err := buf.Write(nameBytes); err != nil {
		return err
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf.Bytes(), 0o644); err != nil {
		return fmt.Errorf("write meta temp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename meta: %w", err)
	}
	return nil
}

func readMeta(path string) (Meta, error) {
	f, err := os.Open(path)
	if err != nil {
		return Meta{}, err
	}
	defer f.Close()

	var magic [8]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		return Meta{}, fmt.Errorf("read meta magic: %w", err)
	}
	if magic != metaMagic {
		return Meta{}, fmt.Errorf("invalid meta format")
	}

	var version uint16
	if err := binary.Read(f, binary.LittleEndian, &version); err != nil {
		return Meta{}, err
	}
	if version != metaVersion {
		return Meta{}, fmt.Errorf("unsupported meta version %d", version)
	}

	var dimension uint32
	var count uint64
	if err := binary.Read(f, binary.LittleEndian, &dimension); err != nil {
		return Meta{}, err
	}
	if err := binary.Read(f, binary.LittleEndian, &count); err != nil {
		return Meta{}, err
	}

	var metricCode [1]byte
	var compressionCode [1]byte
	if _, err := io.ReadFull(f, metricCode[:]); err != nil {
		return Meta{}, err
	}
	if _, err := io.ReadFull(f, compressionCode[:]); err != nil {
		return Meta{}, err
	}

	var nameLen uint16
	if err := binary.Read(f, binary.LittleEndian, &nameLen); err != nil {
		return Meta{}, err
	}
	nameBytes := make([]byte, nameLen)
	if _, err := io.ReadFull(f, nameBytes); err != nil {
		return Meta{}, err
	}

	metric, ok := codeToMetric(metricCode[0])
	if !ok {
		return Meta{}, fmt.Errorf("invalid metric code %d", metricCode[0])
	}
	compression, ok := codeToCompression(compressionCode[0])
	if !ok {
		return Meta{}, fmt.Errorf("invalid compression code %d", compressionCode[0])
	}

	return Meta{
		Name:        string(nameBytes),
		Dimension:   dimension,
		Count:       count,
		Metric:      metric,
		Compression: compression,
	}, nil
}

func normalizeMetric(metric string) string {
	switch strings.ToLower(strings.TrimSpace(metric)) {
	case "l2":
		return "l2"
	case "dot", "dot_product", "dotproduct":
		return "dot_product"
	default:
		return "cosine"
	}
}

func normalizeCompression(compression string) string {
	switch strings.ToLower(strings.TrimSpace(compression)) {
	case "int8", "scalar8", "scalar_int8":
		return "int8"
	default:
		return "none"
	}
}

func metricToCode(metric string) (byte, bool) {
	switch normalizeMetric(metric) {
	case "cosine":
		return 1, true
	case "l2":
		return 2, true
	case "dot_product":
		return 3, true
	default:
		return 0, false
	}
}

func codeToMetric(code byte) (string, bool) {
	switch code {
	case 1:
		return "cosine", true
	case 2:
		return "l2", true
	case 3:
		return "dot_product", true
	default:
		return "", false
	}
}

func compressionToCode(compression string) (byte, bool) {
	switch normalizeCompression(compression) {
	case "none":
		return 0, true
	case "int8":
		return 1, true
	default:
		return 0, false
	}
}

func codeToCompression(code byte) (string, bool) {
	switch code {
	case 0:
		return "none", true
	case 1:
		return "int8", true
	default:
		return "", false
	}
}
