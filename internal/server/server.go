package server

import (
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io/fs"
	"log"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/futlize/databasa/internal/hnsw"
	"github.com/futlize/databasa/internal/resources"
	"github.com/futlize/databasa/internal/storage"
	pb "github.com/futlize/databasa/pkg/pb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	defaultShardCount          = 8
	defaultIndexFlushOps       = 4096
	defaultCompression         = "int8"
	defaultMaxTopK             = 256
	defaultMaxBatchSize        = 1000
	defaultMaxEfSearch         = 4096
	defaultMaxCollectionDim    = 8192
	defaultMaxConcurrentSearch = 64
	defaultMaxConcurrentWrite  = 128

	indexWorkerBatchSize = 256
	indexWorkerBatchWait = 2 * time.Millisecond

	bulkLoadRebuildIdle = 2 * time.Second
	bulkLoadRebuildTick = 1 * time.Second
)

type GuardrailConfig struct {
	MaxTopK             uint32
	MaxBatchSize        int
	MaxEfSearch         uint32
	MaxCollectionDim    uint32
	MaxConcurrentSearch int
	MaxConcurrentWrite  int
	MaxDataDirBytes     int64
	RequireRPCDeadline  bool
}

type shard struct {
	// mu guards shard state updates. In the legacy path, writes hold this lock while
	// also mutating HNSW, which amplifies contention under concurrent inserts.
	mu            sync.Mutex
	saveMu        sync.Mutex
	id            int
	dir           string
	store         *storage.VectorStore
	index         *hnsw.Index
	dirtyIndexOps int
	indexQueue    *indexMutationQueue
	indexDone     chan struct{}
}

type indexMutation struct {
	hasOld bool
	oldID  uint64
	hasNew bool
	newID  uint64
}

// indexMutationQueue is a bounded in-memory queue.
// Producers block when the queue is full to apply backpressure.
type indexMutationQueue struct {
	mu       sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond
	items    []indexMutation
	capacity int
	closed   bool
}

func newIndexMutationQueue(capacity int) *indexMutationQueue {
	if capacity <= 0 {
		capacity = resources.DefaultConfig().InsertQueueSize
	}
	q := &indexMutationQueue{
		capacity: capacity,
		items:    make([]indexMutation, 0, capacity),
	}
	q.notEmpty = sync.NewCond(&q.mu)
	q.notFull = sync.NewCond(&q.mu)
	return q
}

func (q *indexMutationQueue) Enqueue(item indexMutation) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.items) >= q.capacity && !q.closed {
		q.notFull.Wait()
	}
	if q.closed {
		return false
	}
	q.items = append(q.items, item)
	q.notEmpty.Signal()
	return true
}

func (q *indexMutationQueue) PopBatch(max int) ([]indexMutation, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.items) == 0 && !q.closed {
		q.notEmpty.Wait()
	}
	if len(q.items) == 0 && q.closed {
		return nil, false
	}

	n := max
	if n <= 0 || n > len(q.items) {
		n = len(q.items)
	}
	batch := make([]indexMutation, n)
	copy(batch, q.items[:n])
	q.items = q.items[n:]
	q.notFull.Broadcast()
	return batch, true
}

func (q *indexMutationQueue) Drain(max int) []indexMutation {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 || max <= 0 {
		return nil
	}
	n := max
	if n > len(q.items) {
		n = len(q.items)
	}
	batch := make([]indexMutation, n)
	copy(batch, q.items[:n])
	q.items = q.items[n:]
	q.notFull.Broadcast()
	return batch
}

func (q *indexMutationQueue) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.notEmpty.Broadcast()
	q.notFull.Broadcast()
}

func (q *indexMutationQueue) IsClosed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}

type collection struct {
	name        string
	dimension   uint32
	metric      string
	compression string
	indexConfig hnsw.Config
	shards      []*shard
}

func (c *collection) shardForKey(key string) *shard {
	if len(c.shards) == 0 {
		return nil
	}
	if len(c.shards) == 1 {
		return c.shards[0]
	}

	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	idx := int(h.Sum32() % uint32(len(c.shards)))
	return c.shards[idx]
}

func (c *collection) hasIndex() bool {
	if len(c.shards) == 0 {
		return false
	}
	for _, sh := range c.shards {
		if sh.index == nil {
			return false
		}
	}
	return true
}

func (c *collection) count() uint64 {
	var total uint64
	for _, sh := range c.shards {
		total += sh.store.Count()
	}
	return total
}

// Server implements the Databasa gRPC service.
type Server struct {
	pb.UnimplementedDatabasaServer
	mu            sync.RWMutex
	dataDir       string
	collections   map[string]*collection
	defaultShards int
	indexFlushOps int
	compression   string

	maxTopK            uint32
	maxBatchSize       int
	maxEfSearch        uint32
	maxCollectionDim   uint32
	maxDataDirBytes    int64
	requireRPCDeadline bool
	searchLimiter      chan struct{}
	writeLimiter       chan struct{}

	resourceManager *resources.Manager
	insertQueueSize int
	bulkLoadMode    bool

	bulkMu     sync.Mutex
	bulkDirty  map[string]time.Time
	bulkWakeCh chan struct{}
	bulkStopCh chan struct{}
	bulkDoneCh chan struct{}
	closeOnce  sync.Once
}

// New creates a server with optimized defaults for large scale.
func New(dataDir string) (*Server, error) {
	return NewWithConfig(dataDir, defaultShardCount, defaultIndexFlushOps, defaultCompression)
}

// NewWithConfig creates a server with explicit sharding/persistence/compression knobs.
func NewWithConfig(dataDir string, defaultShards, indexFlushOps int, compression string) (*Server, error) {
	rtCfg := currentRuntimeConfig()

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	if defaultShards <= 0 {
		defaultShards = 1
	}
	if indexFlushOps < 0 {
		indexFlushOps = defaultIndexFlushOps
	}
	switch compression {
	case "none", "int8":
	default:
		compression = defaultCompression
	}

	s := &Server{
		dataDir:         dataDir,
		collections:     make(map[string]*collection),
		defaultShards:   defaultShards,
		indexFlushOps:   indexFlushOps,
		compression:     compression,
		resourceManager: resources.Global(),
		insertQueueSize: rtCfg.InsertQueueSize,
		bulkLoadMode:    rtCfg.BulkLoadMode,
		bulkDirty:       make(map[string]time.Time),
		bulkWakeCh:      make(chan struct{}, 1),
		bulkStopCh:      make(chan struct{}),
		bulkDoneCh:      make(chan struct{}),
	}
	s.configureGuardrailsLocked(GuardrailConfig{
		MaxTopK:             defaultMaxTopK,
		MaxBatchSize:        defaultMaxBatchSize,
		MaxEfSearch:         defaultMaxEfSearch,
		MaxCollectionDim:    defaultMaxCollectionDim,
		MaxConcurrentSearch: defaultMaxConcurrentSearch,
		MaxConcurrentWrite:  defaultMaxConcurrentWrite,
	})

	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, fmt.Errorf("read data dir: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		col, err := s.loadCollection(name)
		if err != nil {
			return nil, fmt.Errorf("load collection %s: %w", name, err)
		}
		if col != nil {
			s.collections[name] = col
		}
	}

	if s.bulkLoadMode {
		go s.bulkRebuildLoop()
	} else {
		close(s.bulkDoneCh)
	}

	return s, nil
}

func (s *Server) ConfigureGuardrails(cfg GuardrailConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configureGuardrailsLocked(cfg)
}

func (s *Server) configureGuardrailsLocked(cfg GuardrailConfig) {
	if cfg.MaxTopK == 0 {
		cfg.MaxTopK = defaultMaxTopK
	}
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = defaultMaxBatchSize
	}
	if cfg.MaxEfSearch == 0 {
		cfg.MaxEfSearch = defaultMaxEfSearch
	}
	if cfg.MaxCollectionDim == 0 {
		cfg.MaxCollectionDim = defaultMaxCollectionDim
	}
	if cfg.MaxConcurrentSearch <= 0 {
		cfg.MaxConcurrentSearch = defaultMaxConcurrentSearch
	}
	if cfg.MaxConcurrentWrite <= 0 {
		cfg.MaxConcurrentWrite = defaultMaxConcurrentWrite
	}
	if cfg.MaxDataDirBytes < 0 {
		cfg.MaxDataDirBytes = 0
	}

	s.maxTopK = cfg.MaxTopK
	s.maxBatchSize = cfg.MaxBatchSize
	s.maxEfSearch = cfg.MaxEfSearch
	s.maxCollectionDim = cfg.MaxCollectionDim
	s.maxDataDirBytes = cfg.MaxDataDirBytes
	s.requireRPCDeadline = cfg.RequireRPCDeadline
	s.searchLimiter = make(chan struct{}, cfg.MaxConcurrentSearch)
	s.writeLimiter = make(chan struct{}, cfg.MaxConcurrentWrite)
}

func (s *Server) validateContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if s.requireRPCDeadline {
		if _, ok := ctx.Deadline(); !ok {
			return status.Error(codes.InvalidArgument, "rpc deadline is required")
		}
	}
	return nil
}

func (s *Server) enforceDataDirLimit() error {
	if s.maxDataDirBytes <= 0 {
		return nil
	}
	current, err := dirSizeBytes(s.dataDir)
	if err != nil {
		return status.Errorf(codes.Internal, "check data dir size: %v", err)
	}
	if current >= s.maxDataDirBytes {
		return status.Errorf(codes.ResourceExhausted, "data directory size limit reached: current=%dB limit=%dB", current, s.maxDataDirBytes)
	}
	return nil
}

func (s *Server) tryAcquireSearchSlot() error {
	select {
	case s.searchLimiter <- struct{}{}:
		return nil
	default:
		return status.Error(codes.ResourceExhausted, "too many concurrent search requests")
	}
}

func (s *Server) releaseSearchSlot() {
	<-s.searchLimiter
}

func (s *Server) tryAcquireWriteSlot() error {
	select {
	case s.writeLimiter <- struct{}{}:
		return nil
	default:
		return status.Error(codes.ResourceExhausted, "too many concurrent write requests")
	}
}

func (s *Server) releaseWriteSlot() {
	<-s.writeLimiter
}

func validateEmbeddingValues(embedding []float32) error {
	for i, v := range embedding {
		value := float64(v)
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return status.Errorf(codes.InvalidArgument, "embedding contains NaN/Inf at index %d", i)
		}
	}
	return nil
}

func normalizeQueryForMetric(metric string, embedding []float32) []float32 {
	if metric != "cosine" || len(embedding) == 0 {
		return embedding
	}

	out := make([]float32, len(embedding))
	copy(out, embedding)

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

func (s *Server) loadCollection(name string) (*collection, error) {
	colDir := filepath.Join(s.dataDir, name)

	meta, err := loadCollectionMeta(colDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	col := &collection{
		name:        meta.Name,
		dimension:   meta.Dimension,
		metric:      meta.Metric,
		compression: meta.Compression,
		shards:      make([]*shard, 0, meta.ShardCount),
	}
	for i := 0; i < int(meta.ShardCount); i++ {
		shardDir := shardDir(colDir, i)
		store, err := storage.OpenWithCompression(shardDir, name, meta.Dimension, meta.Metric, meta.Compression)
		if err != nil {
			return nil, fmt.Errorf("open shard %d: %w", i, err)
		}
		sh := &shard{id: i, dir: shardDir, store: store}
		if err := s.tryLoadShardIndex(sh, meta.Metric); err != nil {
			_ = store.Close()
			return nil, err
		}
		if sh.index != nil && col.indexConfig.M == 0 {
			col.indexConfig = sh.index.Config()
		}
		s.initShardIndexer(sh)
		col.shards = append(col.shards, sh)
	}

	return col, nil
}

func (s *Server) tryLoadShardIndex(sh *shard, metric string) error {
	graphPath := filepath.Join(sh.dir, "hnsw.graph")
	if _, err := os.Stat(graphPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	distFunc := getDistanceFunc(metric)
	idx, err := hnsw.Load(sh.dir, distFunc, sh.store.GetByID)
	if err != nil {
		return fmt.Errorf("load index shard %d: %w", sh.id, err)
	}
	idx.SetQueryDistanceFactory(getDistanceQueryFactory(metric))

	// Recovery reconciliation:
	// storage may have acknowledged WAL-backed writes that were not yet indexed
	// before a crash. Re-inserting active IDs is idempotent and catches those gaps.
	for _, id := range sh.store.AllIDs() {
		_ = idx.Insert(id)
	}

	sh.index = idx
	return nil
}

func (s *Server) initShardIndexer(sh *shard) {
	if sh.indexQueue != nil {
		return
	}
	sh.indexQueue = newIndexMutationQueue(s.insertQueueSize)
	sh.indexDone = make(chan struct{})
	go s.runShardIndexer(sh)
}

func (s *Server) stopShardIndexer(sh *shard) {
	if sh.indexQueue == nil {
		return
	}
	sh.indexQueue.Close()
	<-sh.indexDone
	sh.indexQueue = nil
	sh.indexDone = nil
}

func (s *Server) enqueueIndexMutations(sh *shard, muts []indexMutation) {
	if len(muts) == 0 {
		return
	}

	sh.mu.Lock()
	idx := sh.index
	q := sh.indexQueue
	sh.mu.Unlock()
	if idx == nil || q == nil {
		return
	}

	for _, mut := range muts {
		if err := s.resourceManager.WaitIndexingAllowance(context.Background()); err != nil {
			return
		}
		if !q.Enqueue(mut) {
			return
		}
	}
}

func (s *Server) runShardIndexer(sh *shard) {
	defer close(sh.indexDone)
	for {
		batch, ok := sh.indexQueue.PopBatch(indexWorkerBatchSize)
		if !ok {
			return
		}

		// Short coalescing window to turn many tiny writes into fewer index updates.
		time.Sleep(indexWorkerBatchWait)
		if len(batch) < indexWorkerBatchSize {
			extra := sh.indexQueue.Drain(indexWorkerBatchSize - len(batch))
			if len(extra) > 0 {
				batch = append(batch, extra...)
			}
		}

		if err := s.resourceManager.WaitIndexingAllowance(context.Background()); err != nil {
			continue
		}

		acquired := false
		for !acquired {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			err := s.resourceManager.AcquireWorker(ctx, "indexing")
			cancel()
			if err == nil {
				acquired = true
				break
			}
			if sh.indexQueue.IsClosed() {
				return
			}
		}
		s.applyIndexMutations(sh, batch)
		s.resourceManager.ReleaseWorker()
	}
}

func (s *Server) applyIndexMutations(sh *shard, batch []indexMutation) {
	if len(batch) == 0 {
		return
	}

	sh.mu.Lock()
	idx := sh.index
	sh.mu.Unlock()
	if idx == nil {
		return
	}

	appliedOps := 0
	for _, mut := range batch {
		if mut.hasOld {
			idx.Remove(mut.oldID)
			appliedOps++
		}
		if mut.hasNew {
			if err := idx.Insert(mut.newID); err == nil {
				appliedOps++
			}
		}
	}
	if appliedOps == 0 {
		return
	}

	sh.mu.Lock()
	sh.dirtyIndexOps += appliedOps
	_ = s.persistShardIndexLocked(sh, false)
	sh.mu.Unlock()
}

// Close closes all collections.
func (s *Server) Close() {
	s.closeOnce.Do(func() {
		close(s.bulkStopCh)
		<-s.bulkDoneCh

		s.mu.Lock()
		shards := make([]*shard, 0)
		for _, col := range s.collections {
			shards = append(shards, col.shards...)
		}
		s.mu.Unlock()

		for _, sh := range shards {
			s.stopShardIndexer(sh)
			sh.mu.Lock()
			_ = s.persistShardIndexLocked(sh, true)
			sh.mu.Unlock()
			_ = sh.store.Close()
		}
	})
}

func (s *Server) markCollectionIndexDirty(name string) {
	if !s.bulkLoadMode {
		return
	}
	s.bulkMu.Lock()
	s.bulkDirty[name] = time.Now()
	s.bulkMu.Unlock()
	select {
	case s.bulkWakeCh <- struct{}{}:
	default:
	}
}

func (s *Server) bulkRebuildLoop() {
	defer close(s.bulkDoneCh)

	ticker := time.NewTicker(bulkLoadRebuildTick)
	defer ticker.Stop()

	for {
		select {
		case <-s.bulkStopCh:
			return
		case <-ticker.C:
		case <-s.bulkWakeCh:
		}

		now := time.Now()
		type rebuildJob struct {
			collection string
			lastWrite  time.Time
		}
		jobs := make([]rebuildJob, 0)

		s.bulkMu.Lock()
		for name, ts := range s.bulkDirty {
			if now.Sub(ts) >= bulkLoadRebuildIdle {
				jobs = append(jobs, rebuildJob{collection: name, lastWrite: ts})
			}
		}
		s.bulkMu.Unlock()

		for _, job := range jobs {
			if err := s.rebuildCollectionIndex(job.collection); err != nil {
				log.Printf("WARN bulk-load: rebuild failed collection=%s err=%v", job.collection, err)
				continue
			}

			s.bulkMu.Lock()
			current, exists := s.bulkDirty[job.collection]
			if exists && !current.After(job.lastWrite) {
				delete(s.bulkDirty, job.collection)
			}
			s.bulkMu.Unlock()
		}
	}
}

func (s *Server) rebuildCollectionIndex(collectionName string) error {
	if err := s.resourceManager.AcquireWorker(context.Background(), "bulk_rebuild_index"); err != nil {
		return err
	}
	defer s.resourceManager.ReleaseWorker()

	s.mu.Lock()
	defer s.mu.Unlock()

	col, exists := s.collections[collectionName]
	if !exists {
		return nil
	}
	if !col.hasIndex() {
		return nil
	}

	cfg := col.indexConfig
	if cfg.M == 0 {
		cfg = hnsw.DefaultConfig()
	}
	if err := s.buildCollectionIndexLocked(context.Background(), col, cfg); err != nil {
		return err
	}
	col.indexConfig = cfg
	return nil
}

// CreateCollection creates a sharded collection.
func (s *Server) CreateCollection(ctx context.Context, req *pb.CreateCollectionRequest) (*pb.CreateCollectionResponse, error) {
	if err := s.validateContext(ctx); err != nil {
		return nil, err
	}
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "collection name is required")
	}
	if req.Dimension == 0 {
		return nil, status.Error(codes.InvalidArgument, "dimension must be > 0")
	}
	if req.Dimension > s.maxCollectionDim {
		return nil, status.Errorf(codes.InvalidArgument, "dimension must be <= %d", s.maxCollectionDim)
	}
	if err := s.enforceDataDirLimit(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.collections[req.Name]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "collection %q already exists", req.Name)
	}

	metric := metricToString(req.Metric)
	colDir := filepath.Join(s.dataDir, req.Name)
	if err := os.MkdirAll(colDir, 0o755); err != nil {
		return nil, status.Errorf(codes.Internal, "create collection dir: %v", err)
	}

	meta := collectionMeta{
		Name:        req.Name,
		Dimension:   req.Dimension,
		Metric:      metric,
		Compression: s.compression,
		ShardCount:  uint32(s.defaultShards),
	}
	if err := saveCollectionMeta(colDir, meta); err != nil {
		return nil, status.Errorf(codes.Internal, "save collection meta: %v", err)
	}

	col := &collection{
		name:        req.Name,
		dimension:   req.Dimension,
		metric:      metric,
		compression: s.compression,
		shards:      make([]*shard, 0, s.defaultShards),
	}

	for i := 0; i < s.defaultShards; i++ {
		shardDir := shardDir(colDir, i)
		store, err := storage.OpenWithCompression(shardDir, req.Name, req.Dimension, metric, s.compression)
		if err != nil {
			for _, existing := range col.shards {
				s.stopShardIndexer(existing)
				_ = existing.store.Close()
			}
			_ = os.RemoveAll(colDir)
			return nil, status.Errorf(codes.Internal, "create shard %d: %v", i, err)
		}
		sh := &shard{
			id:    i,
			dir:   shardDir,
			store: store,
		}
		s.initShardIndexer(sh)
		col.shards = append(col.shards, sh)
	}

	s.collections[req.Name] = col
	return &pb.CreateCollectionResponse{}, nil
}

func (s *Server) DeleteCollection(ctx context.Context, req *pb.DeleteCollectionRequest) (*pb.DeleteCollectionResponse, error) {
	if err := s.validateContext(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	col, exists := s.collections[req.Name]
	if !exists {
		return nil, status.Errorf(codes.NotFound, "collection %q not found", req.Name)
	}

	for _, sh := range col.shards {
		s.stopShardIndexer(sh)
		sh.mu.Lock()
		_ = s.persistShardIndexLocked(sh, true)
		sh.mu.Unlock()
		_ = sh.store.Close()
	}

	delete(s.collections, req.Name)
	s.bulkMu.Lock()
	delete(s.bulkDirty, req.Name)
	s.bulkMu.Unlock()
	if err := os.RemoveAll(filepath.Join(s.dataDir, req.Name)); err != nil {
		return nil, status.Errorf(codes.Internal, "remove collection data: %v", err)
	}

	return &pb.DeleteCollectionResponse{}, nil
}

func (s *Server) ListCollections(ctx context.Context, req *pb.ListCollectionsRequest) (*pb.ListCollectionsResponse, error) {
	if err := s.validateContext(ctx); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	infos := make([]*pb.CollectionInfo, 0, len(s.collections))
	for name, col := range s.collections {
		infos = append(infos, &pb.CollectionInfo{
			Name:      name,
			Dimension: col.dimension,
			Metric:    stringToMetric(col.metric),
			Count:     col.count(),
			HasIndex:  col.hasIndex(),
		})
	}
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].Name < infos[j].Name
	})

	return &pb.ListCollectionsResponse{Collections: infos}, nil
}

func (s *Server) buildCollectionIndexLocked(ctx context.Context, col *collection, cfg hnsw.Config) error {
	distFunc := getDistanceFunc(col.metric)
	queryFactory := getDistanceQueryFactory(col.metric)

	for _, sh := range col.shards {
		if err := s.resourceManager.WaitIndexingAllowance(ctx); err != nil {
			return err
		}

		err := func() error {
			ids := sh.store.AllIDs()
			if err := s.resourceManager.WaitForMemory(ctx, uint64(len(ids))*8, "index_build_snapshot"); err != nil {
				return err
			}

			idx := hnsw.New(cfg, distFunc, sh.store.GetByID)
			idx.SetQueryDistanceFactory(queryFactory)
			for _, id := range ids {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := idx.Insert(id); err != nil {
					return fmt.Errorf("index shard %d vector %d: %w", sh.id, id, err)
				}
			}

			sh.mu.Lock()
			defer sh.mu.Unlock()
			sh.index = idx
			sh.dirtyIndexOps = 0
			if err := s.enforceDataDirLimit(); err != nil {
				return err
			}
			if err := s.persistShardIndexLocked(sh, true); err != nil {
				return fmt.Errorf("save index shard %d: %w", sh.id, err)
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) CreateIndex(ctx context.Context, req *pb.CreateIndexRequest) (*pb.CreateIndexResponse, error) {
	if err := s.validateContext(ctx); err != nil {
		return nil, err
	}
	if err := s.enforceDataDirLimit(); err != nil {
		return nil, err
	}
	if err := s.resourceManager.AcquireWorker(ctx, "create_index"); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	defer s.resourceManager.ReleaseWorker()
	s.mu.Lock()
	defer s.mu.Unlock()

	col, exists := s.collections[req.Collection]
	if !exists {
		return nil, status.Errorf(codes.NotFound, "collection %q not found", req.Collection)
	}
	if col.hasIndex() {
		return nil, status.Errorf(codes.AlreadyExists, "index already exists for %q", req.Collection)
	}

	cfg := hnsw.DefaultConfig()
	if req.M > 0 {
		cfg.M = int(req.M)
		cfg.MMax0 = cfg.M * 2
	}
	if req.EfConstruction > 0 {
		cfg.EfConstruction = int(req.EfConstruction)
	}

	if err := s.buildCollectionIndexLocked(ctx, col, cfg); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		if st, ok := status.FromError(err); ok {
			return nil, st.Err()
		}
		return nil, status.Errorf(codes.Internal, "create index for %q: %v", req.Collection, err)
	}
	col.indexConfig = cfg

	return &pb.CreateIndexResponse{}, nil
}

func (s *Server) DropIndex(ctx context.Context, req *pb.DropIndexRequest) (*pb.DropIndexResponse, error) {
	if err := s.validateContext(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	col, exists := s.collections[req.Collection]
	if !exists {
		return nil, status.Errorf(codes.NotFound, "collection %q not found", req.Collection)
	}

	for _, sh := range col.shards {
		sh.mu.Lock()
		sh.index = nil
		sh.dirtyIndexOps = 0
		sh.mu.Unlock()
		_ = os.Remove(filepath.Join(sh.dir, "hnsw.graph"))
	}
	col.indexConfig = hnsw.Config{}
	s.bulkMu.Lock()
	delete(s.bulkDirty, req.Collection)
	s.bulkMu.Unlock()

	return &pb.DropIndexResponse{}, nil
}

func (s *Server) Insert(ctx context.Context, req *pb.InsertRequest) (*pb.InsertResponse, error) {
	if err := s.validateContext(ctx); err != nil {
		return nil, err
	}
	if err := s.tryAcquireWriteSlot(); err != nil {
		return nil, err
	}
	defer s.releaseWriteSlot()
	if err := s.resourceManager.AcquireWorker(ctx, "insert"); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	defer s.resourceManager.ReleaseWorker()
	if err := s.enforceDataDirLimit(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	col, exists := s.collections[req.Collection]
	if !exists {
		s.mu.RUnlock()
		return nil, status.Errorf(codes.NotFound, "collection %q not found", req.Collection)
	}
	defer s.mu.RUnlock()
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	if uint32(len(req.Embedding)) != col.dimension {
		return nil, status.Errorf(codes.InvalidArgument, "embedding dimension mismatch: got=%d want=%d", len(req.Embedding), col.dimension)
	}
	if err := validateEmbeddingValues(req.Embedding); err != nil {
		return nil, err
	}
	if err := s.resourceManager.WaitInsertAllowance(ctx); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if err := s.resourceManager.WaitForMemory(ctx, uint64(len(req.Embedding))*8, "insert"); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	sh := col.shardForKey(req.Key)
	if sh == nil {
		return nil, status.Errorf(codes.Internal, "no shard available")
	}

	oldID, hadOld := sh.store.GetID(req.Key)
	id, err := sh.store.Insert(req.Key, req.Embedding)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "insert: %v", err)
	}

	if s.bulkLoadMode && col.hasIndex() {
		s.markCollectionIndexDirty(req.Collection)
	} else {
		// New write path:
		// index maintenance is queued and applied by a background worker in batches.
		// Insert acknowledgement no longer waits for HNSW mutation or index fsync.
		s.enqueueIndexMutations(sh, []indexMutation{{
			hasOld: hadOld,
			oldID:  oldID,
			hasNew: true,
			newID:  id,
		}})
	}

	return &pb.InsertResponse{}, nil
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if err := s.validateContext(ctx); err != nil {
		return nil, err
	}
	s.mu.RLock()
	col, exists := s.collections[req.Collection]
	if !exists {
		s.mu.RUnlock()
		return nil, status.Errorf(codes.NotFound, "collection %q not found", req.Collection)
	}
	defer s.mu.RUnlock()
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	sh := col.shardForKey(req.Key)
	if sh == nil {
		return nil, status.Errorf(codes.Internal, "no shard available")
	}

	embedding, err := sh.store.GetByKey(req.Key)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "key not found: %v", err)
	}

	return &pb.GetResponse{
		Key:       req.Key,
		Embedding: embedding,
	}, nil
}

func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if err := s.validateContext(ctx); err != nil {
		return nil, err
	}
	if err := s.tryAcquireWriteSlot(); err != nil {
		return nil, err
	}
	defer s.releaseWriteSlot()
	if err := s.resourceManager.AcquireWorker(ctx, "delete"); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	defer s.resourceManager.ReleaseWorker()

	s.mu.RLock()
	col, exists := s.collections[req.Collection]
	if !exists {
		s.mu.RUnlock()
		return nil, status.Errorf(codes.NotFound, "collection %q not found", req.Collection)
	}
	defer s.mu.RUnlock()
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	sh := col.shardForKey(req.Key)
	if sh == nil {
		return nil, status.Errorf(codes.Internal, "no shard available")
	}

	id, hadID := sh.store.GetID(req.Key)
	if err := sh.store.Delete(req.Key); err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}

	if hadID && !(s.bulkLoadMode && col.hasIndex()) {
		s.enqueueIndexMutations(sh, []indexMutation{{
			hasOld: true,
			oldID:  id,
		}})
	} else if hadID && s.bulkLoadMode && col.hasIndex() {
		s.markCollectionIndexDirty(req.Collection)
	}

	return &pb.DeleteResponse{}, nil
}

func (s *Server) BatchInsert(ctx context.Context, req *pb.BatchInsertRequest) (*pb.BatchInsertResponse, error) {
	if err := s.validateContext(ctx); err != nil {
		return nil, err
	}
	if err := s.tryAcquireWriteSlot(); err != nil {
		return nil, err
	}
	defer s.releaseWriteSlot()
	if err := s.resourceManager.AcquireWorker(ctx, "batch_insert"); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	defer s.resourceManager.ReleaseWorker()
	if err := s.enforceDataDirLimit(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	col, exists := s.collections[req.Collection]
	if !exists {
		s.mu.RUnlock()
		return nil, status.Errorf(codes.NotFound, "collection %q not found", req.Collection)
	}
	defer s.mu.RUnlock()
	if len(req.Items) == 0 {
		return nil, status.Error(codes.InvalidArgument, "batch items cannot be empty")
	}
	if len(req.Items) > s.maxBatchSize {
		return nil, status.Errorf(codes.InvalidArgument, "batch size exceeds max_batch_size=%d", s.maxBatchSize)
	}
	if err := s.resourceManager.WaitInsertAllowance(ctx); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	grouped := make(map[*shard][]storage.BatchInsertItem, len(col.shards))
	var reserveBytes uint64
	for _, item := range req.Items {
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}
		if item.Key == "" {
			return nil, status.Error(codes.InvalidArgument, "batch item key is required")
		}
		if uint32(len(item.Embedding)) != col.dimension {
			return nil, status.Errorf(codes.InvalidArgument, "batch item %q embedding dimension mismatch: got=%d want=%d", item.Key, len(item.Embedding), col.dimension)
		}
		if err := validateEmbeddingValues(item.Embedding); err != nil {
			return nil, err
		}
		if err := s.enforceDataDirLimit(); err != nil {
			return nil, err
		}
		reserveBytes += uint64(len(item.Embedding)) * 8

		sh := col.shardForKey(item.Key)
		if sh == nil {
			return nil, status.Errorf(codes.Internal, "no shard available")
		}
		grouped[sh] = append(grouped[sh], storage.BatchInsertItem{
			Key:       item.Key,
			Embedding: item.Embedding,
		})
	}
	if err := s.resourceManager.WaitForMemory(ctx, reserveBytes, "batch_insert"); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	var inserted uint64
	for sh, shardItems := range grouped {
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}

		results, err := sh.store.BatchInsert(shardItems)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "batch insert shard %d: %v", sh.id, err)
		}
		inserted += uint64(len(results))

		if len(results) > 0 && !(s.bulkLoadMode && col.hasIndex()) {
			muts := make([]indexMutation, 0, len(results))
			for _, result := range results {
				muts = append(muts, indexMutation{
					hasOld: result.HadOld,
					oldID:  result.OldID,
					hasNew: true,
					newID:  result.ID,
				})
			}
			s.enqueueIndexMutations(sh, muts)
		} else if len(results) > 0 && s.bulkLoadMode && col.hasIndex() {
			s.markCollectionIndexDirty(req.Collection)
		}
	}

	return &pb.BatchInsertResponse{Inserted: inserted}, nil
}

func (s *Server) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchResponse, error) {
	if err := s.validateContext(ctx); err != nil {
		return nil, err
	}
	if err := s.tryAcquireSearchSlot(); err != nil {
		return nil, err
	}
	defer s.releaseSearchSlot()

	s.mu.RLock()
	col, exists := s.collections[req.Collection]
	if !exists {
		s.mu.RUnlock()
		return nil, status.Errorf(codes.NotFound, "collection %q not found", req.Collection)
	}
	defer s.mu.RUnlock()
	if req.TopK == 0 {
		return nil, status.Error(codes.InvalidArgument, "top_k must be > 0")
	}
	if req.TopK > s.maxTopK {
		return nil, status.Errorf(codes.InvalidArgument, "top_k must be <= %d", s.maxTopK)
	}
	if req.EfSearch > s.maxEfSearch {
		return nil, status.Errorf(codes.InvalidArgument, "ef_search must be <= %d", s.maxEfSearch)
	}
	if uint32(len(req.Embedding)) != col.dimension {
		return nil, status.Errorf(codes.InvalidArgument, "embedding dimension mismatch: got=%d want=%d", len(req.Embedding), col.dimension)
	}
	if err := validateEmbeddingValues(req.Embedding); err != nil {
		return nil, err
	}
	includeEmbedding := shouldIncludeEmbedding(ctx, req)
	queryEmbedding := normalizeQueryForMetric(col.metric, req.Embedding)

	topK := int(req.TopK)
	var (
		results []scoredResult
		err     error
	)
	if col.hasIndex() {
		results, err = s.searchIndexedDistributed(ctx, col, queryEmbedding, topK, int(req.EfSearch))
	} else {
		results, err = s.searchBruteForceDistributed(ctx, col, queryEmbedding, topK)
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		return nil, status.Errorf(codes.Internal, "search: %v", err)
	}

	pbResults := make([]*pb.SearchResult, len(results))
	for i, r := range results {
		result := &pb.SearchResult{
			Key:   r.key,
			Score: r.dist,
		}
		if includeEmbedding {
			sh := col.shardForKey(r.key)
			if sh != nil {
				if embedding, err := sh.store.GetByKey(r.key); err == nil {
					setSearchResultEmbedding(result, embedding)
				}
			}
		}
		pbResults[i] = result
	}

	return &pb.SearchResponse{Results: pbResults}, nil
}

func shouldIncludeEmbedding(ctx context.Context, req *pb.SearchRequest) bool {
	if requestIncludesEmbeddingByField(req) {
		return true
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}

	for _, key := range []string{"x-return-embedding", "x-include-embedding"} {
		if slices.ContainsFunc(md.Get(key), parseTrueLikeBool) {
			return true
		}
	}
	return false
}

func requestIncludesEmbeddingByField(req *pb.SearchRequest) bool {
	if req == nil {
		return false
	}

	raw := req.ProtoReflect().GetUnknown()
	for len(raw) > 0 {
		fieldNum, wireType, n := protowire.ConsumeTag(raw)
		if n < 0 {
			return false
		}
		raw = raw[n:]

		if fieldNum == 5 && wireType == protowire.VarintType {
			v, m := protowire.ConsumeVarint(raw)
			if m < 0 {
				return false
			}
			return v != 0
		}

		m := protowire.ConsumeFieldValue(fieldNum, wireType, raw)
		if m < 0 {
			return false
		}
		raw = raw[m:]
	}
	return false
}

func parseTrueLikeBool(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func setSearchResultEmbedding(result *pb.SearchResult, embedding []float32) {
	if result == nil || len(embedding) == 0 {
		return
	}

	packed := make([]byte, len(embedding)*4)
	for i, v := range embedding {
		binary.LittleEndian.PutUint32(packed[i*4:], math.Float32bits(v))
	}

	field := protowire.AppendTag(nil, 3, protowire.BytesType)
	field = protowire.AppendBytes(field, packed)

	msg := result.ProtoReflect()
	msg.SetUnknown(append(msg.GetUnknown(), field...))
}

type scoredResult struct {
	key  string
	dist float32
}

type shardSearchOutput struct {
	items []scoredResult
	err   error
}

func (s *Server) searchIndexedDistributed(ctx context.Context, col *collection, query []float32, topK int, efSearch int) ([]scoredResult, error) {
	candidateK := topK * 2
	if candidateK < 32 {
		candidateK = 32
	}

	out := make(chan shardSearchOutput, len(col.shards))
	for _, shardRef := range col.shards {
		sh := shardRef
		go func() {
			if err := s.resourceManager.AcquireWorker(ctx, "search_shard"); err != nil {
				out <- shardSearchOutput{err: err}
				return
			}
			defer s.resourceManager.ReleaseWorker()

			if err := ctx.Err(); err != nil {
				out <- shardSearchOutput{err: err}
				return
			}
			if sh.index == nil {
				items, err := s.searchShardBruteForce(ctx, sh, query, topK)
				out <- shardSearchOutput{items: items, err: err}
				return
			}

			indexResults, err := sh.index.Search(query, candidateK, efSearch)
			if err != nil {
				out <- shardSearchOutput{err: err}
				return
			}

			ids := make([]uint64, 0, len(indexResults))
			for _, r := range indexResults {
				ids = append(ids, r.ID)
			}
			resolved := sh.store.ResolveActiveKeys(ids)

			items := make([]scoredResult, 0, len(resolved))
			for _, r := range indexResults {
				if err := ctx.Err(); err != nil {
					out <- shardSearchOutput{err: err}
					return
				}
				key, ok := resolved[r.ID]
				if !ok {
					continue
				}
				items = append(items, scoredResult{key: key, dist: r.Distance})
			}

			// Indexing is asynchronous, so recent writes may not be visible in HNSW yet.
			// Fallback keeps search complete by reading shard state directly when needed.
			if len(items) < topK {
				bruteItems, err := s.searchShardBruteForce(ctx, sh, query, topK)
				if err != nil {
					out <- shardSearchOutput{err: err}
					return
				}
				if len(bruteItems) > 0 {
					byKey := make(map[string]float32, len(items)+len(bruteItems))
					for _, item := range items {
						byKey[item.key] = item.dist
					}
					for _, item := range bruteItems {
						if existing, ok := byKey[item.key]; ok {
							if item.dist < existing {
								byKey[item.key] = item.dist
							}
							continue
						}
						byKey[item.key] = item.dist
					}
					merged := make([]scoredResult, 0, len(byKey))
					for key, dist := range byKey {
						merged = append(merged, scoredResult{key: key, dist: dist})
					}
					sort.Slice(merged, func(i, j int) bool {
						if merged[i].dist == merged[j].dist {
							return merged[i].key < merged[j].key
						}
						return merged[i].dist < merged[j].dist
					})
					if len(merged) > topK {
						merged = merged[:topK]
					}
					items = merged
				}
			}
			out <- shardSearchOutput{items: items}
		}()
	}

	merged := &scoredMaxHeap{}
	heap.Init(merged)

	for i := 0; i < len(col.shards); i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case res := <-out:
			if res.err != nil {
				return nil, res.err
			}
			for _, item := range res.items {
				pushTopK(merged, item, topK)
			}
		}
	}
	return heapToAscending(merged), nil
}

func (s *Server) searchBruteForceDistributed(ctx context.Context, col *collection, query []float32, topK int) ([]scoredResult, error) {
	out := make(chan shardSearchOutput, len(col.shards))
	for _, shardRef := range col.shards {
		sh := shardRef
		go func() {
			if err := s.resourceManager.AcquireWorker(ctx, "search_bruteforce_shard"); err != nil {
				out <- shardSearchOutput{err: err}
				return
			}
			defer s.resourceManager.ReleaseWorker()

			items, err := s.searchShardBruteForce(ctx, sh, query, topK)
			out <- shardSearchOutput{items: items, err: err}
		}()
	}

	merged := &scoredMaxHeap{}
	heap.Init(merged)

	for i := 0; i < len(col.shards); i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case res := <-out:
			if res.err != nil {
				return nil, res.err
			}
			for _, item := range res.items {
				pushTopK(merged, item, topK)
			}
		}
	}
	return heapToAscending(merged), nil
}

func (s *Server) searchShardBruteForce(ctx context.Context, sh *shard, query []float32, topK int) ([]scoredResult, error) {
	reserveBytes := sh.store.Count() * uint64(sh.store.Dimension()) * 4
	if err := s.resourceManager.WaitForMemory(ctx, reserveBytes, "search_snapshot"); err != nil {
		return nil, err
	}

	hits, err := sh.store.SearchBruteForceTopK(query, topK)
	if err != nil {
		return nil, err
	}

	items := make([]scoredResult, 0, len(hits))
	for _, hit := range hits {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		items = append(items, scoredResult{
			key:  hit.Key,
			dist: hit.Distance,
		})
	}
	return items, nil
}

func (s *Server) persistShardIndexLocked(sh *shard, force bool) error {
	if sh.index == nil {
		return nil
	}
	if !force {
		if s.indexFlushOps == 0 {
			return nil
		}
		if sh.dirtyIndexOps < s.indexFlushOps {
			return nil
		}
	}

	// Save can be expensive; release shard lock while persisting.
	idx := sh.index
	dir := sh.dir
	sh.dirtyIndexOps = 0

	sh.mu.Unlock()
	sh.saveMu.Lock()
	// Legacy write amplification:
	// idx.Save snapshots and rewrites the full graph file, then calls f.Sync().
	// In the old design this ran in the foreground insert path; now it is invoked
	// by the background indexer, so inserts no longer wait on this disk-heavy step.
	err := idx.Save(dir)
	sh.saveMu.Unlock()
	sh.mu.Lock()

	if err != nil {
		if !force && s.indexFlushOps > 0 && sh.dirtyIndexOps < s.indexFlushOps {
			sh.dirtyIndexOps = s.indexFlushOps
		}
		return err
	}
	return nil
}

type scoredMaxHeap []scoredResult

func (h scoredMaxHeap) Len() int           { return len(h) }
func (h scoredMaxHeap) Less(i, j int) bool { return h[i].dist > h[j].dist }
func (h scoredMaxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *scoredMaxHeap) Push(x interface{}) {
	*h = append(*h, x.(scoredResult))
}
func (h *scoredMaxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func pushTopK(h *scoredMaxHeap, item scoredResult, topK int) {
	if topK <= 0 {
		return
	}
	if h.Len() < topK {
		heap.Push(h, item)
		return
	}
	if item.dist >= (*h)[0].dist {
		return
	}
	(*h)[0] = item
	heap.Fix(h, 0)
}

func heapToAscending(h *scoredMaxHeap) []scoredResult {
	out := make([]scoredResult, h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(h).(scoredResult)
	}
	return out
}

func shardDir(collectionDir string, shardID int) string {
	return filepath.Join(collectionDir, "shards", fmt.Sprintf("%04d", shardID))
}

func dirSizeBytes(root string) (int64, error) {
	var size int64
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		size += info.Size()
		return nil
	})
	if err != nil {
		return 0, err
	}
	return size, nil
}

func getDistanceFunc(metric string) hnsw.DistanceFunc {
	switch metric {
	case "l2":
		return hnsw.L2Distance
	case "dot_product", "cosine":
		return hnsw.DotProductDistance
	default:
		return hnsw.CosineDistance
	}
}

func getDistanceQueryFactory(metric string) hnsw.QueryDistanceFactory {
	switch metric {
	case "l2":
		return hnsw.PrepareL2Distance
	case "dot_product", "cosine":
		return hnsw.PrepareDotProductDistance
	default:
		return hnsw.PrepareCosineDistance
	}
}

func metricToString(m pb.Metric) string {
	switch m {
	case pb.Metric_L2:
		return "l2"
	case pb.Metric_DOT_PRODUCT:
		return "dot_product"
	default:
		return "cosine"
	}
}

func stringToMetric(s string) pb.Metric {
	switch s {
	case "l2":
		return pb.Metric_L2
	case "dot_product":
		return pb.Metric_DOT_PRODUCT
	default:
		return pb.Metric_COSINE
	}
}
