package server

import (
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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
	defaultShardCount       = 8
	defaultCompression      = "int8"
	defaultMaxTopK          = 256
	defaultMaxBatchSize     = 1000
	defaultMaxEfSearch      = 4096
	defaultMaxCollectionDim = 8192

	requestResourceWaitTimeout = 2 * time.Second
)

type GuardrailConfig struct {
	MaxTopK            uint32
	MaxBatchSize       int
	MaxEfSearch        uint32
	MaxCollectionDim   uint32
	MaxDataDirBytes    int64
	RequireRPCDeadline bool
}

type StartupRecoveryReport struct {
	RecoveryNeeded      bool
	WALReplayDuration   time.Duration
	RecoveredOperations uint64
	LoadedPartitions    int
	LoadedShards        int
	LastCheckpoint      time.Time
}

type shard struct {
	id    int
	dir   string
	store *storage.VectorStore
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
	return c.indexConfig.M > 0
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
	compression   string

	maxTopK            uint32
	maxBatchSize       int
	maxEfSearch        uint32
	maxCollectionDim   uint32
	maxDataDirBytes    int64
	requireRPCDeadline bool

	resourceManager *resources.Manager
	storageOptions  storage.Options
	writeAdmission  string

	startupRecovery StartupRecoveryReport

	closeOnce sync.Once

	stopCtx    context.Context
	stopCancel context.CancelFunc
}

// New creates a server with optimized defaults for large scale.
func New(dataDir string) (*Server, error) {
	return NewWithConfig(dataDir, defaultShardCount, defaultCompression)
}

// NewWithConfig creates a server with explicit sharding/persistence/compression knobs.
func NewWithConfig(dataDir string, defaultShards int, compression string) (*Server, error) {
	stopCtx, stopCancel := context.WithCancel(context.Background())
	cleanupOnError := true
	defer func() {
		if cleanupOnError {
			stopCancel()
		}
	}()

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	if defaultShards <= 0 {
		defaultShards = 1
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
		compression:     compression,
		resourceManager: resources.Global(),
		storageOptions:  currentStorageOptions(),
		writeAdmission:  currentRuntimeConfig().WriteAdmissionMode,
		stopCtx:         stopCtx,
		stopCancel:      stopCancel,
	}
	s.configureGuardrailsLocked(GuardrailConfig{
		MaxTopK:          defaultMaxTopK,
		MaxBatchSize:     defaultMaxBatchSize,
		MaxEfSearch:      defaultMaxEfSearch,
		MaxCollectionDim: defaultMaxCollectionDim,
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

	cleanupOnError = false
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
	if cfg.MaxDataDirBytes < 0 {
		cfg.MaxDataDirBytes = 0
	}

	s.maxTopK = cfg.MaxTopK
	s.maxBatchSize = cfg.MaxBatchSize
	s.maxEfSearch = cfg.MaxEfSearch
	s.maxCollectionDim = cfg.MaxCollectionDim
	s.maxDataDirBytes = cfg.MaxDataDirBytes
	s.requireRPCDeadline = cfg.RequireRPCDeadline
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

func contextWithBoundedTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if timeout <= 0 {
		return context.WithCancel(parent)
	}
	if _, ok := parent.Deadline(); ok {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, timeout)
}

func waitForContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func shouldLogAtMost(last *int64, interval time.Duration) bool {
	now := time.Now().UnixNano()
	for {
		prev := atomic.LoadInt64(last)
		if prev != 0 && now-prev < interval.Nanoseconds() {
			return false
		}
		if atomic.CompareAndSwapInt64(last, prev, now) {
			return true
		}
	}
}

func waitWithBoundedContext(ctx context.Context, timeout time.Duration, fn func(context.Context) error) error {
	waitCtx, cancel := contextWithBoundedTimeout(ctx, timeout)
	defer cancel()
	return fn(waitCtx)
}

func resourceWaitStatus(err error, message string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.ResourceExhausted, message)
	}
	if errors.Is(err, context.Canceled) {
		return status.FromContextError(err).Err()
	}
	if st, ok := status.FromError(err); ok {
		return st.Err()
	}
	return status.Errorf(codes.Internal, "%s: %v", message, err)
}

func (s *Server) acquireWorkerRequest(ctx context.Context, op string, message string) error {
	return resourceWaitStatus(waitWithBoundedContext(ctx, requestResourceWaitTimeout, func(waitCtx context.Context) error {
		return s.resourceManager.AcquireWorker(waitCtx, op)
	}), message)
}

func (s *Server) waitInsertAllowanceRequest(ctx context.Context, message string) error {
	return resourceWaitStatus(waitWithBoundedContext(ctx, requestResourceWaitTimeout, func(waitCtx context.Context) error {
		return s.resourceManager.WaitInsertAllowance(waitCtx)
	}), message)
}

func (s *Server) waitForMemoryRequest(ctx context.Context, reserveBytes uint64, op string, message string) error {
	return resourceWaitStatus(waitWithBoundedContext(ctx, requestResourceWaitTimeout, func(waitCtx context.Context) error {
		return s.resourceManager.WaitForMemory(waitCtx, reserveBytes, op)
	}), message)
}

func (s *Server) waitForMemoryContext(ctx context.Context, reserveBytes uint64, op string) error {
	return waitWithBoundedContext(ctx, requestResourceWaitTimeout, func(waitCtx context.Context) error {
		return s.resourceManager.WaitForMemory(waitCtx, reserveBytes, op)
	})
}

func (s *Server) tryAcquireSearchSlot() error {
	return nil
}

func (s *Server) releaseSearchSlot() {
	// no-op: concurrency is intentionally unbounded at server slot level
}

func (s *Server) tryAcquireWriteSlot() error {
	return nil
}

func (s *Server) releaseWriteSlot() {
	// no-op: concurrency is intentionally unbounded at server slot level
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

	idxCfg := indexConfigFromMeta(meta)
	col := &collection{
		name:        meta.Name,
		dimension:   meta.Dimension,
		metric:      meta.Metric,
		compression: meta.Compression,
		indexConfig: idxCfg,
		shards:      make([]*shard, 0, meta.ShardCount),
	}
	for i := 0; i < int(meta.ShardCount); i++ {
		shardDir := shardDir(colDir, i)
		store, err := storage.OpenWithOptions(shardDir, name, meta.Dimension, meta.Metric, meta.Compression, s.storageOptions)
		if err != nil {
			return nil, fmt.Errorf("open shard %d: %w", i, err)
		}
		s.mergeStartupRecovery(store.StartupRecoveryReport())
		if idxCfg.M > 0 {
			store.EnableANN(idxCfg)
		}
		sh := &shard{id: i, dir: shardDir, store: store}
		col.shards = append(col.shards, sh)
	}

	return col, nil
}

// StartupRecoveryReport returns the startup recovery diagnostics aggregated across loaded shards.
func (s *Server) StartupRecoveryReport() StartupRecoveryReport {
	return s.startupRecovery
}

func (s *Server) mergeStartupRecovery(rec storage.StartupRecovery) {
	s.startupRecovery.RecoveryNeeded = s.startupRecovery.RecoveryNeeded || rec.RecoveryNeeded
	s.startupRecovery.WALReplayDuration += rec.WALReplayDuration
	s.startupRecovery.RecoveredOperations += uint64(rec.RecoveredOperations)
	s.startupRecovery.LoadedPartitions += rec.LoadedPartitions
	s.startupRecovery.LoadedShards++
	if rec.LastCheckpoint.After(s.startupRecovery.LastCheckpoint) {
		s.startupRecovery.LastCheckpoint = rec.LastCheckpoint
	}
}

// Close closes all collections.
func (s *Server) Close() {
	s.closeOnce.Do(func() {
		s.stopCancel()

		s.mu.Lock()
		shards := make([]*shard, 0)
		for _, col := range s.collections {
			shards = append(shards, col.shards...)
		}
		s.mu.Unlock()

		for _, sh := range shards {
			_ = sh.store.Close()
		}
	})
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
		IndexM:      0,
		IndexEfC:    0,
		IndexEfS:    0,
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
		store, err := storage.OpenWithOptions(shardDir, req.Name, req.Dimension, metric, s.compression, s.storageOptions)
		if err != nil {
			for _, existing := range col.shards {
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
		_ = sh.store.Close()
	}

	delete(s.collections, req.Name)
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

func (s *Server) CreateIndex(ctx context.Context, req *pb.CreateIndexRequest) (*pb.CreateIndexResponse, error) {
	if err := s.validateContext(ctx); err != nil {
		return nil, err
	}
	if err := s.enforceDataDirLimit(); err != nil {
		return nil, err
	}

	cfg := hnsw.DefaultConfig()
	if req.M > 0 {
		cfg.M = int(req.M)
		cfg.MMax0 = cfg.M * 2
	}
	if req.EfConstruction > 0 {
		cfg.EfConstruction = int(req.EfConstruction)
	}

	s.mu.RLock()
	col, exists := s.collections[req.Collection]
	if !exists {
		s.mu.RUnlock()
		return nil, status.Errorf(codes.NotFound, "collection %q not found", req.Collection)
	}
	shards := append([]*shard(nil), col.shards...)
	previousCfg := col.indexConfig
	colDir := filepath.Join(s.dataDir, col.name)
	meta := collectionMeta{
		Name:        col.name,
		Dimension:   col.dimension,
		Metric:      col.metric,
		Compression: col.compression,
		ShardCount:  uint32(len(col.shards)),
		IndexM:      uint32(cfg.M),
		IndexEfC:    uint32(cfg.EfConstruction),
		IndexEfS:    uint32(cfg.EfSearch),
	}
	s.mu.RUnlock()
	for _, sh := range shards {
		sh.store.EnableANN(cfg)
	}
	if err := saveCollectionMeta(colDir, meta); err != nil {
		for _, sh := range shards {
			if previousCfg.M > 0 {
				sh.store.EnableANN(previousCfg)
			} else {
				sh.store.DisableANN()
			}
		}
		return nil, status.Errorf(codes.Internal, "persist collection index config: %v", err)
	}

	s.mu.Lock()
	if current, ok := s.collections[req.Collection]; ok {
		current.indexConfig = cfg
	}
	s.mu.Unlock()
	return &pb.CreateIndexResponse{}, nil
}

func (s *Server) DropIndex(ctx context.Context, req *pb.DropIndexRequest) (*pb.DropIndexResponse, error) {
	if err := s.validateContext(ctx); err != nil {
		return nil, err
	}
	s.mu.RLock()
	col, exists := s.collections[req.Collection]
	if !exists {
		s.mu.RUnlock()
		return nil, status.Errorf(codes.NotFound, "collection %q not found", req.Collection)
	}
	shards := append([]*shard(nil), col.shards...)
	previousCfg := col.indexConfig
	colDir := filepath.Join(s.dataDir, col.name)
	meta := collectionMeta{
		Name:        col.name,
		Dimension:   col.dimension,
		Metric:      col.metric,
		Compression: col.compression,
		ShardCount:  uint32(len(col.shards)),
		IndexM:      0,
		IndexEfC:    0,
		IndexEfS:    0,
	}
	s.mu.RUnlock()

	for _, sh := range shards {
		sh.store.DisableANN()
	}
	if err := saveCollectionMeta(colDir, meta); err != nil {
		for _, sh := range shards {
			if previousCfg.M > 0 {
				sh.store.EnableANN(previousCfg)
			}
		}
		return nil, status.Errorf(codes.Internal, "persist collection index config: %v", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	col, exists = s.collections[req.Collection]
	if !exists {
		return nil, status.Errorf(codes.NotFound, "collection %q not found", req.Collection)
	}
	for _, sh := range col.shards {
		sh.store.DisableANN()
	}
	col.indexConfig = hnsw.Config{}

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
	if s.writeAdmission == "strict" {
		if err := s.waitInsertAllowanceRequest(ctx, "insert throttled"); err != nil {
			return nil, err
		}
		if err := s.waitForMemoryRequest(ctx, uint64(len(req.Embedding))*8, "insert", "insert memory budget exceeded"); err != nil {
			return nil, err
		}
	}

	sh := col.shardForKey(req.Key)
	if sh == nil {
		return nil, status.Errorf(codes.Internal, "no shard available")
	}

	storeCtx, storeCancel := contextWithBoundedTimeout(ctx, requestResourceWaitTimeout)
	_, err := sh.store.Insert(storeCtx, req.Key, req.Embedding)
	storeCancel()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "insert: %v", err)
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

	storeCtx, storeCancel := contextWithBoundedTimeout(ctx, requestResourceWaitTimeout)
	err := sh.store.DeleteWithContext(storeCtx, req.Key)
	storeCancel()
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
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
	if s.writeAdmission == "strict" {
		if err := s.waitInsertAllowanceRequest(ctx, "batch insert throttled"); err != nil {
			return nil, err
		}
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
		if s.writeAdmission == "strict" {
			reserveBytes += uint64(len(item.Embedding)) * 8
		}

		sh := col.shardForKey(item.Key)
		if sh == nil {
			return nil, status.Errorf(codes.Internal, "no shard available")
		}
		grouped[sh] = append(grouped[sh], storage.BatchInsertItem{
			Key:       item.Key,
			Embedding: item.Embedding,
		})
	}
	if s.writeAdmission == "strict" {
		if err := s.waitForMemoryRequest(ctx, reserveBytes, "batch_insert", "batch insert memory budget exceeded"); err != nil {
			return nil, err
		}
	}

	var inserted uint64
	for sh, shardItems := range grouped {
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}

		storeCtx, storeCancel := contextWithBoundedTimeout(ctx, requestResourceWaitTimeout)
		results, err := sh.store.BatchInsert(storeCtx, shardItems)
		storeCancel()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "batch insert shard %d: %v", sh.id, err)
		}
		inserted += uint64(len(results))
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
	results, err := s.searchDistributed(ctx, col, queryEmbedding, topK, int(req.EfSearch))
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		if st, ok := status.FromError(err); ok {
			return nil, st.Err()
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

func (s *Server) searchDistributed(ctx context.Context, col *collection, query []float32, topK int, efSearch int) ([]scoredResult, error) {
	out := make(chan shardSearchOutput, len(col.shards))
	for _, shardRef := range col.shards {
		sh := shardRef
		go func() {
			if err := s.acquireWorkerRequest(ctx, "search_shard", "search workers saturated"); err != nil {
				out <- shardSearchOutput{err: err}
				return
			}
			defer s.resourceManager.ReleaseWorker()

			hits, err := sh.store.SearchTopK(ctx, query, topK, efSearch)
			if err != nil {
				out <- shardSearchOutput{err: err}
				return
			}
			items := make([]scoredResult, 0, len(hits))
			for _, hit := range hits {
				items = append(items, scoredResult{
					key:  hit.Key,
					dist: hit.Distance,
				})
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
			// Ignore transient files that disappear during background compaction/flush.
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
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

func indexConfigFromMeta(meta collectionMeta) hnsw.Config {
	if meta.IndexM == 0 {
		return hnsw.Config{}
	}
	cfg := hnsw.DefaultConfig()
	cfg.M = int(meta.IndexM)
	cfg.MMax0 = cfg.M * 2
	if meta.IndexEfC > 0 {
		cfg.EfConstruction = int(meta.IndexEfC)
	}
	if meta.IndexEfS > 0 {
		cfg.EfSearch = int(meta.IndexEfS)
	}
	return cfg
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
