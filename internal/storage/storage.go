package storage

import (
	"bytes"
	"container/heap"
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
	"sync/atomic"
	"time"

	"github.com/futlize/databasa/internal/hnsw"
	"github.com/futlize/databasa/internal/resources"
)

const (
	metaFileName = "meta.bin"

	walDirName       = "wal"
	walActiveName    = "wal-active.log"
	partitionsDir    = "partitions"
	indexDirName     = "indexes"
	partitionFileExt = ".part"

	metaVersion      uint16 = 4
	walVersion       uint16 = 1
	partitionVersion uint16 = 1

	opUpsert byte = 1
	opDelete byte = 2

	defaultActivePartitionStripes  = 128
	defaultActivePartitionMaxOps   = 20_000
	defaultActivePartitionMaxBytes = 64 << 20
	defaultPartitionMaxCount       = 12
	defaultPartitionMergeFanIn     = 4
	defaultPartitionIndexMinRows   = 512

	defaultSealCheckInterval  = 250 * time.Millisecond
	defaultMergeCheckInterval = 2 * time.Second
	defaultIndexRescanEvery   = 1 * time.Second

	defaultIndexWorkers      = 2
	defaultMergeWorkers      = 1
	defaultOptimizerQueueCap = 256

	defaultWALQueueSize    = 8192
	defaultWALBatchMaxOps  = 1024
	defaultWALBatchWait    = 1 * time.Millisecond
	defaultWALSyncInterval = 10 * time.Millisecond
)

var (
	metaMagic      = [8]byte{'K', 'D', 'B', 'M', 'E', 'T', 'A', '4'}
	walMagic       = [8]byte{'K', 'D', 'B', 'W', 'A', 'L', '0', '1'}
	partitionMagic = [8]byte{'K', 'D', 'B', 'P', 'A', 'R', '0', '1'}
)

type WALSyncMode string

const (
	WALSyncAlways   WALSyncMode = "always"
	WALSyncPeriodic WALSyncMode = "periodic"
	WALSyncNone     WALSyncMode = "none"
)

// Options controls write-path durability/throughput tradeoffs and optimizer behavior.
type Options struct {
	ActivePartitionStripes  int
	ActivePartitionMaxOps   int
	ActivePartitionMaxBytes uint64

	PartitionMaxCount     int
	PartitionMergeFanIn   int
	PartitionIndexMinRows int

	SealCheckInterval  time.Duration
	MergeCheckInterval time.Duration
	IndexRescanEvery   time.Duration

	IndexWorkers      int
	MergeWorkers      int
	OptimizerQueueCap int

	WALSyncMode     WALSyncMode
	WALQueueSize    int
	WALBatchMaxOps  int
	WALBatchWait    time.Duration
	WALSyncInterval time.Duration

	ANNEnabled bool
	HNSWConfig hnsw.Config
}

// StartupRecovery holds durability/recovery diagnostics captured during OpenWithOptions.
type StartupRecovery struct {
	RecoveryNeeded      bool
	WALReplayDuration   time.Duration
	RecoveredOperations int
	LoadedPartitions    int
	LastCheckpoint      time.Time
}

func DefaultOptions() Options {
	return normalizeOptions(Options{
		ANNEnabled: false,
		HNSWConfig: hnsw.DefaultConfig(),
	})
}

func normalizeOptions(in Options) Options {
	out := in
	if out.ActivePartitionStripes <= 0 {
		out.ActivePartitionStripes = defaultActivePartitionStripes
	}
	if out.ActivePartitionMaxOps <= 0 {
		out.ActivePartitionMaxOps = defaultActivePartitionMaxOps
	}
	if out.ActivePartitionMaxBytes == 0 {
		out.ActivePartitionMaxBytes = defaultActivePartitionMaxBytes
	}
	if out.PartitionMaxCount <= 0 {
		out.PartitionMaxCount = defaultPartitionMaxCount
	}
	if out.PartitionMergeFanIn <= 1 {
		out.PartitionMergeFanIn = defaultPartitionMergeFanIn
	}
	if out.PartitionIndexMinRows <= 0 {
		out.PartitionIndexMinRows = defaultPartitionIndexMinRows
	}
	if out.SealCheckInterval <= 0 {
		out.SealCheckInterval = defaultSealCheckInterval
	}
	if out.MergeCheckInterval <= 0 {
		out.MergeCheckInterval = defaultMergeCheckInterval
	}
	if out.IndexRescanEvery <= 0 {
		out.IndexRescanEvery = defaultIndexRescanEvery
	}
	if out.IndexWorkers <= 0 {
		out.IndexWorkers = defaultIndexWorkers
	}
	if out.MergeWorkers <= 0 {
		out.MergeWorkers = defaultMergeWorkers
	}
	if out.OptimizerQueueCap <= 0 {
		out.OptimizerQueueCap = defaultOptimizerQueueCap
	}
	switch out.WALSyncMode {
	case WALSyncAlways, WALSyncPeriodic, WALSyncNone:
	default:
		out.WALSyncMode = WALSyncAlways
	}
	if out.WALQueueSize <= 0 {
		out.WALQueueSize = defaultWALQueueSize
	}
	if out.WALBatchMaxOps <= 0 {
		out.WALBatchMaxOps = defaultWALBatchMaxOps
	}
	if out.WALBatchWait <= 0 {
		out.WALBatchWait = defaultWALBatchWait
	}
	if out.WALSyncInterval <= 0 {
		out.WALSyncInterval = defaultWALSyncInterval
	}
	if out.HNSWConfig.M <= 0 {
		out.HNSWConfig = hnsw.DefaultConfig()
	}
	return out
}

// Meta stores collection-level settings persisted in meta.bin.
type Meta struct {
	Name        string `json:"name"`
	Dimension   uint32 `json:"dimension"`
	Metric      string `json:"metric"`      // "cosine", "l2", "dot_product"
	Compression string `json:"compression"` // "none", "int8"
	Count       uint64 `json:"count"`
}

// SearchHit is one search result from a shard.
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

// memRecord embedding slices become immutable after prepareEmbeddingForWrite.
// The write path can safely share these slices across id/active/sealed state as
// read-only data to avoid redundant per-record copies in the hot path.
type memRecord struct {
	op        byte
	id        uint64
	key       string
	embedding []float32
}

type keyStateShard struct {
	mu sync.RWMutex
	m  map[string]uint64
}

type keyState struct {
	shards []keyStateShard
}

func newKeyState(stripes int) *keyState {
	if stripes <= 0 {
		stripes = defaultActivePartitionStripes
	}
	k := &keyState{
		shards: make([]keyStateShard, stripes),
	}
	for i := range k.shards {
		k.shards[i].m = make(map[string]uint64)
	}
	return k
}

func (k *keyState) shardFor(key string) *keyStateShard {
	h := fnv32a(key)
	return &k.shards[h%uint32(len(k.shards))]
}

func (k *keyState) Get(key string) (uint64, bool) {
	sh := k.shardFor(key)
	sh.mu.RLock()
	id, ok := sh.m[key]
	sh.mu.RUnlock()
	return id, ok
}

func (k *keyState) Set(key string, id uint64) (oldID uint64, existed bool) {
	sh := k.shardFor(key)
	sh.mu.Lock()
	oldID, existed = sh.m[key]
	sh.m[key] = id
	sh.mu.Unlock()
	return oldID, existed
}

func (k *keyState) Delete(key string) (oldID uint64, existed bool) {
	sh := k.shardFor(key)
	sh.mu.Lock()
	oldID, existed = sh.m[key]
	if existed {
		delete(sh.m, key)
	}
	sh.mu.Unlock()
	return oldID, existed
}

func (k *keyState) SnapshotValues() []uint64 {
	out := make([]uint64, 0)
	for i := range k.shards {
		sh := &k.shards[i]
		sh.mu.RLock()
		for _, id := range sh.m {
			out = append(out, id)
		}
		sh.mu.RUnlock()
	}
	return out
}

type idStateEntry struct {
	key     string
	vec     []float32
	deleted bool
}

type idStateShard struct {
	mu sync.RWMutex
	m  map[uint64]idStateEntry
}

type idState struct {
	shards []idStateShard
}

func newIDState(stripes int) *idState {
	if stripes <= 0 {
		stripes = defaultActivePartitionStripes
	}
	s := &idState{
		shards: make([]idStateShard, stripes),
	}
	for i := range s.shards {
		s.shards[i].m = make(map[uint64]idStateEntry)
	}
	return s
}

func (s *idState) shardFor(id uint64) *idStateShard {
	return &s.shards[id%uint64(len(s.shards))]
}

func (s *idState) Put(id uint64, key string, vec []float32, deleted bool) {
	sh := s.shardFor(id)
	sh.mu.Lock()
	sh.m[id] = idStateEntry{
		key:     key,
		vec:     vec,
		deleted: deleted,
	}
	sh.mu.Unlock()
}

func (s *idState) MarkDeleted(id uint64) {
	sh := s.shardFor(id)
	sh.mu.Lock()
	entry, ok := sh.m[id]
	if ok {
		entry.deleted = true
		entry.vec = nil
		sh.m[id] = entry
	}
	sh.mu.Unlock()
}

func (s *idState) Get(id uint64) (idStateEntry, bool) {
	sh := s.shardFor(id)
	sh.mu.RLock()
	entry, ok := sh.m[id]
	sh.mu.RUnlock()
	return entry, ok
}

type activePartitionStripe struct {
	mu      sync.RWMutex
	records map[string]memRecord
}

type activePartition struct {
	id        uint64
	createdAt time.Time
	stripes   []activePartitionStripe
	opCount   atomic.Uint64
	byteSize  atomic.Uint64
}

func newActivePartition(id uint64, stripes int) *activePartition {
	if stripes <= 0 {
		stripes = defaultActivePartitionStripes
	}
	p := &activePartition{
		id:        id,
		createdAt: time.Now().UTC(),
		stripes:   make([]activePartitionStripe, stripes),
	}
	for i := range p.stripes {
		p.stripes[i].records = make(map[string]memRecord)
	}
	return p
}

func (p *activePartition) stripeFor(key string) *activePartitionStripe {
	h := fnv32a(key)
	return &p.stripes[h%uint32(len(p.stripes))]
}

func (p *activePartition) apply(rec memRecord) {
	stripe := p.stripeFor(rec.key)
	stripe.mu.Lock()
	stripe.records[rec.key] = rec
	stripe.mu.Unlock()

	p.opCount.Add(1)
	if rec.op == opUpsert {
		p.byteSize.Add(uint64(len(rec.embedding)) * 4)
	}
}

func (p *activePartition) snapshotRecords() []memRecord {
	total := 0
	for i := range p.stripes {
		stripe := &p.stripes[i]
		stripe.mu.RLock()
		total += len(stripe.records)
		stripe.mu.RUnlock()
	}

	out := make([]memRecord, 0, total)
	for i := range p.stripes {
		stripe := &p.stripes[i]
		stripe.mu.RLock()
		for _, rec := range stripe.records {
			out = append(out, rec)
		}
		stripe.mu.RUnlock()
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })
	return out
}

func (p *activePartition) shouldSeal(opts Options) bool {
	return int(p.opCount.Load()) >= opts.ActivePartitionMaxOps || p.byteSize.Load() >= opts.ActivePartitionMaxBytes
}

type sealedPartition struct {
	id        uint64
	createdAt time.Time
	walPath   string

	records []memRecord
	vectors map[uint64][]float32
	keys    map[uint64]string

	persisted atomic.Bool
	indexing  atomic.Bool
	merging   atomic.Bool
	index     atomic.Pointer[hnsw.Index]
}

func newSealedPartition(id uint64, records []memRecord) *sealedPartition {
	p := &sealedPartition{
		id:        id,
		createdAt: time.Now().UTC(),
		records:   make([]memRecord, 0, len(records)),
		vectors:   make(map[uint64][]float32),
		keys:      make(map[uint64]string),
	}
	for _, rec := range records {
		p.records = append(p.records, rec)
		if rec.op == opUpsert {
			p.vectors[rec.id] = rec.embedding
			p.keys[rec.id] = rec.key
		}
	}
	return p
}

func (p *sealedPartition) indexReady() bool {
	return p.index.Load() != nil
}

type walAppendReq struct {
	ctx     context.Context
	records []memRecord
	resp    chan error
}

type walRotateReq struct {
	ctx         context.Context
	rotatedPath string
	resp        chan error
}

type walCloseReq struct {
	resp chan error
}

type walWriter struct {
	activePath string
	syncMode   WALSyncMode
	batchWait  time.Duration
	batchMax   int
	syncEvery  time.Duration

	appendCh chan walAppendReq
	rotateCh chan walRotateReq
	closeCh  chan walCloseReq

	wg sync.WaitGroup
}

func newWALWriter(activePath string, opts Options) (*walWriter, error) {
	if err := os.MkdirAll(filepath.Dir(activePath), 0o755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}
	f, err := openWALForAppend(activePath)
	if err != nil {
		return nil, err
	}

	w := &walWriter{
		activePath: activePath,
		syncMode:   opts.WALSyncMode,
		batchWait:  opts.WALBatchWait,
		batchMax:   opts.WALBatchMaxOps,
		syncEvery:  opts.WALSyncInterval,
		appendCh:   make(chan walAppendReq, opts.WALQueueSize),
		rotateCh:   make(chan walRotateReq, 8),
		closeCh:    make(chan walCloseReq, 1),
	}
	w.wg.Add(1)
	go w.loop(f)
	return w, nil
}

func (w *walWriter) Append(ctx context.Context, records []memRecord) error {
	if len(records) == 0 {
		return nil
	}
	req := walAppendReq{
		ctx:     ctx,
		records: records,
		resp:    make(chan error, 1),
	}
	select {
	case w.appendCh <- req:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-req.resp:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *walWriter) Rotate(ctx context.Context, rotatedPath string) error {
	req := walRotateReq{
		ctx:         ctx,
		rotatedPath: rotatedPath,
		resp:        make(chan error, 1),
	}
	select {
	case w.rotateCh <- req:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-req.resp:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *walWriter) Close() error {
	req := walCloseReq{resp: make(chan error, 1)}
	w.closeCh <- req
	err := <-req.resp
	w.wg.Wait()
	return err
}

func (w *walWriter) loop(f *os.File) {
	defer w.wg.Done()
	defer f.Close()

	lastSync := time.Now()
	syncTicker := time.NewTicker(w.syncEvery)
	defer syncTicker.Stop()
	batch := make([]walAppendReq, 0, w.batchMax)
	payload := make([]byte, 0, 64*1024)

	flushAppendBatch := func(batch []walAppendReq) {
		if len(batch) == 0 {
			return
		}
		payload = payload[:0]
		var appendErr error
		for _, req := range batch {
			for _, rec := range req.records {
				var err error
				payload, err = appendEncodedRecord(payload, rec)
				if err != nil {
					appendErr = fmt.Errorf("encode wal record: %w", err)
					break
				}
			}
			if appendErr != nil {
				break
			}
		}

		if appendErr == nil && len(payload) > 0 {
			if _, err := f.Write(payload); err != nil {
				appendErr = fmt.Errorf("write wal batch: %w", err)
			}
		}
		if appendErr == nil && w.syncMode == WALSyncAlways {
			if err := f.Sync(); err != nil {
				appendErr = fmt.Errorf("sync wal: %w", err)
			} else {
				lastSync = time.Now()
			}
		}
		if appendErr == nil && w.syncMode == WALSyncPeriodic && time.Since(lastSync) >= w.syncEvery {
			if err := f.Sync(); err != nil {
				appendErr = fmt.Errorf("periodic sync wal: %w", err)
			} else {
				lastSync = time.Now()
			}
		}
		for _, req := range batch {
			req.resp <- appendErr
		}
	}

	rotate := func(req walRotateReq) {
		rotateErr := error(nil)
		if err := f.Sync(); err != nil {
			rotateErr = fmt.Errorf("sync wal before rotate: %w", err)
		}
		if rotateErr == nil {
			if err := f.Close(); err != nil {
				rotateErr = fmt.Errorf("close wal before rotate: %w", err)
			}
		}
		if rotateErr == nil {
			if err := os.Rename(w.activePath, req.rotatedPath); err != nil {
				rotateErr = fmt.Errorf("rotate wal file: %w", err)
			}
		}

		if rotateErr != nil {
			req.resp <- rotateErr
			reopen, err := openWALForAppend(w.activePath)
			if err == nil {
				f = reopen
			}
			return
		}

		newFile, err := createFreshWAL(w.activePath)
		if err != nil {
			req.resp <- fmt.Errorf("create fresh wal after rotate: %w", err)
			reopen, reopenErr := openWALForAppend(w.activePath)
			if reopenErr == nil {
				f = reopen
			}
			return
		}
		f = newFile
		lastSync = time.Now()
		req.resp <- nil
	}

	for {
		select {
		case closeReq := <-w.closeCh:
			closeErr := error(nil)
			if err := f.Sync(); err != nil {
				closeErr = fmt.Errorf("sync wal on close: %w", err)
			}
			if err := f.Close(); err != nil {
				if closeErr == nil {
					closeErr = fmt.Errorf("close wal: %w", err)
				}
			}
			closeReq.resp <- closeErr
			return

		case rotateReq := <-w.rotateCh:
			rotate(rotateReq)

		case appendReq := <-w.appendCh:
			batch = batch[:0]
			batch = append(batch, appendReq)
			waitTimer := time.NewTimer(w.batchWait)
		collect:
			for len(batch) < w.batchMax {
				select {
				case req := <-w.appendCh:
					batch = append(batch, req)
				case rotateReq := <-w.rotateCh:
					if !waitTimer.Stop() {
						select {
						case <-waitTimer.C:
						default:
						}
					}
					flushAppendBatch(batch)
					rotate(rotateReq)
					batch = batch[:0]
					goto doneCollect
				case <-waitTimer.C:
					break collect
				}
			}
			if !waitTimer.Stop() {
				select {
				case <-waitTimer.C:
				default:
				}
			}
		doneCollect:
			if len(batch) > 0 {
				flushAppendBatch(batch)
			}

		case <-syncTicker.C:
			if w.syncMode == WALSyncPeriodic && time.Since(lastSync) >= w.syncEvery {
				_ = f.Sync()
				lastSync = time.Now()
			}
		}
	}
}

// VectorStore implements a partitioned WAL-backed vector engine.
//
// Invariants:
// - Insert acknowledgements depend only on WAL append durability policy + active partition apply.
// - Active partition is always included in query fan-out, so successful upserts are immediately queryable.
// - ANN construction is asynchronous per sealed partition and never blocks inserts.
type VectorStore struct {
	dir             string
	walDir          string
	partitionsDir   string
	indexDir        string
	walActivePath   string
	resourceManager *resources.Manager

	meta Meta
	opts Options

	startupRecovery StartupRecovery

	keyState *keyState
	idState  *idState

	nextID      atomic.Uint64
	nextWALSeq  atomic.Uint64
	partitionID atomic.Uint64
	liveCount   atomic.Int64

	active atomic.Pointer[activePartition]

	sealedMu   sync.RWMutex
	sealed     []*sealedPartition
	sealedByID map[uint64]*sealedPartition

	annEnabled atomic.Bool
	indexCfg   atomic.Value // hnsw.Config

	wal      *walWriter
	rotateMu sync.Mutex

	sealCh  chan struct{}
	indexCh chan uint64
	mergeCh chan struct{}

	closeCh   chan struct{}
	closeOnce sync.Once
	closed    atomic.Bool
	bgCtx     context.Context
	bgCancel  context.CancelFunc
	bgWG      sync.WaitGroup
}

// Open opens or creates a vector store with default compression ("none").
func Open(dir string, name string, dimension uint32, metric string) (*VectorStore, error) {
	return OpenWithOptions(dir, name, dimension, metric, "none", DefaultOptions())
}

// OpenWithCompression opens or creates a vector store with configurable compression.
func OpenWithCompression(dir string, name string, dimension uint32, metric, compression string) (*VectorStore, error) {
	return OpenWithOptions(dir, name, dimension, metric, compression, DefaultOptions())
}

// OpenWithOptions opens a store with explicit optimizer/WAL/ANN configuration.
func OpenWithOptions(dir string, name string, dimension uint32, metric, compression string, options Options) (*VectorStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create dir: %w", err)
	}
	opts := normalizeOptions(options)
	metric = normalizeMetric(metric)
	compression = normalizeCompression(compression)

	bgCtx, bgCancel := context.WithCancel(context.Background())
	vs := &VectorStore{
		dir:             dir,
		walDir:          filepath.Join(dir, walDirName),
		partitionsDir:   filepath.Join(dir, partitionsDir),
		indexDir:        filepath.Join(dir, indexDirName),
		walActivePath:   filepath.Join(dir, walDirName, walActiveName),
		resourceManager: resources.Global(),
		opts:            opts,
		keyState:        newKeyState(opts.ActivePartitionStripes),
		idState:         newIDState(opts.ActivePartitionStripes),
		sealedByID:      make(map[uint64]*sealedPartition),
		sealCh:          make(chan struct{}, opts.OptimizerQueueCap),
		indexCh:         make(chan uint64, opts.OptimizerQueueCap),
		mergeCh:         make(chan struct{}, opts.OptimizerQueueCap),
		closeCh:         make(chan struct{}),
		bgCtx:           bgCtx,
		bgCancel:        bgCancel,
		meta: Meta{
			Name:        name,
			Dimension:   dimension,
			Metric:      metric,
			Compression: compression,
		},
	}

	vs.annEnabled.Store(opts.ANNEnabled)
	vs.indexCfg.Store(opts.HNSWConfig)

	if err := os.MkdirAll(vs.walDir, 0o755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}
	if err := os.MkdirAll(vs.partitionsDir, 0o755); err != nil {
		return nil, fmt.Errorf("create partitions dir: %w", err)
	}
	if err := os.MkdirAll(vs.indexDir, 0o755); err != nil {
		return nil, fmt.Errorf("create index dir: %w", err)
	}

	if err := vs.loadOrInitializeMeta(name, dimension, metric, compression); err != nil {
		return nil, err
	}

	maxPartitionID, err := vs.loadPersistedPartitions()
	if err != nil {
		return nil, err
	}
	loadedPartitions := len(vs.sealedSnapshot())
	vs.partitionID.Store(maxPartitionID + 1)
	vs.active.Store(newActivePartition(vs.partitionID.Load(), opts.ActivePartitionStripes))

	walReplayStarted := time.Now()
	rotatedWalPaths, maxWALSeq, recoveredOps, err := vs.replayWALFilesToActive()
	if err != nil {
		return nil, err
	}
	walReplayDuration := time.Since(walReplayStarted)
	vs.nextWALSeq.Store(maxWALSeq)

	if err := vs.checkpointActiveWAL(rotatedWalPaths); err != nil {
		return nil, err
	}
	lastCheckpoint := checkpointTimestamp(vs.walActivePath)
	vs.startupRecovery = StartupRecovery{
		RecoveryNeeded:      recoveredOps > 0,
		WALReplayDuration:   walReplayDuration,
		RecoveredOperations: recoveredOps,
		LoadedPartitions:    loadedPartitions,
		LastCheckpoint:      lastCheckpoint,
	}

	walWriter, err := newWALWriter(vs.walActivePath, opts)
	if err != nil {
		return nil, err
	}
	vs.wal = walWriter

	vs.startBackgroundWorkers()
	vs.scheduleSeal()
	vs.scheduleMerge()
	vs.scheduleAllIndexes()
	return vs, nil
}

func (vs *VectorStore) startBackgroundWorkers() {
	vs.bgWG.Add(1)
	go vs.sealLoop()

	for i := 0; i < vs.opts.IndexWorkers; i++ {
		vs.bgWG.Add(1)
		go vs.indexLoop()
	}
	for i := 0; i < vs.opts.MergeWorkers; i++ {
		vs.bgWG.Add(1)
		go vs.mergeLoop()
	}
}

func (vs *VectorStore) sealLoop() {
	defer vs.bgWG.Done()
	ticker := time.NewTicker(vs.opts.SealCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-vs.closeCh:
			_ = vs.sealActive(true)
			_ = vs.persistPendingPartitions()
			return
		case <-vs.sealCh:
			_ = vs.sealActive(false)
		case <-ticker.C:
			_ = vs.sealActive(false)
			_ = vs.persistPendingPartitions()
		}
	}
}

func (vs *VectorStore) indexLoop() {
	defer vs.bgWG.Done()
	ticker := time.NewTicker(vs.opts.IndexRescanEvery)
	defer ticker.Stop()

	for {
		select {
		case <-vs.closeCh:
			return
		case id := <-vs.indexCh:
			_ = vs.buildPartitionIndex(id)
		case <-ticker.C:
			vs.scheduleAllIndexes()
		}
	}
}

func (vs *VectorStore) mergeLoop() {
	defer vs.bgWG.Done()
	ticker := time.NewTicker(vs.opts.MergeCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-vs.closeCh:
			return
		case <-vs.mergeCh:
			_ = vs.mergeIfNeeded()
		case <-ticker.C:
			_ = vs.mergeIfNeeded()
		}
	}
}

func (vs *VectorStore) scheduleSeal() {
	select {
	case vs.sealCh <- struct{}{}:
	default:
	}
}

func (vs *VectorStore) scheduleMerge() {
	select {
	case vs.mergeCh <- struct{}{}:
	default:
	}
}

func (vs *VectorStore) scheduleIndex(id uint64) {
	select {
	case vs.indexCh <- id:
	default:
	}
}

func (vs *VectorStore) scheduleAllIndexes() {
	if !vs.annEnabled.Load() {
		return
	}
	parts := vs.sealedSnapshot()
	for _, p := range parts {
		if len(p.vectors) < vs.opts.PartitionIndexMinRows {
			continue
		}
		if p.indexReady() || p.indexing.Load() {
			continue
		}
		vs.scheduleIndex(p.id)
	}
}

func (vs *VectorStore) applyRecordToGlobal(rec memRecord) {
	vs.advanceNextID(rec.id)
	switch rec.op {
	case opUpsert:
		vs.idState.Put(rec.id, rec.key, rec.embedding, false)
		oldID, existed := vs.keyState.Set(rec.key, rec.id)
		if !existed {
			vs.liveCount.Add(1)
		}
		if existed && oldID != rec.id {
			vs.idState.MarkDeleted(oldID)
		}
	case opDelete:
		oldID, existed := vs.keyState.Delete(rec.key)
		if existed {
			vs.idState.MarkDeleted(oldID)
			vs.liveCount.Add(-1)
		}
		if rec.id > 0 {
			vs.idState.MarkDeleted(rec.id)
		}
	}
}

func (vs *VectorStore) applyRecordToActive(rec memRecord) {
	active := vs.active.Load()
	if active == nil {
		return
	}
	active.apply(rec)
}

func (vs *VectorStore) advanceNextID(id uint64) {
	for {
		current := vs.nextID.Load()
		next := current
		if id >= current {
			next = id + 1
		}
		if next == current {
			return
		}
		if vs.nextID.CompareAndSwap(current, next) {
			return
		}
	}
}

func (vs *VectorStore) allocateID() uint64 {
	return vs.nextID.Add(1) - 1
}

// Insert adds a key+embedding pair and returns the internal ID.
func (vs *VectorStore) Insert(ctx context.Context, key string, embedding []float32) (uint64, error) {
	if vs.closed.Load() {
		return 0, context.Canceled
	}
	if key == "" {
		return 0, fmt.Errorf("key is required")
	}
	if uint32(len(embedding)) != vs.meta.Dimension {
		return 0, fmt.Errorf("dimension mismatch: got %d, want %d", len(embedding), vs.meta.Dimension)
	}
	id := vs.allocateID()
	rec := memRecord{
		op:        opUpsert,
		id:        id,
		key:       key,
		embedding: prepareEmbeddingForWrite(vs.meta.Metric, embedding),
	}
	if err := vs.wal.Append(ctx, []memRecord{rec}); err != nil {
		return 0, err
	}

	vs.applyRecordToGlobal(rec)
	vs.applyRecordToActive(rec)
	vs.scheduleSeal()
	return id, nil
}

// BatchInsert inserts/upserts multiple vectors.
func (vs *VectorStore) BatchInsert(ctx context.Context, items []BatchInsertItem) ([]BatchInsertResult, error) {
	if vs.closed.Load() {
		return nil, context.Canceled
	}
	if len(items) == 0 {
		return nil, nil
	}
	for _, item := range items {
		if item.Key == "" {
			return nil, fmt.Errorf("key is required")
		}
		if uint32(len(item.Embedding)) != vs.meta.Dimension {
			return nil, fmt.Errorf("dimension mismatch: got %d, want %d", len(item.Embedding), vs.meta.Dimension)
		}
	}

	records := make([]memRecord, 0, len(items))
	results := make([]BatchInsertResult, 0, len(items))
	shadow := make(map[string]uint64, len(items))
	for _, item := range items {
		oldID, exists := shadow[item.Key]
		if !exists {
			oldID, exists = vs.keyState.Get(item.Key)
		}
		id := vs.allocateID()
		shadow[item.Key] = id
		records = append(records, memRecord{
			op:        opUpsert,
			id:        id,
			key:       item.Key,
			embedding: prepareEmbeddingForWrite(vs.meta.Metric, item.Embedding),
		})
		result := BatchInsertResult{
			Key: item.Key,
			ID:  id,
		}
		if exists {
			result.HadOld = true
			result.OldID = oldID
		}
		results = append(results, result)
	}

	if err := vs.wal.Append(ctx, records); err != nil {
		return nil, err
	}
	for _, rec := range records {
		vs.applyRecordToGlobal(rec)
		vs.applyRecordToActive(rec)
	}
	vs.scheduleSeal()
	return results, nil
}

// DeleteWithContext removes a key from the store with caller cancellation.
func (vs *VectorStore) DeleteWithContext(ctx context.Context, key string) error {
	if vs.closed.Load() {
		return context.Canceled
	}
	if key == "" {
		return fmt.Errorf("key is required")
	}
	oldID, exists := vs.keyState.Get(key)
	if !exists {
		return fmt.Errorf("key not found: %s", key)
	}
	rec := memRecord{
		op:  opDelete,
		id:  oldID,
		key: key,
	}
	if err := vs.wal.Append(ctx, []memRecord{rec}); err != nil {
		return err
	}
	vs.applyRecordToGlobal(rec)
	vs.applyRecordToActive(rec)
	vs.scheduleSeal()
	return nil
}

// Delete removes a key from the store.
func (vs *VectorStore) Delete(key string) error {
	ctx, cancel := context.WithTimeout(vs.bgCtx, 2*time.Second)
	defer cancel()
	return vs.DeleteWithContext(ctx, key)
}

// EnableANN turns on asynchronous ANN optimization with a new HNSW config.
func (vs *VectorStore) EnableANN(cfg hnsw.Config) {
	if cfg.M <= 0 {
		cfg = hnsw.DefaultConfig()
	}
	vs.indexCfg.Store(cfg)
	vs.annEnabled.Store(true)
	vs.scheduleAllIndexes()
}

// DisableANN turns off ANN serving; search falls back to exact scans.
func (vs *VectorStore) DisableANN() {
	vs.annEnabled.Store(false)
}

// ANNEnabled reports whether ANN serving is enabled.
func (vs *VectorStore) ANNEnabled() bool {
	return vs.annEnabled.Load()
}

// GetByKey returns the embedding for a given key.
func (vs *VectorStore) GetByKey(key string) ([]float32, error) {
	id, ok := vs.keyState.Get(key)
	if !ok {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	entry, ok := vs.idState.Get(id)
	if !ok || entry.deleted {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	current, ok := vs.keyState.Get(key)
	if !ok || current != id {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	return cloneEmbedding(entry.vec), nil
}

// GetByID returns the embedding for a given internal ID.
func (vs *VectorStore) GetByID(id uint64) ([]float32, error) {
	entry, ok := vs.idState.Get(id)
	if !ok || entry.deleted {
		return nil, fmt.Errorf("id not found: %d", id)
	}
	current, ok := vs.keyState.Get(entry.key)
	if !ok || current != id {
		return nil, fmt.Errorf("id not found: %d", id)
	}
	return cloneEmbedding(entry.vec), nil
}

// GetID returns the internal ID for the given key.
func (vs *VectorStore) GetID(key string) (uint64, bool) {
	return vs.keyState.Get(key)
}

// GetKey returns the key for a given internal ID.
func (vs *VectorStore) GetKey(id uint64) (string, bool) {
	entry, ok := vs.idState.Get(id)
	if !ok || entry.deleted {
		return "", false
	}
	current, ok := vs.keyState.Get(entry.key)
	if !ok || current != id {
		return "", false
	}
	return entry.key, true
}

// ResolveActiveKeys resolves keys for candidate IDs in one call.
func (vs *VectorStore) ResolveActiveKeys(ids []uint64) map[uint64]string {
	out := make(map[uint64]string, len(ids))
	for _, id := range ids {
		key, ok := vs.visibleKeyForID(id)
		if ok {
			out[id] = key
		}
	}
	return out
}

// IsDeleted checks if an internal ID has been tombstoned.
func (vs *VectorStore) IsDeleted(id uint64) bool {
	entry, ok := vs.idState.Get(id)
	if !ok {
		return true
	}
	return entry.deleted
}

// Count returns the number of active (non-deleted) vectors.
func (vs *VectorStore) Count() uint64 {
	n := vs.liveCount.Load()
	if n < 0 {
		return 0
	}
	return uint64(n)
}

// StartupRecoveryReport returns immutable startup recovery diagnostics for this store.
func (vs *VectorStore) StartupRecoveryReport() StartupRecovery {
	return vs.startupRecovery
}

// Dimension returns the vector dimension for this collection.
func (vs *VectorStore) Dimension() uint32 {
	return vs.meta.Dimension
}

// MetricName returns the metric name.
func (vs *VectorStore) MetricName() string {
	return vs.meta.Metric
}

// CompressionName returns the compression mode for this shard.
func (vs *VectorStore) CompressionName() string {
	return vs.meta.Compression
}

// AllIDs returns all active internal IDs.
func (vs *VectorStore) AllIDs() []uint64 {
	return vs.keyState.SnapshotValues()
}

func (vs *VectorStore) visibleKeyForID(id uint64) (string, bool) {
	entry, ok := vs.idState.Get(id)
	if !ok || entry.deleted {
		return "", false
	}
	current, ok := vs.keyState.Get(entry.key)
	if !ok || current != id {
		return "", false
	}
	return entry.key, true
}

// SearchTopK performs fan-out search across active + sealed partitions.
func (vs *VectorStore) SearchTopK(ctx context.Context, query []float32, topK int, efSearch int) ([]SearchHit, error) {
	return vs.searchInternal(ctx, query, topK, efSearch, vs.annEnabled.Load())
}

// SearchBruteForceTopK performs exact fan-out scans across active + sealed partitions.
func (vs *VectorStore) SearchBruteForceTopK(ctx context.Context, query []float32, topK int) ([]SearchHit, error) {
	return vs.searchInternal(ctx, query, topK, 0, false)
}

func (vs *VectorStore) searchInternal(ctx context.Context, query []float32, topK int, efSearch int, allowANN bool) ([]SearchHit, error) {
	if topK <= 0 {
		return nil, nil
	}
	if uint32(len(query)) != vs.meta.Dimension {
		return nil, fmt.Errorf("dimension mismatch: got %d, want %d", len(query), vs.meta.Dimension)
	}
	if vs.meta.Metric == "cosine" {
		query = prepareEmbeddingForWrite(vs.meta.Metric, query)
	}
	candidateK := topK * 2
	if candidateK < 32 {
		candidateK = 32
	}
	if efSearch <= 0 {
		cfg, _ := vs.indexCfg.Load().(hnsw.Config)
		efSearch = cfg.EfSearch
		if efSearch <= 0 {
			efSearch = 64
		}
	}

	best := &searchHitMaxHeap{}
	heap.Init(best)

	active := vs.active.Load()
	if active != nil {
		activeHits, err := vs.searchActive(ctx, active, query, topK)
		if err != nil {
			return nil, err
		}
		for _, hit := range activeHits {
			pushSearchHit(best, hit, topK)
		}
	}

	for _, part := range vs.sealedSnapshot() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var partHits []SearchHit
		var err error
		if allowANN && part.indexReady() {
			partHits, err = vs.searchSealedANN(ctx, part, query, topK, candidateK, efSearch)
		} else {
			partHits, err = vs.searchSealedBruteForce(ctx, part, query, topK)
		}
		if err != nil {
			return nil, err
		}
		for _, hit := range partHits {
			pushSearchHit(best, hit, topK)
		}
	}

	return heapToAscending(best), nil
}

func (vs *VectorStore) searchActive(ctx context.Context, part *activePartition, query []float32, topK int) ([]SearchHit, error) {
	distance := buildDistanceEvaluator(vs.meta.Metric, query)
	best := &searchHitMaxHeap{}
	heap.Init(best)

	for i := range part.stripes {
		stripe := &part.stripes[i]
		stripe.mu.RLock()
		candidates := make([]memRecord, 0, len(stripe.records))
		for _, rec := range stripe.records {
			if rec.op == opUpsert {
				candidates = append(candidates, rec)
			}
		}
		stripe.mu.RUnlock()

		for _, rec := range candidates {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			key, ok := vs.visibleKeyForID(rec.id)
			if !ok {
				continue
			}
			dist := distance(rec.embedding)
			pushSearchHit(best, SearchHit{
				ID:       rec.id,
				Key:      key,
				Distance: dist,
			}, topK)
		}
	}
	return heapToAscending(best), nil
}

func (vs *VectorStore) searchSealedANN(ctx context.Context, part *sealedPartition, query []float32, topK, candidateK, efSearch int) ([]SearchHit, error) {
	idx := part.index.Load()
	if idx == nil {
		return vs.searchSealedBruteForce(ctx, part, query, topK)
	}
	results, err := idx.Search(query, candidateK, efSearch)
	if err != nil {
		return nil, err
	}

	out := make([]SearchHit, 0, len(results))
	for _, item := range results {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key, ok := vs.visibleKeyForID(item.ID)
		if !ok {
			continue
		}
		out = append(out, SearchHit{
			ID:       item.ID,
			Key:      key,
			Distance: item.Distance,
		})
		if len(out) >= topK {
			break
		}
	}
	return out, nil
}

func (vs *VectorStore) searchSealedBruteForce(ctx context.Context, part *sealedPartition, query []float32, topK int) ([]SearchHit, error) {
	distance := buildDistanceEvaluator(vs.meta.Metric, query)
	best := &searchHitMaxHeap{}
	heap.Init(best)

	for id, vec := range part.vectors {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key, ok := vs.visibleKeyForID(id)
		if !ok {
			continue
		}
		dist := distance(vec)
		pushSearchHit(best, SearchHit{
			ID:       id,
			Key:      key,
			Distance: dist,
		}, topK)
	}
	return heapToAscending(best), nil
}

func (vs *VectorStore) sealedSnapshot() []*sealedPartition {
	vs.sealedMu.RLock()
	out := append([]*sealedPartition(nil), vs.sealed...)
	vs.sealedMu.RUnlock()
	return out
}

func (vs *VectorStore) sealActive(force bool) error {
	vs.rotateMu.Lock()
	defer vs.rotateMu.Unlock()

	active := vs.active.Load()
	if active == nil {
		return nil
	}
	if !force && !active.shouldSeal(vs.opts) {
		return nil
	}
	records := active.snapshotRecords()
	if len(records) == 0 {
		return nil
	}

	rotatedWal := filepath.Join(vs.walDir, fmt.Sprintf("wal-%020d.log", vs.nextWALSeq.Add(1)))
	rotateCtx, cancel := context.WithTimeout(vs.bgCtx, 5*time.Second)
	err := vs.wal.Rotate(rotateCtx, rotatedWal)
	cancel()
	if err != nil {
		return err
	}

	nextPartitionID := vs.partitionID.Add(1)
	vs.active.Store(newActivePartition(nextPartitionID, vs.opts.ActivePartitionStripes))

	part := newSealedPartition(active.id, records)
	part.walPath = rotatedWal
	vs.appendSealedPartition(part)

	if err := vs.persistSealedPartition(part); err != nil {
		// Keep in-memory partition queryable and WAL file for crash recovery.
		return err
	}
	vs.scheduleIndex(part.id)
	vs.scheduleMerge()
	return nil
}

func (vs *VectorStore) appendSealedPartition(part *sealedPartition) {
	vs.sealedMu.Lock()
	vs.sealed = append(vs.sealed, part)
	sort.Slice(vs.sealed, func(i, j int) bool { return vs.sealed[i].id < vs.sealed[j].id })
	vs.sealedByID[part.id] = part
	vs.sealedMu.Unlock()
}

func (vs *VectorStore) persistPendingPartitions() error {
	parts := vs.sealedSnapshot()
	for _, part := range parts {
		if part.persisted.Load() {
			continue
		}
		if err := vs.persistSealedPartition(part); err != nil {
			return err
		}
	}
	return nil
}

func (vs *VectorStore) persistSealedPartition(part *sealedPartition) error {
	if part.persisted.Load() {
		return nil
	}

	path := vs.partitionPath(part.id)
	tmp := path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("ensure partition dir: %w", err)
	}
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create partition temp: %w", err)
	}
	if err := writeLogHeader(f, partitionMagic, partitionVersion); err != nil {
		_ = f.Close()
		return fmt.Errorf("write partition header: %w", err)
	}
	for _, rec := range part.records {
		if err := writeRecord(f, rec); err != nil {
			_ = f.Close()
			return fmt.Errorf("write partition record: %w", err)
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync partition file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close partition temp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("commit partition file: %w", err)
	}

	part.persisted.Store(true)
	if part.walPath != "" {
		_ = os.Remove(part.walPath)
		part.walPath = ""
	}
	return nil
}

func (vs *VectorStore) partitionPath(partitionID uint64) string {
	return filepath.Join(vs.partitionsDir, fmt.Sprintf("partition-%020d%s", partitionID, partitionFileExt))
}

func (vs *VectorStore) partitionIndexPath(partitionID uint64) string {
	return filepath.Join(vs.indexDir, fmt.Sprintf("partition-%020d", partitionID))
}

func (vs *VectorStore) buildPartitionIndex(partitionID uint64) error {
	if !vs.annEnabled.Load() {
		return nil
	}
	part := vs.partitionByID(partitionID)
	if part == nil {
		return nil
	}
	if len(part.vectors) < vs.opts.PartitionIndexMinRows {
		return nil
	}
	if part.indexReady() {
		return nil
	}
	if !part.indexing.CompareAndSwap(false, true) {
		return nil
	}
	defer part.indexing.Store(false)

	waitCtx, cancel := context.WithTimeout(vs.bgCtx, 2*time.Second)
	defer cancel()
	if err := vs.resourceManager.WaitIndexingAllowance(waitCtx); err != nil {
		return err
	}

	cfg, _ := vs.indexCfg.Load().(hnsw.Config)
	if cfg.M <= 0 {
		cfg = hnsw.DefaultConfig()
	}
	distFunc := getDistanceFunc(vs.meta.Metric)
	queryFactory := getDistanceQueryFactory(vs.meta.Metric)
	idx := hnsw.New(cfg, distFunc, func(id uint64) ([]float32, error) {
		vec, ok := part.vectors[id]
		if !ok {
			return nil, fmt.Errorf("id %d not found in partition", id)
		}
		return vec, nil
	})
	idx.SetQueryDistanceFactory(queryFactory)

	ids := make([]uint64, 0, len(part.vectors))
	for id := range part.vectors {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	for _, id := range ids {
		if err := vs.bgCtx.Err(); err != nil {
			return err
		}
		if err := idx.Insert(id); err != nil {
			return fmt.Errorf("insert id %d into partition index: %w", id, err)
		}
	}

	indexPath := vs.partitionIndexPath(part.id)
	if err := os.MkdirAll(indexPath, 0o755); err != nil {
		return fmt.Errorf("create partition index dir: %w", err)
	}
	if err := idx.Save(indexPath); err != nil {
		// Keep in-memory index even when fs persistence fails.
		part.index.Store(idx)
		return fmt.Errorf("persist partition index: %w", err)
	}
	part.index.Store(idx)
	return nil
}

func (vs *VectorStore) partitionByID(id uint64) *sealedPartition {
	vs.sealedMu.RLock()
	part := vs.sealedByID[id]
	vs.sealedMu.RUnlock()
	return part
}

func (vs *VectorStore) mergeIfNeeded() error {
	parts := vs.sealedSnapshot()
	if len(parts) <= vs.opts.PartitionMaxCount {
		return nil
	}
	mergeN := vs.opts.PartitionMergeFanIn
	if mergeN > len(parts) {
		mergeN = len(parts)
	}
	candidates := make([]*sealedPartition, 0, mergeN)
	for _, p := range parts {
		if !p.persisted.Load() {
			continue
		}
		if !p.merging.CompareAndSwap(false, true) {
			continue
		}
		candidates = append(candidates, p)
		if len(candidates) >= mergeN {
			break
		}
	}
	if len(candidates) < 2 {
		for _, p := range candidates {
			p.merging.Store(false)
		}
		return nil
	}
	defer func() {
		for _, p := range candidates {
			p.merging.Store(false)
		}
	}()

	replayed := make(map[string]memRecord, 0)
	for _, p := range candidates {
		for _, rec := range p.records {
			replayed[rec.key] = memRecord{
				op:        rec.op,
				id:        rec.id,
				key:       rec.key,
				embedding: cloneEmbedding(rec.embedding),
			}
		}
	}
	keys := make([]string, 0, len(replayed))
	for key := range replayed {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	mergedRecords := make([]memRecord, 0, len(keys))
	for _, key := range keys {
		mergedRecords = append(mergedRecords, replayed[key])
	}
	newPartID := vs.partitionID.Add(1)
	newPart := newSealedPartition(newPartID, mergedRecords)
	if err := vs.persistSealedPartition(newPart); err != nil {
		return err
	}

	removeIDs := make(map[uint64]struct{}, len(candidates))
	for _, p := range candidates {
		removeIDs[p.id] = struct{}{}
	}

	vs.sealedMu.Lock()
	nextList := make([]*sealedPartition, 0, len(vs.sealed)-len(candidates)+1)
	for _, p := range vs.sealed {
		if _, drop := removeIDs[p.id]; drop {
			delete(vs.sealedByID, p.id)
			continue
		}
		nextList = append(nextList, p)
	}
	nextList = append(nextList, newPart)
	sort.Slice(nextList, func(i, j int) bool { return nextList[i].id < nextList[j].id })
	vs.sealed = nextList
	vs.sealedByID[newPart.id] = newPart
	vs.sealedMu.Unlock()

	for _, oldPart := range candidates {
		_ = os.Remove(vs.partitionPath(oldPart.id))
		_ = os.RemoveAll(vs.partitionIndexPath(oldPart.id))
	}
	vs.scheduleIndex(newPart.id)
	return nil
}

func (vs *VectorStore) loadPersistedPartitions() (uint64, error) {
	entries, err := os.ReadDir(vs.partitionsDir)
	if err != nil {
		return 0, fmt.Errorf("read partition dir: %w", err)
	}
	type partRef struct {
		id   uint64
		path string
	}
	parts := make([]partRef, 0, len(entries))
	var maxID uint64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		id, ok := parsePartitionID(entry.Name())
		if !ok {
			continue
		}
		parts = append(parts, partRef{
			id:   id,
			path: filepath.Join(vs.partitionsDir, entry.Name()),
		})
		if id > maxID {
			maxID = id
		}
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].id < parts[j].id })

	for _, part := range parts {
		records, err := readPartitionFile(part.path, vs.meta.Dimension)
		if err != nil {
			return maxID, fmt.Errorf("read partition %s: %w", part.path, err)
		}
		sealed := newSealedPartition(part.id, records)
		sealed.persisted.Store(true)
		vs.appendSealedPartition(sealed)
		for _, rec := range records {
			vs.applyRecordToGlobal(rec)
		}
		_ = vs.loadPartitionIndex(sealed)
	}
	return maxID, nil
}

func (vs *VectorStore) loadPartitionIndex(part *sealedPartition) error {
	if !vs.annEnabled.Load() {
		return nil
	}
	indexPath := vs.partitionIndexPath(part.id)
	if _, err := os.Stat(filepath.Join(indexPath, "hnsw.graph")); err != nil {
		return nil
	}
	distFunc := getDistanceFunc(vs.meta.Metric)
	idx, err := hnsw.Load(indexPath, distFunc, func(id uint64) ([]float32, error) {
		vec, ok := part.vectors[id]
		if !ok {
			return nil, fmt.Errorf("id %d missing in partition %d", id, part.id)
		}
		return vec, nil
	})
	if err != nil {
		return err
	}
	idx.SetQueryDistanceFactory(getDistanceQueryFactory(vs.meta.Metric))
	part.index.Store(idx)
	return nil
}

func parsePartitionID(name string) (uint64, bool) {
	if !strings.HasPrefix(name, "partition-") || !strings.HasSuffix(name, partitionFileExt) {
		return 0, false
	}
	idPart := strings.TrimSuffix(strings.TrimPrefix(name, "partition-"), partitionFileExt)
	id, err := strconv.ParseUint(idPart, 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

func readPartitionFile(path string, dimension uint32) ([]memRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if err := readLogHeader(f, partitionMagic, partitionVersion); err != nil {
		return nil, err
	}
	records := make([]memRecord, 0, 1024)
	for {
		rec, err := readRecord(f, dimension, false)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		records = append(records, rec)
	}
	return records, nil
}

func parseWALSeq(name string) (uint64, bool) {
	if !strings.HasPrefix(name, "wal-") || !strings.HasSuffix(name, ".log") {
		return 0, false
	}
	if name == walActiveName {
		return 0, false
	}
	seqPart := strings.TrimSuffix(strings.TrimPrefix(name, "wal-"), ".log")
	seq, err := strconv.ParseUint(seqPart, 10, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
}

func (vs *VectorStore) replayWALFilesToActive() ([]string, uint64, int, error) {
	entries, err := os.ReadDir(vs.walDir)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("read wal dir: %w", err)
	}

	type walRef struct {
		seq  uint64
		path string
	}
	rotated := make([]walRef, 0, len(entries))
	activeExists := false
	var maxSeq uint64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == walActiveName {
			activeExists = true
			continue
		}
		seq, ok := parseWALSeq(name)
		if !ok {
			continue
		}
		rotated = append(rotated, walRef{
			seq:  seq,
			path: filepath.Join(vs.walDir, name),
		})
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	sort.Slice(rotated, func(i, j int) bool { return rotated[i].seq < rotated[j].seq })

	removeAfterCheckpoint := make([]string, 0, len(rotated))
	var recoveredOps int
	for _, walRef := range rotated {
		count, err := vs.replayWALFile(walRef.path)
		if err != nil {
			return nil, maxSeq, recoveredOps, fmt.Errorf("replay wal %s: %w", walRef.path, err)
		}
		recoveredOps += count
		removeAfterCheckpoint = append(removeAfterCheckpoint, walRef.path)
	}
	if activeExists {
		count, err := vs.replayWALFile(vs.walActivePath)
		if err != nil {
			return nil, maxSeq, recoveredOps, fmt.Errorf("replay active wal: %w", err)
		}
		recoveredOps += count
	}
	return removeAfterCheckpoint, maxSeq, recoveredOps, nil
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
	var replayed int
	for {
		rec, err := readRecord(f, vs.meta.Dimension, true)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return replayed, nil
			}
			return replayed, err
		}
		vs.applyRecordToGlobal(rec)
		vs.applyRecordToActive(rec)
		replayed++
	}
}

func checkpointTimestamp(path string) time.Time {
	info, err := os.Stat(path)
	if err != nil {
		return time.Time{}
	}
	return info.ModTime().UTC()
}

func (vs *VectorStore) checkpointActiveWAL(rotatedWalPaths []string) error {
	f, err := createFreshWAL(vs.walActivePath)
	if err != nil {
		return err
	}
	active := vs.active.Load()
	if active != nil {
		for _, rec := range active.snapshotRecords() {
			if err := writeRecord(f, rec); err != nil {
				_ = f.Close()
				return fmt.Errorf("checkpoint wal record: %w", err)
			}
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync checkpoint wal: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close checkpoint wal: %w", err)
	}
	for _, path := range rotatedWalPaths {
		_ = os.Remove(path)
	}
	return nil
}

// Close closes open resources and flushes pending background work.
func (vs *VectorStore) Close() error {
	var closeErr error
	vs.closeOnce.Do(func() {
		vs.closed.Store(true)
		close(vs.closeCh)
		vs.bgCancel()
		vs.bgWG.Wait()

		var errs []error
		if vs.wal != nil {
			if err := vs.wal.Close(); err != nil {
				errs = append(errs, err)
			}
			vs.wal = nil
		}

		vs.meta.Count = vs.Count()
		if err := vs.persistMeta(); err != nil {
			errs = append(errs, err)
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
	return nil
}

func (vs *VectorStore) persistMeta() error {
	return writeMeta(filepath.Join(vs.dir, metaFileName), vs.meta)
}

type searchHitMaxHeap []SearchHit

func (h searchHitMaxHeap) Len() int { return len(h) }

func (h searchHitMaxHeap) Less(i, j int) bool {
	if h[i].Distance == h[j].Distance {
		return h[i].Key > h[j].Key
	}
	return h[i].Distance > h[j].Distance
}

func (h searchHitMaxHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *searchHitMaxHeap) Push(x interface{}) {
	*h = append(*h, x.(SearchHit))
}

func (h *searchHitMaxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func pushSearchHit(h *searchHitMaxHeap, candidate SearchHit, limit int) {
	if limit <= 0 {
		return
	}
	if h.Len() < limit {
		heap.Push(h, candidate)
		return
	}
	worst := (*h)[0]
	if candidate.Distance > worst.Distance {
		return
	}
	if candidate.Distance == worst.Distance && candidate.Key >= worst.Key {
		return
	}
	(*h)[0] = candidate
	heap.Fix(h, 0)
}

func heapToAscending(h *searchHitMaxHeap) []SearchHit {
	out := make([]SearchHit, h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(h).(SearchHit)
	}
	return out
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
		return func(candidate []float32) float32 {
			var dot, normA, normB float32
			for i, q := range query {
				v := candidate[i]
				dot += q * v
				normA += q * q
				normB += v * v
			}
			if normA == 0 || normB == 0 {
				return 1
			}
			sim := dot / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))
			return 1 - sim
		}
	}
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

func appendEncodedRecord(dst []byte, rec memRecord) ([]byte, error) {
	switch rec.op {
	case opUpsert, opDelete:
	default:
		return dst, fmt.Errorf("unknown record op %d", rec.op)
	}

	maxUint32 := int(^uint32(0))
	if len(rec.key) > maxUint32 {
		return dst, fmt.Errorf("key too long: %d", len(rec.key))
	}

	payloadLen := 1 + 8 + 4 + len(rec.key)
	if rec.op == opUpsert {
		if len(rec.embedding) > (maxUint32-payloadLen-4)/4 {
			return dst, fmt.Errorf("embedding too large: %d", len(rec.embedding))
		}
		payloadLen += 4 + len(rec.embedding)*4
	}
	if payloadLen > maxUint32 {
		return dst, fmt.Errorf("record payload too large: %d", payloadLen)
	}

	start := len(dst)
	dst = append(dst, 0, 0, 0, 0)
	dst = append(dst, rec.op)
	dst = binary.LittleEndian.AppendUint64(dst, rec.id)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(rec.key)))
	dst = append(dst, rec.key...)

	if rec.op == opUpsert {
		dst = binary.LittleEndian.AppendUint32(dst, uint32(len(rec.embedding)))
		for _, v := range rec.embedding {
			dst = binary.LittleEndian.AppendUint32(dst, math.Float32bits(v))
		}
	}
	binary.LittleEndian.PutUint32(dst[start:start+4], uint32(payloadLen))
	return dst, nil
}

func writeRecord(w io.Writer, rec memRecord) error {
	payload, err := appendEncodedRecord(nil, rec)
	if err != nil {
		return err
	}
	_, err = w.Write(payload)
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
	rec := memRecord{
		op:  op[0],
		id:  id,
		key: string(keyBytes),
	}
	switch rec.op {
	case opUpsert:
		var vecLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &vecLen); err != nil {
			return memRecord{}, err
		}
		if dimension > 0 && vecLen != dimension {
			return memRecord{}, fmt.Errorf("vector dimension mismatch in log record: got %d want %d", vecLen, dimension)
		}
		rec.embedding = make([]float32, vecLen)
		for i := range rec.embedding {
			var bits uint32
			if err := binary.Read(reader, binary.LittleEndian, &bits); err != nil {
				return memRecord{}, err
			}
			rec.embedding[i] = math.Float32frombits(bits)
		}
	case opDelete:
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
			return nil, fmt.Errorf("seek wal start: %w", err)
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

func fnv32a(s string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	hash := uint32(offset32)
	for i := 0; i < len(s); i++ {
		hash ^= uint32(s[i])
		hash *= prime32
	}
	return hash
}
