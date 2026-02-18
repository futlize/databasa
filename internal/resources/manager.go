package resources

import (
	"context"
	"log"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pbnjay/memory"
)

const (
	defaultMemoryBudgetPercent = 80
	defaultInsertQueueSize     = 4096
	minMemoryBudgetPercent     = 10
	maxMemoryBudgetPercent     = 95

	memoryWarnPercent = 70

	indexBlockNumerator   = 90
	indexBlockDenominator = 100

	insertThrottleNumerator   = 85
	insertThrottleDenominator = 100

	logIntervalMemory   = 5 * time.Second
	logIntervalWorkers  = 2 * time.Second
	logIntervalInserts  = 2 * time.Second
	throttleSleepPeriod = 25 * time.Millisecond
)

type Config struct {
	MaxWorkers          int
	MemoryBudgetPercent int
	InsertQueueSize     int
	BulkLoadMode        bool
}

type Manager struct {
	cfg Config

	workerSem chan struct{}

	memoryBudgetBytes   uint64
	memoryWarnBytes     uint64
	indexBlockBytes     uint64
	insertThrottleBytes uint64

	lastMemoryLog   int64
	lastWorkersLog  int64
	lastInsertLog   int64
	lastIndexingLog int64
}

var (
	globalMu      sync.RWMutex
	globalManager = NewManager(DefaultConfig())
)

func DefaultConfig() Config {
	return NormalizeConfig(Config{})
}

func NormalizeConfig(cfg Config) Config {
	if cfg.MaxWorkers <= 0 {
		cfg.MaxWorkers = defaultMaxWorkers()
	}
	if cfg.MemoryBudgetPercent <= 0 {
		cfg.MemoryBudgetPercent = defaultMemoryBudgetPercent
	}
	if cfg.MemoryBudgetPercent < minMemoryBudgetPercent {
		cfg.MemoryBudgetPercent = minMemoryBudgetPercent
	}
	if cfg.MemoryBudgetPercent > maxMemoryBudgetPercent {
		cfg.MemoryBudgetPercent = maxMemoryBudgetPercent
	}
	if cfg.InsertQueueSize <= 0 {
		cfg.InsertQueueSize = defaultInsertQueueSize
	}
	return cfg
}

func NewManager(cfg Config) *Manager {
	cfg = NormalizeConfig(cfg)

	totalRAM := memory.TotalMemory()
	if totalRAM == 0 {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		// Best effort fallback when host RAM cannot be detected.
		totalRAM = ms.Sys * 4
		if totalRAM == 0 {
			totalRAM = 8 << 30
		}
	}

	budget := totalRAM * uint64(cfg.MemoryBudgetPercent) / 100
	if budget == 0 {
		budget = totalRAM * uint64(defaultMemoryBudgetPercent) / 100
	}

	warnAt := budget * memoryWarnPercent / 100
	indexBlockAt := budget * indexBlockNumerator / indexBlockDenominator
	insertThrottleAt := budget * insertThrottleNumerator / insertThrottleDenominator
	if indexBlockAt == 0 {
		indexBlockAt = budget
	}
	if indexBlockAt < insertThrottleAt {
		indexBlockAt = insertThrottleAt
	}
	if insertThrottleAt == 0 {
		insertThrottleAt = budget
	}

	debug.SetMemoryLimit(int64(budget))

	return &Manager{
		cfg:                 cfg,
		workerSem:           make(chan struct{}, cfg.MaxWorkers),
		memoryBudgetBytes:   budget,
		memoryWarnBytes:     warnAt,
		indexBlockBytes:     indexBlockAt,
		insertThrottleBytes: insertThrottleAt,
	}
}

func ConfigureGlobal(cfg Config) Config {
	normalized := NormalizeConfig(cfg)
	manager := NewManager(normalized)
	globalMu.Lock()
	globalManager = manager
	globalMu.Unlock()
	return normalized
}

func Global() *Manager {
	globalMu.RLock()
	mgr := globalManager
	globalMu.RUnlock()
	return mgr
}

func (m *Manager) Config() Config {
	if m == nil {
		return DefaultConfig()
	}
	return m.cfg
}

func (m *Manager) AcquireWorker(ctx context.Context, op string) error {
	if m == nil {
		return nil
	}
	select {
	case m.workerSem <- struct{}{}:
		return nil
	default:
		if shouldLog(&m.lastWorkersLog, logIntervalWorkers) {
			log.Printf("WARN resources: workers saturated op=%s active=%d max=%d", op, len(m.workerSem), cap(m.workerSem))
		}
	}

	select {
	case m.workerSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) ReleaseWorker() {
	if m == nil {
		return
	}
	select {
	case <-m.workerSem:
	default:
	}
}

func (m *Manager) WaitInsertAllowance(ctx context.Context) error {
	if m == nil {
		return nil
	}
	for {
		usage := m.currentMemoryUsageBytes()
		m.maybeLogMemoryWarning(usage)
		if usage < m.insertThrottleBytes {
			return nil
		}

		if shouldLog(&m.lastInsertLog, logIntervalInserts) {
			log.Printf("WARN resources: inserts throttled usage=%d budget=%d", usage, m.memoryBudgetBytes)
		}

		if err := waitForContext(ctx, throttleSleepPeriod); err != nil {
			return err
		}
	}
}

func (m *Manager) WaitIndexingAllowance(ctx context.Context) error {
	if m == nil {
		return nil
	}
	for {
		usage := m.currentMemoryUsageBytes()
		m.maybeLogMemoryWarning(usage)
		if usage < m.indexBlockBytes {
			return nil
		}

		if shouldLog(&m.lastIndexingLog, logIntervalInserts) {
			log.Printf("WARN resources: indexing paused by memory pressure usage=%d budget=%d", usage, m.memoryBudgetBytes)
		}

		if err := waitForContext(ctx, throttleSleepPeriod); err != nil {
			return err
		}
	}
}

func (m *Manager) WaitForMemory(ctx context.Context, reserveBytes uint64, op string) error {
	if m == nil || reserveBytes == 0 {
		return nil
	}
	for {
		usage := m.currentMemoryUsageBytes()
		m.maybeLogMemoryWarning(usage)
		if usage+reserveBytes < m.memoryBudgetBytes {
			return nil
		}

		if shouldLog(&m.lastInsertLog, logIntervalInserts) {
			log.Printf("WARN resources: allocation delayed op=%s usage=%d reserve=%d budget=%d", op, usage, reserveBytes, m.memoryBudgetBytes)
		}

		if err := waitForContext(ctx, throttleSleepPeriod); err != nil {
			return err
		}
	}
}

func (m *Manager) currentMemoryUsageBytes() uint64 {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.Sys
}

func (m *Manager) maybeLogMemoryWarning(usage uint64) {
	if m.memoryWarnBytes == 0 || usage < m.memoryWarnBytes {
		return
	}
	if shouldLog(&m.lastMemoryLog, logIntervalMemory) {
		pct := float64(usage) * 100
		if m.memoryBudgetBytes > 0 {
			pct = pct / float64(m.memoryBudgetBytes)
		}
		log.Printf("WARN resources: memory usage high usage=%d budget=%d usage_of_budget=%.1f%%", usage, m.memoryBudgetBytes, pct)
	}
}

func defaultMaxWorkers() int {
	workers := runtime.NumCPU() / 2
	if workers < 1 {
		workers = 1
	}
	if workers > 8 {
		workers = 8
	}
	return workers
}

func shouldLog(last *int64, interval time.Duration) bool {
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
