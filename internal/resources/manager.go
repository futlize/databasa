package resources

import (
	"context"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strconv"
	"strings"
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

	memoryWarnPercent          = 70
	memoryThrottleEnterPercent = 80
	memoryThrottleExitPercent  = 70
	indexBlockPercent          = 90

	logIntervalMemory   = 5 * time.Second
	logIntervalWorkers  = 2 * time.Second
	logIntervalInserts  = 2 * time.Second
	throttleSleepPeriod = 25 * time.Millisecond
	memorySamplePeriod  = 300 * time.Millisecond
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

	memoryBudgetBytes       uint64
	memoryWarnBytes         uint64
	indexBlockBytes         uint64
	throttleEnterBytes      uint64
	throttleExitBytes       uint64
	sampledMemoryUsageBytes uint64
	insertThrottlingActive  uint32
	sampleStopCh            chan struct{}
	sampleDoneCh            chan struct{}
	sampleStopOnce          sync.Once

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
	if cgroupLimit, ok := readCgroupMemoryLimitBytes(); ok && (totalRAM == 0 || cgroupLimit < totalRAM) {
		totalRAM = cgroupLimit
	}
	if totalRAM == 0 {
		totalRAM = runtimeMemoryFallbackBytes()
	}

	budget := totalRAM * uint64(cfg.MemoryBudgetPercent) / 100
	if budget == 0 {
		budget = totalRAM * uint64(defaultMemoryBudgetPercent) / 100
	}

	warnAt := budget * memoryWarnPercent / 100
	throttleEnterAt := budget * memoryThrottleEnterPercent / 100
	throttleExitAt := budget * memoryThrottleExitPercent / 100
	indexBlockAt := budget * indexBlockPercent / 100
	if throttleEnterAt == 0 {
		throttleEnterAt = budget
	}
	if throttleExitAt == 0 {
		throttleExitAt = throttleEnterAt
	}
	if throttleExitAt > throttleEnterAt {
		throttleExitAt = throttleEnterAt
	}
	if indexBlockAt == 0 {
		indexBlockAt = budget
	}
	if indexBlockAt < throttleEnterAt {
		indexBlockAt = throttleEnterAt
	}

	debug.SetMemoryLimit(int64(budget))

	m := &Manager{
		cfg:                cfg,
		workerSem:          make(chan struct{}, cfg.MaxWorkers),
		memoryBudgetBytes:  budget,
		memoryWarnBytes:    warnAt,
		indexBlockBytes:    indexBlockAt,
		throttleEnterBytes: throttleEnterAt,
		throttleExitBytes:  throttleExitAt,
		sampleStopCh:       make(chan struct{}),
		sampleDoneCh:       make(chan struct{}),
	}
	usage := m.refreshMemoryUsageSample()
	m.updateThrottleState(usage)
	go m.sampleMemoryLoop()
	return m
}

func ConfigureGlobal(cfg Config) Config {
	normalized := NormalizeConfig(cfg)
	manager := NewManager(normalized)
	globalMu.Lock()
	previous := globalManager
	globalManager = manager
	globalMu.Unlock()
	if previous != nil {
		previous.Stop()
	}
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

func (m *Manager) Stop() {
	if m == nil {
		return
	}
	m.sampleStopOnce.Do(func() {
		close(m.sampleStopCh)
		<-m.sampleDoneCh
	})
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
		throttled := m.updateThrottleState(usage)
		m.maybeLogMemoryWarning(usage)
		if !throttled {
			return nil
		}

		if shouldLog(&m.lastInsertLog, logIntervalInserts) {
			log.Printf("WARN resources: inserts throttled usage=%d budget=%d throttle_enter=%d throttle_exit=%d", usage, m.memoryBudgetBytes, m.throttleEnterBytes, m.throttleExitBytes)
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
		throttled := m.updateThrottleState(usage)
		m.maybeLogMemoryWarning(usage)
		if usage+reserveBytes < m.memoryBudgetBytes && !throttled {
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
	if m == nil {
		return 0
	}
	usage := atomic.LoadUint64(&m.sampledMemoryUsageBytes)
	if usage != 0 {
		return usage
	}
	return m.refreshMemoryUsageSample()
}

func (m *Manager) sampleMemoryLoop() {
	ticker := time.NewTicker(memorySamplePeriod)
	defer ticker.Stop()
	defer close(m.sampleDoneCh)
	for {
		select {
		case <-m.sampleStopCh:
			return
		case <-ticker.C:
			usage := m.refreshMemoryUsageSample()
			m.updateThrottleState(usage)
		}
	}
}

func (m *Manager) refreshMemoryUsageSample() uint64 {
	usage := processMemoryUsageBytes()
	atomic.StoreUint64(&m.sampledMemoryUsageBytes, usage)
	return usage
}

func (m *Manager) updateThrottleState(usage uint64) bool {
	for {
		prev := atomic.LoadUint32(&m.insertThrottlingActive)
		next := prev
		if prev == 0 && usage >= m.throttleEnterBytes {
			next = 1
		}
		if prev == 1 && usage <= m.throttleExitBytes {
			next = 0
		}
		if prev == next {
			return next == 1
		}
		if atomic.CompareAndSwapUint32(&m.insertThrottlingActive, prev, next) {
			return next == 1
		}
	}
}

func processMemoryUsageBytes() uint64 {
	if rssBytes, ok := linuxProcessRSSBytes(); ok && rssBytes > 0 {
		return rssBytes
	}
	if runtimeBytes, ok := runtimeMetricBytes("/memory/classes/total:bytes"); ok && runtimeBytes > 0 {
		return runtimeBytes
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.Sys
}

func runtimeMemoryFallbackBytes() uint64 {
	if runtimeBytes, ok := runtimeMetricBytes("/memory/classes/total:bytes"); ok && runtimeBytes > 0 {
		return runtimeBytes * 4
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	if ms.Sys > 0 {
		return ms.Sys * 4
	}
	return 8 << 30
}

func runtimeMetricBytes(name string) (uint64, bool) {
	sample := []metrics.Sample{{Name: name}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindUint64 {
		return 0, false
	}
	return sample[0].Value.Uint64(), true
}

func linuxProcessRSSBytes() (uint64, bool) {
	if runtime.GOOS != "linux" {
		return 0, false
	}
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, false
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0, false
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return pages * uint64(os.Getpagesize()), true
}

func readCgroupMemoryLimitBytes() (uint64, bool) {
	if runtime.GOOS != "linux" {
		return 0, false
	}
	if limit, ok := parseMemoryLimitFile("/sys/fs/cgroup/memory.max"); ok {
		return limit, true
	}
	limit, ok := parseMemoryLimitFile("/sys/fs/cgroup/memory/memory.limit_in_bytes")
	if !ok {
		return 0, false
	}
	// v1 unlimited sentinel can be an extreme value; ignore it.
	if limit > (1 << 62) {
		return 0, false
	}
	return limit, true
}

func parseMemoryLimitFile(path string) (uint64, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	value := strings.TrimSpace(string(data))
	if value == "" || value == "max" {
		return 0, false
	}
	limit, err := strconv.ParseUint(value, 10, 64)
	if err != nil || limit == 0 {
		return 0, false
	}
	return limit, true
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
