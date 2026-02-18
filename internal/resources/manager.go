package resources

import (
	"context"
	"sync"
)

// Config is kept for backwards compatibility. Resource limits are ignored.
type Config struct {
	MaxWorkers          int
	MemoryBudgetPercent int
	InsertQueueSize     int
	BulkLoadMode        bool
}

type Manager struct {
	cfg Config
}

var (
	globalMu      sync.RWMutex
	globalManager = NewManager(DefaultConfig())
)

func DefaultConfig() Config {
	return Config{}
}

func NormalizeConfig(cfg Config) Config {
	return cfg
}

func NewManager(cfg Config) *Manager {
	return &Manager{cfg: NormalizeConfig(cfg)}
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

func (m *Manager) Stop() {}

// AcquireWorker no longer throttles; caller context is still respected.
func (m *Manager) AcquireWorker(ctx context.Context, op string) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func (m *Manager) ReleaseWorker() {}

// WaitInsertAllowance no longer throttles; caller context is still respected.
func (m *Manager) WaitInsertAllowance(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

// WaitIndexingAllowance no longer throttles; caller context is still respected.
func (m *Manager) WaitIndexingAllowance(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

// WaitForMemory no longer throttles; caller context is still respected.
func (m *Manager) WaitForMemory(ctx context.Context, reserveBytes uint64, op string) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}
