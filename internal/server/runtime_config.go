package server

import (
	"sync"

	"github.com/juniodev/kekdb/internal/resources"
)

type RuntimeConfig struct {
	MaxWorkers          int
	MemoryBudgetPercent int
	InsertQueueSize     int
	BulkLoadMode        bool
}

var (
	runtimeCfgMu sync.RWMutex
	runtimeCfg   = fromResourceConfig(resources.ConfigureGlobal(resources.DefaultConfig()))
)

func DefaultRuntimeConfig() RuntimeConfig {
	runtimeCfgMu.RLock()
	cfg := runtimeCfg
	runtimeCfgMu.RUnlock()
	return cfg
}

func SetDefaultRuntimeConfig(cfg RuntimeConfig) RuntimeConfig {
	normalized := normalizeRuntimeConfig(cfg)
	runtimeCfgMu.Lock()
	runtimeCfg = normalized
	runtimeCfgMu.Unlock()
	resources.ConfigureGlobal(toResourceConfig(normalized))
	return normalized
}

func currentRuntimeConfig() RuntimeConfig {
	runtimeCfgMu.RLock()
	cfg := runtimeCfg
	runtimeCfgMu.RUnlock()
	return cfg
}

func normalizeRuntimeConfig(cfg RuntimeConfig) RuntimeConfig {
	return fromResourceConfig(resources.NormalizeConfig(toResourceConfig(cfg)))
}

func toResourceConfig(cfg RuntimeConfig) resources.Config {
	return resources.Config{
		MaxWorkers:          cfg.MaxWorkers,
		MemoryBudgetPercent: cfg.MemoryBudgetPercent,
		InsertQueueSize:     cfg.InsertQueueSize,
		BulkLoadMode:        cfg.BulkLoadMode,
	}
}

func fromResourceConfig(cfg resources.Config) RuntimeConfig {
	return RuntimeConfig{
		MaxWorkers:          cfg.MaxWorkers,
		MemoryBudgetPercent: cfg.MemoryBudgetPercent,
		InsertQueueSize:     cfg.InsertQueueSize,
		BulkLoadMode:        cfg.BulkLoadMode,
	}
}
