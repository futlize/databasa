package server

import (
	"sync"
)

type RuntimeConfig struct {
	WriteAdmissionMode string
}

var (
	runtimeCfgMu sync.RWMutex
	runtimeCfg   = normalizeRuntimeConfig(RuntimeConfig{})
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
	return normalized
}

func currentRuntimeConfig() RuntimeConfig {
	runtimeCfgMu.RLock()
	cfg := runtimeCfg
	runtimeCfgMu.RUnlock()
	return cfg
}

func normalizeRuntimeConfig(cfg RuntimeConfig) RuntimeConfig {
	normalized := RuntimeConfig{}
	switch cfg.WriteAdmissionMode {
	case "strict", "performance":
		normalized.WriteAdmissionMode = cfg.WriteAdmissionMode
	default:
		normalized.WriteAdmissionMode = "strict"
	}
	return normalized
}
