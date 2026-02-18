package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

const defaultConfigPath = "databasa.toml"

type AppConfig struct {
	Server     ServerConfig
	Storage    StorageConfig
	Guardrails GuardrailsConfig
	Resources  ResourcesConfig
}

type ServerConfig struct {
	Port             int
	GRPCMaxRecvMB    int
	GRPCMaxSendMB    int
	EnableReflection bool
}

type StorageConfig struct {
	DataDir       string
	Shards        int
	IndexFlushOps int
	Compression   string
}

type GuardrailsConfig struct {
	MaxTopK             int
	MaxBatchSize        int
	MaxEfSearch         int
	MaxCollectionDim    int
	MaxConcurrentSearch int
	MaxConcurrentWrite  int
	MaxDataDirMB        int
	RequireRPCDeadline  bool
}

type ResourcesConfig struct {
	MaxWorkers          int
	MemoryBudgetPercent int
	InsertQueueSize     int
	BulkLoadMode        bool
}

func DefaultAppConfig() AppConfig {
	return AppConfig{
		Server: ServerConfig{
			Port:             50051,
			GRPCMaxRecvMB:    32,
			GRPCMaxSendMB:    32,
			EnableReflection: false,
		},
		Storage: StorageConfig{
			DataDir:       "./data",
			Shards:        8,
			IndexFlushOps: 4096,
			Compression:   "int8",
		},
		Guardrails: GuardrailsConfig{
			MaxTopK:             256,
			MaxBatchSize:        1000,
			MaxEfSearch:         4096,
			MaxCollectionDim:    8192,
			MaxConcurrentSearch: 64,
			MaxConcurrentWrite:  128,
			MaxDataDirMB:        0,
			RequireRPCDeadline:  false,
		},
		Resources: ResourcesConfig{
			MaxWorkers:          defaultMaxWorkers(),
			MemoryBudgetPercent: 80,
			InsertQueueSize:     4096,
			BulkLoadMode:        false,
		},
	}
}

func LoadOrCreateConfig(path string) (AppConfig, bool, error) {
	cfg := DefaultAppConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return cfg, false, fmt.Errorf("read config %q: %w", path, err)
		}
		if err := writeDefaultConfig(path, cfg); err != nil {
			return cfg, false, err
		}
		return cfg, true, nil
	}

	if len(data) > 0 {
		if err := parseConfig(data, &cfg); err != nil {
			return cfg, false, fmt.Errorf("parse config %q: %w", path, err)
		}
	}

	cfg.normalize()
	return cfg, false, nil
}

func writeDefaultConfig(path string, cfg AppConfig) error {
	body := []byte(renderConfig(cfg))
	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create config dir %q: %w", dir, err)
		}
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		return fmt.Errorf("write default config %q: %w", path, err)
	}
	return nil
}

func parseConfig(data []byte, cfg *AppConfig) error {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	section := ""
	lineNo := 0

	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}

		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.ToLower(strings.TrimSpace(line[1 : len(line)-1]))
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("line %d: expected key=value", lineNo)
		}
		key := strings.ToLower(strings.TrimSpace(parts[0]))
		value := cleanConfigValue(parts[1])

		switch section {
		case "server":
			if err := parseServerKey(cfg, key, value, lineNo); err != nil {
				return err
			}
		case "storage":
			if err := parseStorageKey(cfg, key, value, lineNo); err != nil {
				return err
			}
		case "guardrails":
			if err := parseGuardrailsKey(cfg, key, value, lineNo); err != nil {
				return err
			}
		case "resources":
			if err := parseResourcesKey(cfg, key, value, lineNo); err != nil {
				return err
			}
		default:
			return fmt.Errorf("line %d: key %q outside known section", lineNo, key)
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func cleanConfigValue(raw string) string {
	v := strings.TrimSpace(stripInlineComment(raw))
	if len(v) >= 2 {
		if (v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'') {
			v = v[1 : len(v)-1]
		}
	}
	return strings.TrimSpace(v)
}

func stripInlineComment(raw string) string {
	inQuote := byte(0)
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if inQuote != 0 {
			if ch == inQuote {
				inQuote = 0
			}
			continue
		}
		if ch == '"' || ch == '\'' {
			inQuote = ch
			continue
		}
		if ch == '#' || ch == ';' {
			if i == 0 || raw[i-1] == ' ' || raw[i-1] == '\t' {
				return strings.TrimSpace(raw[:i])
			}
		}
	}
	return strings.TrimSpace(raw)
}

func parseServerKey(cfg *AppConfig, key, value string, lineNo int) error {
	switch key {
	case "port":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for server.port", lineNo)
		}
		cfg.Server.Port = n
	case "grpc_max_recv_mb":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for server.grpc_max_recv_mb", lineNo)
		}
		cfg.Server.GRPCMaxRecvMB = n
	case "grpc_max_send_mb":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for server.grpc_max_send_mb", lineNo)
		}
		cfg.Server.GRPCMaxSendMB = n
	case "enable_reflection":
		b, err := parseBool(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid bool for server.enable_reflection", lineNo)
		}
		cfg.Server.EnableReflection = b
	default:
		return fmt.Errorf("line %d: unknown key %q in [server]", lineNo, key)
	}
	return nil
}

func parseStorageKey(cfg *AppConfig, key, value string, lineNo int) error {
	switch key {
	case "data_dir":
		cfg.Storage.DataDir = value
	case "shards":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for storage.shards", lineNo)
		}
		cfg.Storage.Shards = n
	case "index_flush_ops":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for storage.index_flush_ops", lineNo)
		}
		cfg.Storage.IndexFlushOps = n
	case "compression":
		cfg.Storage.Compression = strings.ToLower(value)
	default:
		return fmt.Errorf("line %d: unknown key %q in [storage]", lineNo, key)
	}
	return nil
}

func parseGuardrailsKey(cfg *AppConfig, key, value string, lineNo int) error {
	switch key {
	case "max_top_k":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for guardrails.max_top_k", lineNo)
		}
		cfg.Guardrails.MaxTopK = n
	case "max_batch_size":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for guardrails.max_batch_size", lineNo)
		}
		cfg.Guardrails.MaxBatchSize = n
	case "max_ef_search":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for guardrails.max_ef_search", lineNo)
		}
		cfg.Guardrails.MaxEfSearch = n
	case "max_collection_dim":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for guardrails.max_collection_dim", lineNo)
		}
		cfg.Guardrails.MaxCollectionDim = n
	case "max_concurrent_search":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for guardrails.max_concurrent_search", lineNo)
		}
		cfg.Guardrails.MaxConcurrentSearch = n
	case "max_concurrent_write":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for guardrails.max_concurrent_write", lineNo)
		}
		cfg.Guardrails.MaxConcurrentWrite = n
	case "max_data_dir_mb":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for guardrails.max_data_dir_mb", lineNo)
		}
		cfg.Guardrails.MaxDataDirMB = n
	case "require_rpc_deadline":
		b, err := parseBool(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid bool for guardrails.require_rpc_deadline", lineNo)
		}
		cfg.Guardrails.RequireRPCDeadline = b
	default:
		return fmt.Errorf("line %d: unknown key %q in [guardrails]", lineNo, key)
	}
	return nil
}

func parseResourcesKey(cfg *AppConfig, key, value string, lineNo int) error {
	switch key {
	case "max_workers":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for resources.max_workers", lineNo)
		}
		cfg.Resources.MaxWorkers = n
	case "memory_budget_percent":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for resources.memory_budget_percent", lineNo)
		}
		cfg.Resources.MemoryBudgetPercent = n
	case "insert_queue_size":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for resources.insert_queue_size", lineNo)
		}
		cfg.Resources.InsertQueueSize = n
	case "bulk_load_mode":
		b, err := parseBool(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid bool for resources.bulk_load_mode", lineNo)
		}
		cfg.Resources.BulkLoadMode = b
	default:
		return fmt.Errorf("line %d: unknown key %q in [resources]", lineNo, key)
	}
	return nil
}

func parseBool(value string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("invalid bool: %s", value)
	}
}

func renderConfig(cfg AppConfig) string {
	return fmt.Sprintf(`# Databasa configuration (TOML style)

[server]
port = %d
grpc_max_recv_mb = %d
grpc_max_send_mb = %d
enable_reflection = %t

[storage]
data_dir = %s
shards = %d
index_flush_ops = %d
compression = %s

[guardrails]
max_top_k = %d
max_batch_size = %d
max_ef_search = %d
max_collection_dim = %d
max_concurrent_search = %d
max_concurrent_write = %d
max_data_dir_mb = %d
require_rpc_deadline = %t

[resources]
max_workers = %d
memory_budget_percent = %d
insert_queue_size = %d
bulk_load_mode = %t
`, cfg.Server.Port, cfg.Server.GRPCMaxRecvMB, cfg.Server.GRPCMaxSendMB, cfg.Server.EnableReflection,
		cfg.Storage.DataDir, cfg.Storage.Shards, cfg.Storage.IndexFlushOps, cfg.Storage.Compression,
		cfg.Guardrails.MaxTopK, cfg.Guardrails.MaxBatchSize, cfg.Guardrails.MaxEfSearch, cfg.Guardrails.MaxCollectionDim,
		cfg.Guardrails.MaxConcurrentSearch, cfg.Guardrails.MaxConcurrentWrite, cfg.Guardrails.MaxDataDirMB,
		cfg.Guardrails.RequireRPCDeadline,
		cfg.Resources.MaxWorkers, cfg.Resources.MemoryBudgetPercent, cfg.Resources.InsertQueueSize, cfg.Resources.BulkLoadMode)
}

func (c *AppConfig) normalize() {
	defaults := DefaultAppConfig()

	if c.Server.Port <= 0 {
		c.Server.Port = defaults.Server.Port
	}
	if c.Server.GRPCMaxRecvMB <= 0 {
		c.Server.GRPCMaxRecvMB = defaults.Server.GRPCMaxRecvMB
	}
	if c.Server.GRPCMaxSendMB <= 0 {
		c.Server.GRPCMaxSendMB = defaults.Server.GRPCMaxSendMB
	}

	if c.Storage.DataDir == "" {
		c.Storage.DataDir = defaults.Storage.DataDir
	}
	if c.Storage.Shards <= 0 {
		c.Storage.Shards = defaults.Storage.Shards
	}
	if c.Storage.IndexFlushOps < 0 {
		c.Storage.IndexFlushOps = defaults.Storage.IndexFlushOps
	}
	switch c.Storage.Compression {
	case "none", "int8":
	default:
		c.Storage.Compression = defaults.Storage.Compression
	}

	if c.Guardrails.MaxTopK <= 0 {
		c.Guardrails.MaxTopK = defaults.Guardrails.MaxTopK
	}
	if c.Guardrails.MaxBatchSize <= 0 {
		c.Guardrails.MaxBatchSize = defaults.Guardrails.MaxBatchSize
	}
	if c.Guardrails.MaxEfSearch <= 0 {
		c.Guardrails.MaxEfSearch = defaults.Guardrails.MaxEfSearch
	}
	if c.Guardrails.MaxCollectionDim <= 0 {
		c.Guardrails.MaxCollectionDim = defaults.Guardrails.MaxCollectionDim
	}
	if c.Guardrails.MaxConcurrentSearch <= 0 {
		c.Guardrails.MaxConcurrentSearch = defaults.Guardrails.MaxConcurrentSearch
	}
	if c.Guardrails.MaxConcurrentWrite <= 0 {
		c.Guardrails.MaxConcurrentWrite = defaults.Guardrails.MaxConcurrentWrite
	}
	if c.Guardrails.MaxDataDirMB < 0 {
		c.Guardrails.MaxDataDirMB = defaults.Guardrails.MaxDataDirMB
	}

	if c.Resources.MaxWorkers <= 0 {
		c.Resources.MaxWorkers = defaults.Resources.MaxWorkers
	}
	if c.Resources.MemoryBudgetPercent <= 0 {
		c.Resources.MemoryBudgetPercent = defaults.Resources.MemoryBudgetPercent
	}
	if c.Resources.MemoryBudgetPercent < 10 {
		c.Resources.MemoryBudgetPercent = 10
	}
	if c.Resources.MemoryBudgetPercent > 95 {
		c.Resources.MemoryBudgetPercent = 95
	}
	if c.Resources.InsertQueueSize <= 0 {
		c.Resources.InsertQueueSize = defaults.Resources.InsertQueueSize
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
