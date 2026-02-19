package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const defaultConfigPath = "databasa.toml"

type AppConfig struct {
	Server     ServerConfig
	Storage    StorageConfig
	Guardrails GuardrailsConfig
	Security   SecurityConfig
}

type ServerConfig struct {
	Port             int
	GRPCMaxRecvMB    int
	GRPCMaxSendMB    int
	EnableReflection bool
}

type StorageConfig struct {
	DataDir                   string
	Shards                    int
	Compression               string
	WriteMode                 string
	WALSyncMode               string
	WALSyncIntervalMS         int
	WALBatchWaitMS            int
	WALBatchMaxOps            int
	WALQueueSize              int
	ActivePartitionMaxOps     int
	ActivePartitionMaxBytesMB int
	PartitionMaxCount         int
	PartitionMergeFanIn       int
	PartitionIndexMinRows     int
	IndexWorkers              int
	MergeWorkers              int
	OptimizerQueueCap         int
	IndexM                    int
	IndexEfConstruction       int
	IndexEfSearch             int
}

type GuardrailsConfig struct {
	MaxTopK            int
	MaxBatchSize       int
	MaxEfSearch        int
	MaxCollectionDim   int
	MaxDataDirMB       int
	RequireRPCDeadline bool
}

type SecurityConfig struct {
	AuthEnabled   bool
	RequireAuth   bool
	APIKeyHeader  string
	TLSEnabled    bool
	TLSCertFile   string
	TLSKeyFile    string
	TLSClientAuth string
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
			DataDir:                   "./data",
			Shards:                    8,
			Compression:               "int8",
			WriteMode:                 "strict",
			WALSyncMode:               "auto",
			WALSyncIntervalMS:         10,
			WALBatchWaitMS:            1,
			WALBatchMaxOps:            1024,
			WALQueueSize:              8192,
			ActivePartitionMaxOps:     20000,
			ActivePartitionMaxBytesMB: 64,
			PartitionMaxCount:         12,
			PartitionMergeFanIn:       4,
			PartitionIndexMinRows:     512,
			IndexWorkers:              2,
			MergeWorkers:              1,
			OptimizerQueueCap:         256,
			IndexM:                    16,
			IndexEfConstruction:       200,
			IndexEfSearch:             50,
		},
		Guardrails: GuardrailsConfig{
			MaxTopK:            256,
			MaxBatchSize:       1000,
			MaxEfSearch:        4096,
			MaxCollectionDim:   8192,
			MaxDataDirMB:       0,
			RequireRPCDeadline: false,
		},
		Security: SecurityConfig{
			AuthEnabled:   true,
			RequireAuth:   true,
			APIKeyHeader:  "authorization",
			TLSEnabled:    false,
			TLSCertFile:   "./certs/server.crt",
			TLSKeyFile:    "./certs/server.key",
			TLSClientAuth: "none",
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
		case "security":
			if err := parseSecurityKey(cfg, key, value, lineNo); err != nil {
				return err
			}
		case "resources":
			// Legacy section: ignored.
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
	case "compression":
		cfg.Storage.Compression = strings.ToLower(value)
	case "write_mode":
		cfg.Storage.WriteMode = strings.ToLower(value)
	case "wal_sync_mode":
		cfg.Storage.WALSyncMode = strings.ToLower(value)
	case "wal_sync_interval_ms":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for storage.wal_sync_interval_ms", lineNo)
		}
		cfg.Storage.WALSyncIntervalMS = n
	case "wal_batch_wait_ms":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for storage.wal_batch_wait_ms", lineNo)
		}
		cfg.Storage.WALBatchWaitMS = n
	case "wal_batch_max_ops":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for storage.wal_batch_max_ops", lineNo)
		}
		cfg.Storage.WALBatchMaxOps = n
	case "wal_queue_size":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for storage.wal_queue_size", lineNo)
		}
		cfg.Storage.WALQueueSize = n
	case "active_partition_max_ops":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for storage.active_partition_max_ops", lineNo)
		}
		cfg.Storage.ActivePartitionMaxOps = n
	case "active_partition_max_bytes_mb":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for storage.active_partition_max_bytes_mb", lineNo)
		}
		cfg.Storage.ActivePartitionMaxBytesMB = n
	case "partition_max_count":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for storage.partition_max_count", lineNo)
		}
		cfg.Storage.PartitionMaxCount = n
	case "partition_merge_fan_in":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for storage.partition_merge_fan_in", lineNo)
		}
		cfg.Storage.PartitionMergeFanIn = n
	case "partition_index_min_rows":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for storage.partition_index_min_rows", lineNo)
		}
		cfg.Storage.PartitionIndexMinRows = n
	case "index_workers":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for storage.index_workers", lineNo)
		}
		cfg.Storage.IndexWorkers = n
	case "merge_workers":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for storage.merge_workers", lineNo)
		}
		cfg.Storage.MergeWorkers = n
	case "optimizer_queue_cap":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for storage.optimizer_queue_cap", lineNo)
		}
		cfg.Storage.OptimizerQueueCap = n
	case "index_m":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for storage.index_m", lineNo)
		}
		cfg.Storage.IndexM = n
	case "index_ef_construction":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for storage.index_ef_construction", lineNo)
		}
		cfg.Storage.IndexEfConstruction = n
	case "index_ef_search":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid int for storage.index_ef_search", lineNo)
		}
		cfg.Storage.IndexEfSearch = n
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
		// Legacy key: ignored.
	case "max_concurrent_write":
		// Legacy key: ignored.
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

func parseSecurityKey(cfg *AppConfig, key, value string, lineNo int) error {
	switch key {
	case "auth_enabled":
		b, err := parseBool(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid bool for security.auth_enabled", lineNo)
		}
		cfg.Security.AuthEnabled = b
	case "require_auth":
		b, err := parseBool(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid bool for security.require_auth", lineNo)
		}
		cfg.Security.RequireAuth = b
	case "api_key_header":
		cfg.Security.APIKeyHeader = strings.ToLower(strings.TrimSpace(value))
	case "tls_enabled":
		b, err := parseBool(value)
		if err != nil {
			return fmt.Errorf("line %d: invalid bool for security.tls_enabled", lineNo)
		}
		cfg.Security.TLSEnabled = b
	case "tls_cert_file":
		cfg.Security.TLSCertFile = strings.TrimSpace(value)
	case "tls_key_file":
		cfg.Security.TLSKeyFile = strings.TrimSpace(value)
	case "tls_client_auth":
		cfg.Security.TLSClientAuth = strings.ToLower(strings.TrimSpace(value))
	default:
		return fmt.Errorf("line %d: unknown key %q in [security]", lineNo, key)
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
compression = %s
write_mode = %s
wal_sync_mode = %s
wal_sync_interval_ms = %d
wal_batch_wait_ms = %d
wal_batch_max_ops = %d
wal_queue_size = %d
active_partition_max_ops = %d
active_partition_max_bytes_mb = %d
partition_max_count = %d
partition_merge_fan_in = %d
partition_index_min_rows = %d
index_workers = %d
merge_workers = %d
optimizer_queue_cap = %d
index_m = %d
index_ef_construction = %d
index_ef_search = %d

[guardrails]
max_top_k = %d
max_batch_size = %d
max_ef_search = %d
max_collection_dim = %d
max_data_dir_mb = %d
require_rpc_deadline = %t

[security]
auth_enabled = %t
require_auth = %t
api_key_header = %s
tls_enabled = %t
tls_cert_file = %s
tls_key_file = %s
tls_client_auth = %s
`, cfg.Server.Port, cfg.Server.GRPCMaxRecvMB, cfg.Server.GRPCMaxSendMB, cfg.Server.EnableReflection,
		cfg.Storage.DataDir, cfg.Storage.Shards, cfg.Storage.Compression, cfg.Storage.WriteMode,
		cfg.Storage.WALSyncMode, cfg.Storage.WALSyncIntervalMS, cfg.Storage.WALBatchWaitMS, cfg.Storage.WALBatchMaxOps, cfg.Storage.WALQueueSize,
		cfg.Storage.ActivePartitionMaxOps, cfg.Storage.ActivePartitionMaxBytesMB, cfg.Storage.PartitionMaxCount, cfg.Storage.PartitionMergeFanIn,
		cfg.Storage.PartitionIndexMinRows, cfg.Storage.IndexWorkers, cfg.Storage.MergeWorkers, cfg.Storage.OptimizerQueueCap,
		cfg.Storage.IndexM, cfg.Storage.IndexEfConstruction, cfg.Storage.IndexEfSearch,
		cfg.Guardrails.MaxTopK, cfg.Guardrails.MaxBatchSize, cfg.Guardrails.MaxEfSearch, cfg.Guardrails.MaxCollectionDim,
		cfg.Guardrails.MaxDataDirMB,
		cfg.Guardrails.RequireRPCDeadline,
		cfg.Security.AuthEnabled, cfg.Security.RequireAuth, cfg.Security.APIKeyHeader,
		cfg.Security.TLSEnabled, cfg.Security.TLSCertFile, cfg.Security.TLSKeyFile, cfg.Security.TLSClientAuth)
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
	switch c.Storage.Compression {
	case "none", "int8":
	default:
		c.Storage.Compression = defaults.Storage.Compression
	}
	switch c.Storage.WriteMode {
	case "strict", "performance":
	default:
		c.Storage.WriteMode = defaults.Storage.WriteMode
	}
	switch c.Storage.WALSyncMode {
	case "auto", "always", "periodic", "none":
	default:
		c.Storage.WALSyncMode = defaults.Storage.WALSyncMode
	}
	if c.Storage.WALSyncIntervalMS <= 0 {
		c.Storage.WALSyncIntervalMS = defaults.Storage.WALSyncIntervalMS
	}
	if c.Storage.WALBatchWaitMS <= 0 {
		c.Storage.WALBatchWaitMS = defaults.Storage.WALBatchWaitMS
	}
	if c.Storage.WALBatchMaxOps <= 0 {
		c.Storage.WALBatchMaxOps = defaults.Storage.WALBatchMaxOps
	}
	if c.Storage.WALQueueSize <= 0 {
		c.Storage.WALQueueSize = defaults.Storage.WALQueueSize
	}
	if c.Storage.ActivePartitionMaxOps <= 0 {
		c.Storage.ActivePartitionMaxOps = defaults.Storage.ActivePartitionMaxOps
	}
	if c.Storage.ActivePartitionMaxBytesMB <= 0 {
		c.Storage.ActivePartitionMaxBytesMB = defaults.Storage.ActivePartitionMaxBytesMB
	}
	if c.Storage.PartitionMaxCount <= 1 {
		c.Storage.PartitionMaxCount = defaults.Storage.PartitionMaxCount
	}
	if c.Storage.PartitionMergeFanIn <= 1 {
		c.Storage.PartitionMergeFanIn = defaults.Storage.PartitionMergeFanIn
	}
	if c.Storage.PartitionIndexMinRows <= 0 {
		c.Storage.PartitionIndexMinRows = defaults.Storage.PartitionIndexMinRows
	}
	if c.Storage.IndexWorkers <= 0 {
		c.Storage.IndexWorkers = defaults.Storage.IndexWorkers
	}
	if c.Storage.MergeWorkers <= 0 {
		c.Storage.MergeWorkers = defaults.Storage.MergeWorkers
	}
	if c.Storage.OptimizerQueueCap <= 0 {
		c.Storage.OptimizerQueueCap = defaults.Storage.OptimizerQueueCap
	}
	if c.Storage.IndexM <= 0 {
		c.Storage.IndexM = defaults.Storage.IndexM
	}
	if c.Storage.IndexEfConstruction <= 0 {
		c.Storage.IndexEfConstruction = defaults.Storage.IndexEfConstruction
	}
	if c.Storage.IndexEfSearch <= 0 {
		c.Storage.IndexEfSearch = defaults.Storage.IndexEfSearch
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
	if c.Guardrails.MaxDataDirMB < 0 {
		c.Guardrails.MaxDataDirMB = defaults.Guardrails.MaxDataDirMB
	}

	if strings.TrimSpace(c.Security.APIKeyHeader) == "" {
		c.Security.APIKeyHeader = defaults.Security.APIKeyHeader
	}
	c.Security.APIKeyHeader = strings.ToLower(strings.TrimSpace(c.Security.APIKeyHeader))
	if strings.TrimSpace(c.Security.TLSCertFile) == "" {
		c.Security.TLSCertFile = defaults.Security.TLSCertFile
	}
	if strings.TrimSpace(c.Security.TLSKeyFile) == "" {
		c.Security.TLSKeyFile = defaults.Security.TLSKeyFile
	}
	switch strings.ToLower(strings.TrimSpace(c.Security.TLSClientAuth)) {
	case "none", "request", "require_any", "verify_if_given", "require":
		c.Security.TLSClientAuth = strings.ToLower(strings.TrimSpace(c.Security.TLSClientAuth))
	default:
		c.Security.TLSClientAuth = defaults.Security.TLSClientAuth
	}
}
