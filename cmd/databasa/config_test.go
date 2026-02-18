package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadOrCreateConfigCreatesDefaultFile(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "databasa.toml")

	cfg, created, err := LoadOrCreateConfig(cfgPath)
	if err != nil {
		t.Fatalf("load or create config: %v", err)
	}
	if !created {
		t.Fatalf("expected config file to be created")
	}
	if _, err := os.Stat(cfgPath); err != nil {
		t.Fatalf("expected config file on disk: %v", err)
	}

	defaults := DefaultAppConfig()
	if cfg.Server.Port != defaults.Server.Port {
		t.Fatalf("unexpected default port: got=%d want=%d", cfg.Server.Port, defaults.Server.Port)
	}
	if cfg.Storage.DataDir != defaults.Storage.DataDir {
		t.Fatalf("unexpected default data dir: got=%q want=%q", cfg.Storage.DataDir, defaults.Storage.DataDir)
	}
}

func TestLoadOrCreateConfigMergesWithDefaults(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "databasa.toml")
	content := `
[server]
port = 60051 ; grpc port

[storage]
data_dir = ./custom-data
write_mode = performance

[guardrails]
max_top_k = 42 # max returned docs
max_data_dir_mb = 256
`
	if err := os.WriteFile(cfgPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, created, err := LoadOrCreateConfig(cfgPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if created {
		t.Fatalf("did not expect config creation when file already exists")
	}

	defaults := DefaultAppConfig()
	if cfg.Server.Port != 60051 {
		t.Fatalf("unexpected port: %d", cfg.Server.Port)
	}
	if cfg.Storage.DataDir != "./custom-data" {
		t.Fatalf("unexpected data dir: %q", cfg.Storage.DataDir)
	}
	if cfg.Storage.WriteMode != "performance" {
		t.Fatalf("unexpected write_mode: %q", cfg.Storage.WriteMode)
	}
	if cfg.Guardrails.MaxTopK != 42 {
		t.Fatalf("unexpected max_top_k: %d", cfg.Guardrails.MaxTopK)
	}
	if cfg.Guardrails.MaxDataDirMB != 256 {
		t.Fatalf("unexpected max_data_dir_mb: %d", cfg.Guardrails.MaxDataDirMB)
	}
	if cfg.Guardrails.MaxBatchSize != defaults.Guardrails.MaxBatchSize {
		t.Fatalf("expected default max_batch_size=%d, got=%d", defaults.Guardrails.MaxBatchSize, cfg.Guardrails.MaxBatchSize)
	}
}

func TestLoadOrCreateConfigNormalizesInvalidWriteMode(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "databasa.toml")
	content := `
[storage]
write_mode = ultra
wal_sync_mode = unknown
`
	if err := os.WriteFile(cfgPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, created, err := LoadOrCreateConfig(cfgPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if created {
		t.Fatalf("did not expect config creation when file already exists")
	}

	if cfg.Storage.WriteMode != "strict" {
		t.Fatalf("expected normalized write_mode=strict, got=%q", cfg.Storage.WriteMode)
	}
	if cfg.Storage.WALSyncMode != "auto" {
		t.Fatalf("expected normalized wal_sync_mode=auto, got=%q", cfg.Storage.WALSyncMode)
	}
}

func TestLoadOrCreateConfigIgnoresLegacyResourcesSection(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "databasa.toml")
	content := `
[storage]
data_dir = ./data

[resources]
max_workers = 64
memory_budget_percent = 99
insert_queue_size = 1
bulk_load_mode = true
`
	if err := os.WriteFile(cfgPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, created, err := LoadOrCreateConfig(cfgPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if created {
		t.Fatalf("did not expect config creation when file already exists")
	}

	if cfg.Storage.DataDir != "./data" {
		t.Fatalf("unexpected data_dir: %q", cfg.Storage.DataDir)
	}
}
