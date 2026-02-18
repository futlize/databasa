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
index_flush_ops = 0

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
	if cfg.Storage.IndexFlushOps != 0 {
		t.Fatalf("expected explicit index_flush_ops=0 to be preserved, got=%d", cfg.Storage.IndexFlushOps)
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
