package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadCLIConfigMissingDoesNotCreateFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "databasa.toml")

	cfg, loaded, err := loadCLIConfig(path)
	if err != nil {
		t.Fatalf("expected no error for missing config, got %v", err)
	}
	if loaded {
		t.Fatalf("expected loaded=false for missing config")
	}
	if cfg.Server.Port != DefaultAppConfig().Server.Port {
		t.Fatalf("unexpected default server port: got=%d", cfg.Server.Port)
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("expected config file to remain absent, stat err=%v", statErr)
	}
}

func TestLoadCLIConfigParsesExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "databasa.toml")
	body := `
[server]
port = 51111

[security]
auth_enabled = true
require_auth = true
tls_enabled = true
`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, loaded, err := loadCLIConfig(path)
	if err != nil {
		t.Fatalf("expected config load success, got %v", err)
	}
	if !loaded {
		t.Fatalf("expected loaded=true")
	}
	if cfg.Server.Port != 51111 {
		t.Fatalf("expected port=51111, got %d", cfg.Server.Port)
	}
	if !cfg.Security.TLSEnabled {
		t.Fatalf("expected tls_enabled=true")
	}
}

func TestDefaultCLIConfigEnablesTLSAndAuth(t *testing.T) {
	cfg := defaultCLIConfig()
	if !cfg.Security.TLSEnabled {
		t.Fatalf("expected fallback tls enabled")
	}
	if !cfg.Security.AuthEnabled || !cfg.Security.RequireAuth {
		t.Fatalf("expected fallback auth enabled+required")
	}
}
