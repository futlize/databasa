package main

import (
	"testing"
)

func TestDefaultCLIConfigEnablesTLSAndAuth(t *testing.T) {
	cfg := defaultCLIConfig()
	if !cfg.Security.TLSEnabled {
		t.Fatalf("expected fallback tls enabled")
	}
	if !cfg.Security.AuthEnabled || !cfg.Security.RequireAuth {
		t.Fatalf("expected fallback auth enabled+required")
	}
}

func TestResolveCLIAddressUsesPortShortcut(t *testing.T) {
	addr, err := resolveCLIAddress(60001, 50051)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if addr != "127.0.0.1:60001" {
		t.Fatalf("unexpected addr: %s", addr)
	}
}

func TestResolveCLIAddressUsesDefaultPortFallback(t *testing.T) {
	addr, err := resolveCLIAddress(0, 51111)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if addr != "127.0.0.1:51111" {
		t.Fatalf("unexpected addr: %s", addr)
	}
}

func TestResolveCLIAddressRejectsInvalidPort(t *testing.T) {
	if _, err := resolveCLIAddress(70000, 50051); err == nil {
		t.Fatalf("expected invalid port error")
	}
}

func TestResolveCLIAddressUsesDefaultPortWhenUnset(t *testing.T) {
	addr, err := resolveCLIAddress(0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if addr != "127.0.0.1:50051" {
		t.Fatalf("unexpected addr: %s", addr)
	}
}
