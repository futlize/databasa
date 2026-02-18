package server

import "testing"

func TestNormalizeRuntimeConfigWriteAdmissionMode(t *testing.T) {
	cfg := normalizeRuntimeConfig(RuntimeConfig{WriteAdmissionMode: "performance"})
	if cfg.WriteAdmissionMode != "performance" {
		t.Fatalf("expected performance mode, got %q", cfg.WriteAdmissionMode)
	}

	cfg = normalizeRuntimeConfig(RuntimeConfig{WriteAdmissionMode: "invalid"})
	if cfg.WriteAdmissionMode != "strict" {
		t.Fatalf("expected strict mode fallback, got %q", cfg.WriteAdmissionMode)
	}
}
