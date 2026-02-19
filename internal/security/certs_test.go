package security

import (
	"crypto/tls"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestGenerateSelfSignedCertificate(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "certs", "server.crt")
	keyFile := filepath.Join(dir, "certs", "server.key")

	out, err := GenerateSelfSignedCertificate(CertGenerationOptions{
		CertFile: certFile,
		KeyFile:  keyFile,
		Hosts:    []string{"db.example.local"},
	})
	if err != nil {
		t.Fatalf("generate cert: %v", err)
	}
	if out.CertFile != certFile || out.KeyFile != keyFile {
		t.Fatalf("unexpected output paths: %+v", out)
	}
	if len(out.Hosts) == 0 {
		t.Fatalf("expected SAN hosts")
	}
	if _, err := tls.LoadX509KeyPair(certFile, keyFile); err != nil {
		t.Fatalf("generated cert/key pair is invalid: %v", err)
	}

	if runtime.GOOS != "windows" {
		keyInfo, err := os.Stat(keyFile)
		if err != nil {
			t.Fatalf("stat key file: %v", err)
		}
		if got := keyInfo.Mode().Perm(); got != 0o600 {
			t.Fatalf("expected key mode 0600, got %o", got)
		}
	}
}

func TestGenerateSelfSignedCertificateRespectsForceFlag(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "server.crt")
	keyFile := filepath.Join(dir, "server.key")

	if _, err := GenerateSelfSignedCertificate(CertGenerationOptions{
		CertFile: certFile,
		KeyFile:  keyFile,
	}); err != nil {
		t.Fatalf("initial generation failed: %v", err)
	}

	if _, err := GenerateSelfSignedCertificate(CertGenerationOptions{
		CertFile: certFile,
		KeyFile:  keyFile,
	}); err == nil {
		t.Fatalf("expected overwrite without force to fail")
	}

	if _, err := GenerateSelfSignedCertificate(CertGenerationOptions{
		CertFile: certFile,
		KeyFile:  keyFile,
		Force:    true,
	}); err != nil {
		t.Fatalf("expected generation with force to succeed: %v", err)
	}
}
