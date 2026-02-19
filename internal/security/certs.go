package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type CertGenerationOptions struct {
	CertFile string
	KeyFile  string
	Force    bool
	Hosts    []string
}

type GeneratedCertificate struct {
	CertFile string
	KeyFile  string
	NotAfter time.Time
	Hosts    []string
}

func GenerateSelfSignedCertificate(opts CertGenerationOptions) (GeneratedCertificate, error) {
	certFile := strings.TrimSpace(opts.CertFile)
	keyFile := strings.TrimSpace(opts.KeyFile)
	if certFile == "" || keyFile == "" {
		return GeneratedCertificate{}, errors.New("certificate and key paths are required")
	}
	if certFile == keyFile {
		return GeneratedCertificate{}, errors.New("certificate and key path must be different")
	}
	if !opts.Force {
		if _, err := os.Stat(certFile); err == nil {
			return GeneratedCertificate{}, fmt.Errorf("certificate file already exists: %s", certFile)
		}
		if _, err := os.Stat(keyFile); err == nil {
			return GeneratedCertificate{}, fmt.Errorf("key file already exists: %s", keyFile)
		}
	}

	now := time.Now().UTC()
	notAfter := now.AddDate(1, 0, 0)
	if notAfter.Before(now) {
		notAfter = now.Add(365 * 24 * time.Hour)
	}

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return GeneratedCertificate{}, fmt.Errorf("generate private key: %w", err)
	}

	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return GeneratedCertificate{}, fmt.Errorf("generate certificate serial: %w", err)
	}

	hosts := normalizeHosts(opts.Hosts)
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: "databasa",
		},
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	for _, host := range hosts {
		if ip := net.ParseIP(host); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
			continue
		}
		template.DNSNames = append(template.DNSNames, host)
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return GeneratedCertificate{}, fmt.Errorf("create certificate: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: der,
	})
	keyBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return GeneratedCertificate{}, fmt.Errorf("marshal private key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: keyBytes,
	})

	if err := os.MkdirAll(filepath.Dir(certFile), 0o750); err != nil {
		return GeneratedCertificate{}, fmt.Errorf("create certificate directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(keyFile), 0o750); err != nil {
		return GeneratedCertificate{}, fmt.Errorf("create key directory: %w", err)
	}

	if err := writeFileAtomic(certFile, certPEM, 0o644); err != nil {
		return GeneratedCertificate{}, fmt.Errorf("write certificate: %w", err)
	}
	if err := writeFileAtomic(keyFile, keyPEM, 0o600); err != nil {
		return GeneratedCertificate{}, fmt.Errorf("write private key: %w", err)
	}

	return GeneratedCertificate{
		CertFile: certFile,
		KeyFile:  keyFile,
		NotAfter: notAfter,
		Hosts:    hosts,
	}, nil
}

func normalizeHosts(input []string) []string {
	seen := make(map[string]struct{}, len(input)+4)
	out := make([]string, 0, len(input)+4)
	appendHost := func(host string) {
		host = strings.TrimSpace(host)
		if host == "" {
			return
		}
		if _, ok := seen[host]; ok {
			return
		}
		seen[host] = struct{}{}
		out = append(out, host)
	}

	appendHost("localhost")
	appendHost("127.0.0.1")
	appendHost("::1")

	if name, err := os.Hostname(); err == nil {
		appendHost(name)
	}

	for _, host := range input {
		appendHost(host)
	}
	return out
}

func writeFileAtomic(path string, body []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		_ = tmp.Close()
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()

	if err := tmp.Chmod(mode); err != nil {
		return err
	}
	if _, err := tmp.Write(body); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	if err := os.Chmod(path, mode); err != nil {
		return err
	}
	cleanup = false
	return nil
}
