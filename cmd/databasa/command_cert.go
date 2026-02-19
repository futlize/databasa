package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/futlize/databasa/internal/security"
)

func runCertCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: databasa cert <generate>")
	}
	switch args[0] {
	case "generate":
		return runCertGenerate(args[1:])
	default:
		return fmt.Errorf("unknown cert subcommand %q", args[0])
	}
}

func runCertGenerate(args []string) error {
	flagSet := flag.NewFlagSet("cert generate", flag.ContinueOnError)
	flagSet.SetOutput(os.Stdout)

	configPath := flagSet.String("config", resolvedDefaultConfigPath(), "path to databasa.toml")
	certFile := flagSet.String("cert-file", "", "output certificate path (defaults to [security].tls_cert_file)")
	keyFile := flagSet.String("key-file", "", "output key path (defaults to [security].tls_key_file)")
	force := flagSet.Bool("force", false, "overwrite existing certificate/key files")
	hostsCSV := flagSet.String("hosts", "", "comma-separated extra SAN hosts/ip addresses")
	if err := flagSet.Parse(args); err != nil {
		return err
	}

	cfg, _, err := LoadOrCreateConfig(*configPath)
	if err != nil {
		return err
	}

	targetCert := strings.TrimSpace(*certFile)
	if targetCert == "" {
		targetCert = cfg.Security.TLSCertFile
	}
	targetKey := strings.TrimSpace(*keyFile)
	if targetKey == "" {
		targetKey = cfg.Security.TLSKeyFile
	}
	if strings.TrimSpace(targetCert) == "" || strings.TrimSpace(targetKey) == "" {
		return errors.New("certificate and key paths cannot be empty")
	}

	cert, err := security.GenerateSelfSignedCertificate(security.CertGenerationOptions{
		CertFile: targetCert,
		KeyFile:  targetKey,
		Force:    *force,
		Hosts:    parseCSV(*hostsCSV),
	})
	if err != nil {
		return err
	}

	fmt.Printf("generated self-signed certificate\n")
	fmt.Printf("certificate: %s\n", cert.CertFile)
	fmt.Printf("private key: %s\n", cert.KeyFile)
	fmt.Printf("expires at:  %s\n", cert.NotAfter.UTC().Format("2006-01-02T15:04:05Z"))
	fmt.Printf("SAN hosts:   %s\n", strings.Join(cert.Hosts, ", "))
	fmt.Println("")
	fmt.Println("To use CA-signed certificates in production:")
	fmt.Printf("1. Replace %s with your CA-signed certificate chain.\n", cert.CertFile)
	fmt.Printf("2. Replace %s with the matching private key.\n", cert.KeyFile)
	fmt.Println("3. Ensure key permissions stay restricted (0600 for databasa service user).")
	fmt.Println("4. Restart databasa service.")
	return nil
}

func parseCSV(raw string) []string {
	parts := strings.Split(strings.TrimSpace(raw), ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			continue
		}
		out = append(out, item)
	}
	return out
}
