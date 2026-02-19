package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/futlize/databasa/internal/hnsw"
	"github.com/futlize/databasa/internal/security"
	"github.com/futlize/databasa/internal/server"
	"github.com/futlize/databasa/internal/storage"
	pb "github.com/futlize/databasa/pkg/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
)

func main() {
	log.SetFlags(log.LstdFlags)

	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "auth":
			if err := runAuthCommand(os.Args[2:]); err != nil {
				log.Fatalf("auth command failed: %v", err)
			}
			return
		case "cert":
			if err := runCertCommand(os.Args[2:]); err != nil {
				log.Fatalf("certificate command failed: %v", err)
			}
			return
		}
	}
	runServer(os.Args[1:])
}

func runServer(args []string) {
	defaultPath := defaultConfigPath
	if envPath := strings.TrimSpace(os.Getenv("DATABASA_CONFIG")); envPath != "" {
		defaultPath = envPath
	}

	flagSet := flag.NewFlagSet("databasa", flag.ContinueOnError)
	configPath := flagSet.String("config", defaultPath, "path to Databasa config file (TOML style). Env override: DATABASA_CONFIG")
	if err := flagSet.Parse(args); err != nil {
		fatalStartup("parse command flags", err)
	}

	cfg, created, err := LoadOrCreateConfig(*configPath)
	if err != nil {
		fatalStartup("load configuration", err)
	}
	if created {
		log.Printf("INFO created default config at %s", *configPath)
	}

	server.SetDefaultRuntimeConfig(server.RuntimeConfig{
		WriteAdmissionMode: cfg.Storage.WriteMode,
	})
	walMode := walSyncModeFromConfig(cfg.Storage.WriteMode, cfg.Storage.WALSyncMode)
	storageOpts := storage.Options{
		ActivePartitionMaxOps:   cfg.Storage.ActivePartitionMaxOps,
		ActivePartitionMaxBytes: uint64(cfg.Storage.ActivePartitionMaxBytesMB) * 1024 * 1024,
		PartitionMaxCount:       cfg.Storage.PartitionMaxCount,
		PartitionMergeFanIn:     cfg.Storage.PartitionMergeFanIn,
		PartitionIndexMinRows:   cfg.Storage.PartitionIndexMinRows,
		SealCheckInterval:       250 * time.Millisecond,
		MergeCheckInterval:      2 * time.Second,
		IndexRescanEvery:        1 * time.Second,
		IndexWorkers:            cfg.Storage.IndexWorkers,
		MergeWorkers:            cfg.Storage.MergeWorkers,
		OptimizerQueueCap:       cfg.Storage.OptimizerQueueCap,
		WALSyncMode:             walMode,
		WALQueueSize:            cfg.Storage.WALQueueSize,
		WALBatchMaxOps:          cfg.Storage.WALBatchMaxOps,
		WALBatchWait:            time.Duration(cfg.Storage.WALBatchWaitMS) * time.Millisecond,
		WALSyncInterval:         time.Duration(cfg.Storage.WALSyncIntervalMS) * time.Millisecond,
		ANNEnabled:              false,
		HNSWConfig: hnsw.Config{
			M:              cfg.Storage.IndexM,
			MMax0:          cfg.Storage.IndexM * 2,
			EfConstruction: cfg.Storage.IndexEfConstruction,
			EfSearch:       cfg.Storage.IndexEfSearch,
		},
	}
	server.SetDefaultStorageOptions(storageOpts)

	// Initialize datastore service state before opening listeners.
	srv, err := server.NewWithConfig(
		cfg.Storage.DataDir,
		cfg.Storage.Shards,
		cfg.Storage.Compression,
	)
	if err != nil {
		fatalStartup("initialize server", err)
	}
	defer srv.Close()
	srv.ConfigureGuardrails(server.GuardrailConfig{
		MaxTopK:            uint32(cfg.Guardrails.MaxTopK),
		MaxBatchSize:       cfg.Guardrails.MaxBatchSize,
		MaxEfSearch:        uint32(cfg.Guardrails.MaxEfSearch),
		MaxCollectionDim:   uint32(cfg.Guardrails.MaxCollectionDim),
		MaxDataDirBytes:    int64(cfg.Guardrails.MaxDataDirMB) * 1024 * 1024,
		RequireRPCDeadline: cfg.Guardrails.RequireRPCDeadline,
	})

	authenticator, err := security.NewAuthenticator(security.AuthConfig{
		Enabled:      cfg.Security.AuthEnabled,
		RequireAuth:  cfg.Security.RequireAuth,
		APIKeyHeader: cfg.Security.APIKeyHeader,
	}, security.AuthStorePath(cfg.Storage.DataDir))
	if err != nil {
		fatalStartup("initialize authentication", err)
	}

	grpcOptions := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(cfg.Server.GRPCMaxRecvMB * 1024 * 1024),
		grpc.MaxSendMsgSize(cfg.Server.GRPCMaxSendMB * 1024 * 1024),
		grpc.ChainUnaryInterceptor(authenticator.UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(authenticator.StreamServerInterceptor()),
		grpc.StatsHandler(authenticator),
	}

	tlsConfig, err := buildServerTLSConfig(cfg.Security)
	if err != nil {
		fatalStartup("configure tls", err)
	}
	if tlsConfig != nil {
		grpcOptions = append(grpcOptions, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}

	grpcServer := grpc.NewServer(grpcOptions...)
	pb.RegisterDatabasaServer(grpcServer, srv)

	if cfg.Server.EnableReflection {
		reflection.Register(grpcServer)
	}

	addr := fmt.Sprintf(":%d", cfg.Server.Port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		fatalStartup(fmt.Sprintf("bind listener on %s", addr), err)
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down gracefully...")
		grpcServer.GracefulStop()
	}()

	logStartupInitializationReport(cfg, *configPath, storageOpts, walMode, srv, addr)
	fmt.Println("databasa ready: accepting requests")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func buildServerTLSConfig(cfg SecurityConfig) (*tls.Config, error) {
	if !cfg.TLSEnabled {
		return nil, nil
	}
	certFile := strings.TrimSpace(cfg.TLSCertFile)
	keyFile := strings.TrimSpace(cfg.TLSKeyFile)
	if certFile == "" || keyFile == "" {
		return nil, errors.New("tls_enabled=true requires tls_cert_file and tls_key_file")
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load tls certificate/key pair: %w", err)
	}

	tlsConfig := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"h2"},
	}

	mode := strings.ToLower(strings.TrimSpace(cfg.TLSClientAuth))
	switch mode {
	case "", "none":
		tlsConfig.ClientAuth = tls.NoClientCert
	case "request":
		tlsConfig.ClientAuth = tls.RequestClientCert
	case "require_any":
		tlsConfig.ClientAuth = tls.RequireAnyClientCert
	case "verify_if_given":
		tlsConfig.ClientAuth = tls.VerifyClientCertIfGiven
	case "require":
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	default:
		return nil, fmt.Errorf("invalid tls_client_auth value %q", cfg.TLSClientAuth)
	}

	if tlsConfig.ClientAuth == tls.VerifyClientCertIfGiven || tlsConfig.ClientAuth == tls.RequireAndVerifyClientCert {
		pool, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("load system cert pool for mTLS verification: %w", err)
		}
		if pool == nil {
			return nil, errors.New("system cert pool is unavailable for mTLS verification")
		}
		tlsConfig.ClientCAs = pool
	}
	return tlsConfig, nil
}

func walSyncModeFromConfig(writeMode string, walSyncMode string) storage.WALSyncMode {
	mode := strings.ToLower(strings.TrimSpace(writeMode))
	wal := storage.WALSyncAlways
	if mode == "performance" {
		wal = storage.WALSyncPeriodic
	}

	switch strings.ToLower(strings.TrimSpace(walSyncMode)) {
	case "always":
		return storage.WALSyncAlways
	case "periodic":
		return storage.WALSyncPeriodic
	case "none":
		return storage.WALSyncNone
	default:
		return wal
	}
}
