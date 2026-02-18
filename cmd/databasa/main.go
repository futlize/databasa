package main

import (
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
	"github.com/futlize/databasa/internal/server"
	"github.com/futlize/databasa/internal/storage"
	pb "github.com/futlize/databasa/pkg/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	defaultPath := defaultConfigPath
	if envPath := strings.TrimSpace(os.Getenv("DATABASA_CONFIG")); envPath != "" {
		defaultPath = envPath
	}

	configPath := flag.String("config", defaultPath, "path to Databasa config file (TOML style). Env override: DATABASA_CONFIG")
	flag.Parse()

	log.SetFlags(log.LstdFlags)

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

	// Initialize server
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

	// Create gRPC server
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(cfg.Server.GRPCMaxRecvMB*1024*1024),
		grpc.MaxSendMsgSize(cfg.Server.GRPCMaxSendMB*1024*1024),
	)
	pb.RegisterDatabasaServer(grpcServer, srv)

	if cfg.Server.EnableReflection {
		reflection.Register(grpcServer)
	}

	// Listen
	addr := fmt.Sprintf(":%d", cfg.Server.Port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		fatalStartup(fmt.Sprintf("bind listener on %s", addr), err)
	}

	// Graceful shutdown
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
