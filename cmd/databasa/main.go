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

	"github.com/futlize/databasa/internal/server"
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

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	cfg, created, err := LoadOrCreateConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	if created {
		log.Printf("Created default config at %s", *configPath)
	}

	if envBulk := strings.TrimSpace(os.Getenv("BULK_LOAD_MODE")); envBulk != "" {
		b, err := parseBool(envBulk)
		if err != nil {
			log.Fatalf("Invalid BULK_LOAD_MODE=%q: %v", envBulk, err)
		}
		cfg.Resources.BulkLoadMode = b
	}

	server.SetDefaultRuntimeConfig(server.RuntimeConfig{
		MaxWorkers:          cfg.Resources.MaxWorkers,
		MemoryBudgetPercent: cfg.Resources.MemoryBudgetPercent,
		InsertQueueSize:     cfg.Resources.InsertQueueSize,
		BulkLoadMode:        cfg.Resources.BulkLoadMode,
	})

	// Initialize server
	srv, err := server.NewWithConfig(
		cfg.Storage.DataDir,
		cfg.Storage.Shards,
		cfg.Storage.IndexFlushOps,
		cfg.Storage.Compression,
	)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}
	defer srv.Close()
	srv.ConfigureGuardrails(server.GuardrailConfig{
		MaxTopK:             uint32(cfg.Guardrails.MaxTopK),
		MaxBatchSize:        cfg.Guardrails.MaxBatchSize,
		MaxEfSearch:         uint32(cfg.Guardrails.MaxEfSearch),
		MaxCollectionDim:    uint32(cfg.Guardrails.MaxCollectionDim),
		MaxConcurrentSearch: cfg.Guardrails.MaxConcurrentSearch,
		MaxConcurrentWrite:  cfg.Guardrails.MaxConcurrentWrite,
		MaxDataDirBytes:     int64(cfg.Guardrails.MaxDataDirMB) * 1024 * 1024,
		RequireRPCDeadline:  cfg.Guardrails.RequireRPCDeadline,
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
		log.Fatalf("Failed to listen on %s: %v", addr, err)
	}

	// Graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down gracefully...")
		grpcServer.GracefulStop()
	}()

	log.Printf("Databasa server listening on %s", addr)
	log.Printf("Config file: %s", *configPath)
	log.Printf("Data directory: %s", cfg.Storage.DataDir)
	log.Printf("Guardrails: max_top_k=%d max_batch_size=%d max_ef_search=%d max_collection_dim=%d max_concurrent_search=%d max_concurrent_write=%d max_data_dir_mb=%d require_rpc_deadline=%t",
		cfg.Guardrails.MaxTopK, cfg.Guardrails.MaxBatchSize, cfg.Guardrails.MaxEfSearch, cfg.Guardrails.MaxCollectionDim,
		cfg.Guardrails.MaxConcurrentSearch, cfg.Guardrails.MaxConcurrentWrite, cfg.Guardrails.MaxDataDirMB, cfg.Guardrails.RequireRPCDeadline)
	log.Printf("Resources: max_workers=%d memory_budget_percent=%d insert_queue_size=%d bulk_load_mode=%t",
		cfg.Resources.MaxWorkers, cfg.Resources.MemoryBudgetPercent, cfg.Resources.InsertQueueSize, cfg.Resources.BulkLoadMode)
	log.Printf("gRPC limits: max_recv_mb=%d max_send_mb=%d reflection=%t", cfg.Server.GRPCMaxRecvMB, cfg.Server.GRPCMaxSendMB, cfg.Server.EnableReflection)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
