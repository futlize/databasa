package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/futlize/databasa/internal/security"
	"github.com/futlize/databasa/internal/server"
	pb "github.com/futlize/databasa/pkg/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	wrapperspb "google.golang.org/protobuf/types/known/wrapperspb"
)

type config struct {
	addr                string
	collection          string
	metric              string
	createIndexOnCreate bool
	indexM              int
	indexEfConstruction int
	vectors             int
	dimension           int
	batchSize           int
	workers             int
	rpcMode             string
	dialTimeout         time.Duration
	rpcTimeout          time.Duration
	progressEvery       time.Duration
	dropFirst           bool
	apiKey              string

	embedded             bool
	embeddedListen       string
	embeddedDataDir      string
	embeddedShards       int
	embeddedCompression  string
	embeddedKeepDataPath bool
	embeddedDevNoAuth    bool
}

type batchJob struct {
	start int
	count int
}

const (
	maxBatchInsertRetries = 8
	fixedInsertCount      = 200000
)

func main() {
	cfg := parseFlags()
	if err := cfg.validate(); err != nil {
		log.Fatalf("invalid flags: %v", err)
	}
	if err := run(cfg); err != nil {
		log.Fatalf("benchmark failed: %v", err)
	}
}

func parseFlags() config {
	cfg := config{}
	flag.StringVar(&cfg.addr, "addr", "127.0.0.1:50051", "Databasa gRPC address (used when -embedded=false)")
	flag.StringVar(&cfg.collection, "collection", "benchmark_insert", "collection used for benchmark")
	flag.StringVar(&cfg.metric, "metric", "cosine", "collection metric: cosine|l2|dot_product")
	flag.BoolVar(&cfg.createIndexOnCreate, "create-index-on-create", true, "create HNSW index right after creating the collection")
	flag.IntVar(&cfg.indexM, "index-m", 16, "HNSW M (max connections per node)")
	flag.IntVar(&cfg.indexEfConstruction, "index-ef-construction", 200, "HNSW ef_construction (build-time beam width)")
	flag.IntVar(&cfg.vectors, "vectors", fixedInsertCount, "number of vectors to insert")
	flag.IntVar(&cfg.dimension, "dimension", 128, "vector dimension")
	flag.IntVar(&cfg.batchSize, "batch-size", 512, "vectors per BatchInsert call")
	flag.IntVar(&cfg.workers, "workers", 1, "number of concurrent workers")
	flag.StringVar(&cfg.rpcMode, "rpc-mode", "batch", "insert RPC mode: batch|insert")
	flag.DurationVar(&cfg.dialTimeout, "dial-timeout", 15*time.Second, "timeout for initial gRPC dial")
	flag.DurationVar(&cfg.rpcTimeout, "rpc-timeout", 30*time.Second, "timeout for each gRPC request")
	flag.DurationVar(&cfg.progressEvery, "progress-every", 5*time.Second, "progress report interval (0 disables)")
	flag.BoolVar(&cfg.dropFirst, "drop-first", true, "delete collection before creating it")
	flag.StringVar(&cfg.apiKey, "api-key", "", "API key used once for Login on the gRPC channel")

	flag.BoolVar(&cfg.embedded, "embedded", false, "run benchmark with an embedded in-process Databasa server")
	flag.StringVar(&cfg.embeddedListen, "embedded-listen", "127.0.0.1:0", "listen address for embedded server")
	flag.StringVar(&cfg.embeddedDataDir, "embedded-data-dir", "", "data directory for embedded server (default: temporary under ./data)")
	flag.IntVar(&cfg.embeddedShards, "embedded-shards", 8, "number of shards for embedded server")
	flag.StringVar(&cfg.embeddedCompression, "embedded-compression", "int8", "compression for embedded server: int8|none")
	flag.BoolVar(&cfg.embeddedKeepDataPath, "keep-data", false, "keep embedded data directory after benchmark")
	flag.BoolVar(&cfg.embeddedDevNoAuth, "embedded-dev-no-auth", false, "disable auth for embedded mode only (insecure, local development only)")

	flag.Parse()
	return cfg
}

func (c config) validate() error {
	if c.vectors <= 0 {
		return errors.New("vectors must be > 0")
	}
	if c.dimension <= 0 {
		return errors.New("dimension must be > 0")
	}
	if c.batchSize <= 0 {
		return errors.New("batch-size must be > 0")
	}
	if c.workers <= 0 {
		return errors.New("workers must be > 0")
	}
	if c.workers > c.vectors {
		return errors.New("workers must be <= vectors")
	}
	switch strings.ToLower(strings.TrimSpace(c.rpcMode)) {
	case "batch", "insert":
	default:
		return errors.New("rpc-mode must be batch or insert")
	}
	if c.progressEvery < 0 {
		return errors.New("progress-every must be >= 0")
	}
	if c.indexM <= 0 {
		return errors.New("index-m must be > 0")
	}
	if c.indexEfConstruction <= 0 {
		return errors.New("index-ef-construction must be > 0")
	}
	if !c.embedded && c.addr == "" {
		return errors.New("addr is required when embedded=false")
	}
	switch strings.ToLower(strings.TrimSpace(c.embeddedCompression)) {
	case "int8", "none":
	default:
		return errors.New("embedded-compression must be int8 or none")
	}
	return nil
}

func run(cfg config) error {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf("configured insert count: %d", cfg.vectors)

	targetAddr := cfg.addr
	embeddedAPIKey := ""
	cleanup := func() {}

	if cfg.embedded {
		embeddedAddr, stop, dataDir, provisionedKey, err := startEmbeddedServer(cfg)
		if err != nil {
			return err
		}
		targetAddr = embeddedAddr
		embeddedAPIKey = provisionedKey
		cleanup = stop
		defer cleanup()
		authMode := "session-auth"
		if cfg.embeddedDevNoAuth {
			authMode = "dev-no-auth (insecure)"
		}
		log.Printf("embedded server: addr=%s data_dir=%s shards=%d compression=%s auth=%s", targetAddr, dataDir, cfg.embeddedShards, cfg.embeddedCompression, authMode)
	} else {
		log.Printf("external server: addr=%s", targetAddr)
	}

	connCtx, connCancel := context.WithTimeout(context.Background(), cfg.dialTimeout)
	defer connCancel()

	conn, err := grpc.DialContext(
		connCtx,
		targetAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("dial %s: %w", targetAddr, err)
	}
	defer conn.Close()

	client := pb.NewDatabasaClient(conn)
	apiKey := strings.TrimSpace(cfg.apiKey)
	if apiKey == "" {
		apiKey = embeddedAPIKey
	}
	if apiKey != "" {
		if err := login(context.Background(), client, apiKey, cfg.rpcTimeout); err != nil {
			return err
		}
	} else if cfg.embedded && !cfg.embeddedDevNoAuth {
		return errors.New("embedded server is running in auth mode but no API key was available for Login")
	}

	metric, err := parseMetric(cfg.metric)
	if err != nil {
		return err
	}

	if cfg.dropFirst {
		if err := deleteCollection(context.Background(), client, cfg.collection, cfg.rpcTimeout); err != nil {
			return err
		}
	}
	createdCollection := false
	if err := createCollection(context.Background(), client, cfg.collection, uint32(cfg.dimension), metric, cfg.rpcTimeout); err != nil {
		if status.Code(err) != codes.AlreadyExists {
			return err
		}
		log.Printf("collection already exists: %s", cfg.collection)
	} else {
		createdCollection = true
	}

	if createdCollection && cfg.createIndexOnCreate {
		log.Printf(
			"creating index on new collection: collection=%s m=%d ef_construction=%d",
			cfg.collection,
			cfg.indexM,
			cfg.indexEfConstruction,
		)
		if err := createIndex(context.Background(), client, cfg.collection, uint32(cfg.indexM), uint32(cfg.indexEfConstruction), cfg.rpcTimeout); err != nil {
			return err
		}
	}
	if !createdCollection && cfg.createIndexOnCreate {
		log.Printf("skipping create-index-on-create because collection already existed: %s", cfg.collection)
	}

	log.Printf(
		"starting benchmark: vectors=%d dimension=%d batch_size=%d workers=%d collection=%s metric=%s create_index_on_create=%t",
		cfg.vectors,
		cfg.dimension,
		cfg.batchSize,
		cfg.workers,
		cfg.collection,
		strings.ToLower(cfg.metric),
		cfg.createIndexOnCreate,
	)

	start := time.Now()
	inserted, err := runInsertBenchmark(client, cfg)
	elapsed := time.Since(start)
	if err != nil {
		return err
	}

	if inserted == 0 {
		return errors.New("no vectors inserted")
	}

	throughput := float64(inserted) / elapsed.Seconds()
	batches := (cfg.vectors + cfg.batchSize - 1) / cfg.batchSize
	log.Printf(
		"benchmark complete: inserted=%d elapsed=%s throughput=%.2f vectors/s batches=%d",
		inserted,
		elapsed.Round(time.Millisecond),
		throughput,
		batches,
	)
	return nil
}

func startEmbeddedServer(cfg config) (addr string, stop func(), dataDir string, apiKey string, err error) {
	dataDir = cfg.embeddedDataDir
	if dataDir == "" {
		if mkErr := os.MkdirAll("data", 0o755); mkErr != nil {
			return "", nil, "", "", fmt.Errorf("create data dir: %w", mkErr)
		}
		tempDir, mkErr := os.MkdirTemp("data", "benchmark-insert-")
		if mkErr != nil {
			return "", nil, "", "", fmt.Errorf("create temp data dir: %w", mkErr)
		}
		dataDir = tempDir
	}

	srv, err := server.NewWithConfig(dataDir, cfg.embeddedShards, strings.ToLower(cfg.embeddedCompression))
	if err != nil {
		return "", nil, "", "", fmt.Errorf("create embedded server: %w", err)
	}
	srv.ConfigureGuardrails(server.GuardrailConfig{
		MaxBatchSize: cfg.batchSize,
	})

	grpcOptions := make([]grpc.ServerOption, 0, 3)
	if !cfg.embeddedDevNoAuth {
		storePath := security.AuthStorePath(dataDir)
		manager := security.NewManager(storePath)
		provisioned, keyErr := ensureBenchmarkAdminKey(manager)
		if keyErr != nil {
			srv.Close()
			return "", nil, "", "", keyErr
		}
		apiKey = provisioned

		auth, authErr := security.NewAuthenticator(security.AuthConfig{
			Enabled:      true,
			RequireAuth:  true,
			APIKeyHeader: "authorization",
		}, storePath)
		if authErr != nil {
			srv.Close()
			return "", nil, "", "", fmt.Errorf("initialize embedded authenticator: %w", authErr)
		}
		grpcOptions = append(grpcOptions,
			grpc.ChainUnaryInterceptor(auth.UnaryServerInterceptor()),
			grpc.ChainStreamInterceptor(auth.StreamServerInterceptor()),
			grpc.StatsHandler(auth),
		)
	}

	grpcServer := grpc.NewServer(grpcOptions...)
	pb.RegisterDatabasaServer(grpcServer, srv)

	lis, err := net.Listen("tcp", cfg.embeddedListen)
	if err != nil {
		srv.Close()
		return "", nil, "", "", fmt.Errorf("listen on %s: %w", cfg.embeddedListen, err)
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- grpcServer.Serve(lis)
	}()

	stop = func() {
		done := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(10 * time.Second):
			grpcServer.Stop()
		}

		srv.Close()
		_ = lis.Close()

		if !cfg.embeddedKeepDataPath {
			_ = os.RemoveAll(dataDir)
		}

		select {
		case serveErrVal := <-serveErr:
			if serveErrVal != nil && !errors.Is(serveErrVal, grpc.ErrServerStopped) {
				log.Printf("embedded server stop error: %v", serveErrVal)
			}
		default:
		}
	}

	select {
	case serveErrVal := <-serveErr:
		srv.Close()
		_ = lis.Close()
		return "", nil, "", "", fmt.Errorf("embedded server failed to start: %w", serveErrVal)
	default:
	}

	return lis.Addr().String(), stop, dataDir, apiKey, nil
}

func ensureBenchmarkAdminKey(manager *security.Manager) (string, error) {
	const (
		userName = "benchmark-admin"
		keyName  = "benchmark-primary"
	)
	_, generated, err := manager.CreateUserWithAPIKey(userName, []security.Role{security.RoleAdmin}, keyName)
	if err == nil {
		return generated.Plaintext, nil
	}
	rotated, rotateErr := manager.CreateAPIKey(userName, keyName)
	if rotateErr == nil {
		return rotated.Plaintext, nil
	}
	return "", fmt.Errorf("provision benchmark auth user: create-user=%v create-key=%v", err, rotateErr)
}

func login(ctx context.Context, client pb.DatabasaClient, apiKey string, timeout time.Duration) error {
	callCtx, cancel := rpcContext(ctx, timeout)
	defer cancel()

	if _, err := client.Login(callCtx, &wrapperspb.StringValue{Value: strings.TrimSpace(apiKey)}); err != nil {
		return fmt.Errorf("login: %w", err)
	}
	return nil
}

func deleteCollection(ctx context.Context, client pb.DatabasaClient, collection string, timeout time.Duration) error {
	callCtx, cancel := rpcContext(ctx, timeout)
	defer cancel()

	_, err := client.DeleteCollection(callCtx, &pb.DeleteCollectionRequest{Name: collection})
	if err != nil && status.Code(err) != codes.NotFound {
		return fmt.Errorf("delete collection %q: %w", collection, err)
	}
	return nil
}

func createCollection(ctx context.Context, client pb.DatabasaClient, collection string, dimension uint32, metric pb.Metric, timeout time.Duration) error {
	callCtx, cancel := rpcContext(ctx, timeout)
	defer cancel()

	_, err := client.CreateCollection(callCtx, &pb.CreateCollectionRequest{
		Name:      collection,
		Dimension: dimension,
		Metric:    metric,
	})
	if err != nil {
		return fmt.Errorf("create collection %q: %w", collection, err)
	}
	return nil
}

func createIndex(ctx context.Context, client pb.DatabasaClient, collection string, m uint32, efConstruction uint32, timeout time.Duration) error {
	callCtx, cancel := rpcContext(ctx, timeout)
	defer cancel()

	_, err := client.CreateIndex(callCtx, &pb.CreateIndexRequest{
		Collection:     collection,
		M:              m,
		EfConstruction: efConstruction,
	})
	if err != nil {
		return fmt.Errorf("create index for collection %q: %w", collection, err)
	}
	return nil
}

func runInsertBenchmark(client pb.DatabasaClient, cfg config) (uint64, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.rpcMode)) {
	case "insert":
		return runSingleInsertBenchmark(client, cfg)
	default:
		return runBatchInsertBenchmark(client, cfg)
	}
}

func runBatchInsertBenchmark(client pb.DatabasaClient, cfg config) (uint64, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobs := make(chan batchJob, cfg.workers*2)
	errCh := make(chan error, 1)
	var inserted atomic.Uint64

	var wg sync.WaitGroup
	for i := 0; i < cfg.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				items := make([]*pb.BatchInsertItem, job.count)
				for offset := 0; offset < job.count; offset++ {
					vecID := job.start + offset
					items[offset] = &pb.BatchInsertItem{
						Key:       "bench:" + strconv.Itoa(vecID),
						Embedding: buildEmbedding(vecID, cfg.dimension),
					}
				}

				req := &pb.BatchInsertRequest{
					Collection: cfg.collection,
					Items:      items,
				}

				var lastErr error
				for attempt := 1; attempt <= maxBatchInsertRetries; attempt++ {
					callCtx, callCancel := rpcContext(ctx, cfg.rpcTimeout)
					_, err := client.BatchInsert(callCtx, req)
					callCancel()
					if err == nil {
						inserted.Add(uint64(job.count))
						lastErr = nil
						break
					}

					lastErr = err
					if !isRetryableBatchError(err) || attempt == maxBatchInsertRetries {
						break
					}

					backoff := time.Duration(attempt*50) * time.Millisecond
					time.Sleep(backoff)
				}

				if lastErr != nil {
					select {
					case errCh <- fmt.Errorf("batch insert start=%d count=%d: %w", job.start, job.count, lastErr):
					default:
					}
					cancel()
					return
				}
			}
		}()
	}

	progressDone := make(chan struct{})
	if cfg.progressEvery > 0 {
		go progressReporter(ctx, progressDone, cfg.progressEvery, cfg.vectors, &inserted)
	} else {
		close(progressDone)
	}

sendLoop:
	for start := 0; start < cfg.vectors; start += cfg.batchSize {
		count := cfg.batchSize
		remaining := cfg.vectors - start
		if remaining < count {
			count = remaining
		}

		select {
		case <-ctx.Done():
			break sendLoop
		case jobs <- batchJob{start: start, count: count}:
		}
	}

	close(jobs)
	wg.Wait()
	cancel()
	<-progressDone

	select {
	case err := <-errCh:
		return inserted.Load(), err
	default:
	}

	return inserted.Load(), nil
}

func runSingleInsertBenchmark(client pb.DatabasaClient, cfg config) (uint64, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobs := make(chan int, cfg.workers*2)
	errCh := make(chan error, 1)
	var inserted atomic.Uint64

	var wg sync.WaitGroup
	for i := 0; i < cfg.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for vecID := range jobs {
				req := &pb.InsertRequest{
					Collection: cfg.collection,
					Key:        "bench:" + strconv.Itoa(vecID),
					Embedding:  buildEmbedding(vecID, cfg.dimension),
				}

				var lastErr error
				for attempt := 1; attempt <= maxBatchInsertRetries; attempt++ {
					callCtx, callCancel := rpcContext(ctx, cfg.rpcTimeout)
					_, err := client.Insert(callCtx, req)
					callCancel()
					if err == nil {
						inserted.Add(1)
						lastErr = nil
						break
					}

					lastErr = err
					if !isRetryableBatchError(err) || attempt == maxBatchInsertRetries {
						break
					}

					backoff := time.Duration(attempt*50) * time.Millisecond
					time.Sleep(backoff)
				}

				if lastErr != nil {
					select {
					case errCh <- fmt.Errorf("insert id=%d: %w", vecID, lastErr):
					default:
					}
					cancel()
					return
				}
			}
		}()
	}

	progressDone := make(chan struct{})
	if cfg.progressEvery > 0 {
		go progressReporter(ctx, progressDone, cfg.progressEvery, cfg.vectors, &inserted)
	} else {
		close(progressDone)
	}

sendLoop:
	for vecID := 0; vecID < cfg.vectors; vecID++ {
		select {
		case <-ctx.Done():
			break sendLoop
		case jobs <- vecID:
		}
	}

	close(jobs)
	wg.Wait()
	cancel()
	<-progressDone

	select {
	case err := <-errCh:
		return inserted.Load(), err
	default:
	}

	return inserted.Load(), nil
}

func progressReporter(ctx context.Context, done chan<- struct{}, every time.Duration, total int, inserted *atomic.Uint64) {
	defer close(done)

	ticker := time.NewTicker(every)
	defer ticker.Stop()

	lastCount := uint64(0)
	lastAt := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			current := inserted.Load()
			delta := current - lastCount
			seconds := now.Sub(lastAt).Seconds()
			speed := 0.0
			if seconds > 0 {
				speed = float64(delta) / seconds
			}

			percent := (float64(current) / float64(total)) * 100
			log.Printf("progress: %d/%d (%.2f%%) window_throughput=%.2f vectors/s", current, total, percent, speed)

			lastCount = current
			lastAt = now
		}
	}
}

func rpcContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, timeout)
}

func buildEmbedding(vectorID, dim int) []float32 {
	out := make([]float32, dim)
	state := uint32(vectorID*747796405 + 2891336453)
	for i := 0; i < dim; i++ {
		state ^= state << 13
		state ^= state >> 17
		state ^= state << 5
		out[i] = float32(state%10000) / 10000
	}
	return out
}

func parseMetric(metric string) (pb.Metric, error) {
	switch strings.ToLower(strings.TrimSpace(metric)) {
	case "cosine":
		return pb.Metric_COSINE, nil
	case "l2":
		return pb.Metric_L2, nil
	case "dot", "dot_product":
		return pb.Metric_DOT_PRODUCT, nil
	default:
		return pb.Metric_COSINE, fmt.Errorf("invalid metric %q (use cosine, l2 or dot_product)", metric)
	}
}

func isRetryableBatchError(err error) bool {
	code := status.Code(err)
	if code == codes.DeadlineExceeded || code == codes.Unavailable || code == codes.ResourceExhausted {
		return true
	}

	if code != codes.Internal {
		return false
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "rename meta") ||
		strings.Contains(msg, "permission denied") ||
		strings.Contains(msg, "access is denied") ||
		strings.Contains(msg, "acesso negado")
}
