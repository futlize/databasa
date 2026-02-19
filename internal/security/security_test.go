package security

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/futlize/databasa/internal/server"
	pb "github.com/futlize/databasa/pkg/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	wrapperspb "google.golang.org/protobuf/types/known/wrapperspb"
)

const testRPCDeadline = 2 * time.Second

type authHarness struct {
	t          *testing.T
	listener   *bufconn.Listener
	grpcServer *grpc.Server
	dataServer *server.Server
	manager    *Manager
	auth       *Authenticator
	serveErr   chan error
}

func startAuthHarness(t *testing.T, cfg AuthConfig, prepareStore func(manager *Manager)) *authHarness {
	t.Helper()

	dataDir := filepath.Join(t.TempDir(), "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("create data dir: %v", err)
	}

	storePath := AuthStorePath(dataDir)
	manager := NewManager(storePath)
	if prepareStore != nil {
		prepareStore(manager)
	}

	dataServer, err := server.NewWithConfig(dataDir, 1, "int8")
	if err != nil {
		t.Fatalf("new databasa server: %v", err)
	}

	auth, err := NewAuthenticator(cfg, storePath)
	if err != nil {
		dataServer.Close()
		t.Fatalf("new authenticator: %v", err)
	}

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(auth.UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(auth.StreamServerInterceptor()),
		grpc.StatsHandler(auth),
	)
	pb.RegisterDatabasaServer(grpcServer, dataServer)

	listener := bufconn.Listen(1 << 20)
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- grpcServer.Serve(listener)
	}()

	h := &authHarness{
		t:          t,
		listener:   listener,
		grpcServer: grpcServer,
		dataServer: dataServer,
		manager:    manager,
		auth:       auth,
		serveErr:   serveErr,
	}
	t.Cleanup(h.close)
	return h
}

func (h *authHarness) close() {
	h.grpcServer.Stop()
	h.dataServer.Close()
	_ = h.listener.Close()

	select {
	case err := <-h.serveErr:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			h.t.Fatalf("grpc serve failed: %v", err)
		}
	default:
	}
}

func (h *authHarness) newClient(t *testing.T) (*grpc.ClientConn, pb.DatabasaClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testRPCDeadline)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		"bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return h.listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	return conn, pb.NewDatabasaClient(conn)
}

func rpcContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), testRPCDeadline)
}

func loginWithAPIKey(t *testing.T, client pb.DatabasaClient, apiKey string) {
	t.Helper()
	ctx, cancel := rpcContext()
	defer cancel()

	_, err := client.Login(ctx, &wrapperspb.StringValue{Value: apiKey})
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}
}

func TestManagerCreateUserStoresOnlyHashedKeys(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "security", "auth.json")
	manager := NewManager(storePath)

	user, generated, err := manager.CreateUserWithAPIKey("alice", []Role{RoleRead, RoleWrite}, "primary")
	if err != nil {
		t.Fatalf("create user with key: %v", err)
	}
	if user.Name != "alice" {
		t.Fatalf("unexpected user name: %q", user.Name)
	}
	if generated.Plaintext == "" || generated.KeyID == "" {
		t.Fatalf("expected generated api key")
	}

	storeRaw, err := os.ReadFile(storePath)
	if err != nil {
		t.Fatalf("read auth store: %v", err)
	}
	if strings.Contains(string(storeRaw), generated.Plaintext) {
		t.Fatalf("auth store must not contain plaintext api keys")
	}

	store, err := loadIdentityStore(storePath)
	if err != nil {
		t.Fatalf("load auth store: %v", err)
	}
	if len(store.Users) != 1 || len(store.Users[0].Keys) != 1 {
		t.Fatalf("unexpected auth store shape: users=%d keys=%d", len(store.Users), len(store.Users[0].Keys))
	}
	if store.Users[0].Keys[0].Hash == "" || store.Users[0].Keys[0].Salt == "" {
		t.Fatalf("expected hashed key material in auth store")
	}
}

func TestNonLoginRPCBeforeAuthRejected(t *testing.T) {
	var readKey GeneratedKey
	h := startAuthHarness(t, AuthConfig{
		Enabled:      true,
		RequireAuth:  true,
		APIKeyHeader: "authorization",
	}, func(manager *Manager) {
		_, key, err := manager.CreateUserWithAPIKey("reader", []Role{RoleRead}, "primary")
		if err != nil {
			t.Fatalf("create key: %v", err)
		}
		readKey = key
	})
	_ = readKey

	conn, client := h.newClient(t)
	defer conn.Close()

	ctx, cancel := rpcContext()
	defer cancel()
	_, err := client.ListCollections(ctx, &pb.ListCollectionsRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated before Login, got: %v", err)
	}

	ctxWithKey, cancelWithKey := rpcContext()
	defer cancelWithKey()
	ctxWithKey = metadata.AppendToOutgoingContext(ctxWithKey, "authorization", "Bearer "+readKey.Plaintext)
	_, err = client.ListCollections(ctxWithKey, &pb.ListCollectionsRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("non-Login RPC must not accept per-RPC api key metadata; got: %v", err)
	}
}

func TestLoginBindsSessionToConnectionAndNoFurtherAPIKeyRequired(t *testing.T) {
	var readKey GeneratedKey
	h := startAuthHarness(t, AuthConfig{
		Enabled:      true,
		RequireAuth:  true,
		APIKeyHeader: "authorization",
	}, func(manager *Manager) {
		_, key, err := manager.CreateUserWithAPIKey("reader", []Role{RoleRead}, "primary")
		if err != nil {
			t.Fatalf("create key: %v", err)
		}
		readKey = key
	})

	conn1, client1 := h.newClient(t)
	defer conn1.Close()

	loginWithAPIKey(t, client1, readKey.Plaintext)

	ctx, cancel := rpcContext()
	defer cancel()
	if _, err := client1.ListCollections(ctx, &pb.ListCollectionsRequest{}); err != nil {
		t.Fatalf("post-login call without api key should succeed: %v", err)
	}

	conn2, client2 := h.newClient(t)
	defer conn2.Close()

	ctx2, cancel2 := rpcContext()
	defer cancel2()
	_, err := client2.ListCollections(ctx2, &pb.ListCollectionsRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("second connection without Login should be unauthenticated, got: %v", err)
	}
}

func TestPermissionChecksAreEnforcedAfterLogin(t *testing.T) {
	var readKey GeneratedKey
	h := startAuthHarness(t, AuthConfig{
		Enabled:      true,
		RequireAuth:  true,
		APIKeyHeader: "authorization",
	}, func(manager *Manager) {
		_, key, err := manager.CreateUserWithAPIKey("reader", []Role{RoleRead}, "primary")
		if err != nil {
			t.Fatalf("create key: %v", err)
		}
		readKey = key
	})

	conn, client := h.newClient(t)
	defer conn.Close()

	loginWithAPIKey(t, client, readKey.Plaintext)

	ctx, cancel := rpcContext()
	defer cancel()
	_, err := client.Insert(ctx, &pb.InsertRequest{
		Collection: "docs",
		Key:        "k1",
		Embedding:  []float32{0.1},
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied for write with read-only role, got: %v", err)
	}
}

func TestRevokedKeyInvalidatesActiveSessions(t *testing.T) {
	var (
		readKey GeneratedKey
		err     error
	)
	h := startAuthHarness(t, AuthConfig{
		Enabled:      true,
		RequireAuth:  true,
		APIKeyHeader: "authorization",
	}, func(manager *Manager) {
		_, key, createErr := manager.CreateUserWithAPIKey("reader", []Role{RoleRead}, "primary")
		if createErr != nil {
			t.Fatalf("create key: %v", createErr)
		}
		readKey = key
	})

	conn, client := h.newClient(t)
	defer conn.Close()

	loginWithAPIKey(t, client, readKey.Plaintext)

	ctx, cancel := rpcContext()
	defer cancel()
	if _, err = client.ListCollections(ctx, &pb.ListCollectionsRequest{}); err != nil {
		t.Fatalf("pre-revocation request failed: %v", err)
	}

	if err := h.manager.RevokeAPIKey("reader", readKey.KeyID); err != nil {
		t.Fatalf("revoke key: %v", err)
	}

	// Force immediate state refresh in test process and verify session invalidation deterministically.
	h.auth.lastModUnixNs.Store(0)
	h.auth.maybeReload()

	attemptCtx, attemptCancel := rpcContext()
	defer attemptCancel()
	_, err = client.ListCollections(attemptCtx, &pb.ListCollectionsRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected active session to be invalidated immediately after revoke, got: %v", err)
	}
}

func TestRevokedKeyInvalidatesAllActiveSessionsForKey(t *testing.T) {
	var readKey GeneratedKey
	h := startAuthHarness(t, AuthConfig{
		Enabled:      true,
		RequireAuth:  true,
		APIKeyHeader: "authorization",
	}, func(manager *Manager) {
		_, key, err := manager.CreateUserWithAPIKey("reader", []Role{RoleRead}, "primary")
		if err != nil {
			t.Fatalf("create key: %v", err)
		}
		readKey = key
	})

	conn1, client1 := h.newClient(t)
	defer conn1.Close()
	loginWithAPIKey(t, client1, readKey.Plaintext)

	conn2, client2 := h.newClient(t)
	defer conn2.Close()
	loginWithAPIKey(t, client2, readKey.Plaintext)

	if err := h.manager.RevokeAPIKey("reader", readKey.KeyID); err != nil {
		t.Fatalf("revoke key: %v", err)
	}

	h.auth.lastModUnixNs.Store(0)
	h.auth.maybeReload()

	ctx1, cancel1 := rpcContext()
	defer cancel1()
	_, err1 := client1.ListCollections(ctx1, &pb.ListCollectionsRequest{})
	if status.Code(err1) != codes.Unauthenticated {
		t.Fatalf("expected connection 1 session invalidated after revoke, got: %v", err1)
	}

	ctx2, cancel2 := rpcContext()
	defer cancel2()
	_, err2 := client2.ListCollections(ctx2, &pb.ListCollectionsRequest{})
	if status.Code(err2) != codes.Unauthenticated {
		t.Fatalf("expected connection 2 session invalidated after revoke, got: %v", err2)
	}
}

func TestAnonymousModeNeverAllowsWriteOrAdmin(t *testing.T) {
	var adminKey GeneratedKey
	h := startAuthHarness(t, AuthConfig{
		Enabled:      true,
		RequireAuth:  false,
		APIKeyHeader: "authorization",
	}, func(manager *Manager) {
		_, key, err := manager.CreateUserWithAPIKey("admin1", []Role{RoleAdmin}, "primary")
		if err != nil {
			t.Fatalf("create admin key: %v", err)
		}
		adminKey = key
	})

	conn, client := h.newClient(t)
	defer conn.Close()

	readCtx, readCancel := rpcContext()
	defer readCancel()
	if _, err := client.ListCollections(readCtx, &pb.ListCollectionsRequest{}); err != nil {
		t.Fatalf("anonymous read should be allowed when require_auth=false: %v", err)
	}

	adminCtx, adminCancel := rpcContext()
	defer adminCancel()
	_, err := client.CreateCollection(adminCtx, &pb.CreateCollectionRequest{
		Name:      "docs",
		Dimension: 3,
		Metric:    pb.Metric_COSINE,
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("anonymous admin call must be rejected, got: %v", err)
	}

	loginWithAPIKey(t, client, adminKey.Plaintext)

	adminCtx2, adminCancel2 := rpcContext()
	defer adminCancel2()
	if _, err := client.CreateCollection(adminCtx2, &pb.CreateCollectionRequest{
		Name:      "docs",
		Dimension: 3,
		Metric:    pb.Metric_COSINE,
	}); err != nil {
		t.Fatalf("authenticated admin call should succeed: %v", err)
	}
}
