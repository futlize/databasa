package security

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/futlize/databasa/internal/adminapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	structpb "google.golang.org/protobuf/types/known/structpb"
)

type principalContextKey struct{}
type connectionContextKey struct{}

const loginRPCFullMethod = "/databasa.Databasa/Login"
const authServiceLoginRPCFullMethod = "/databasa.AuthService/Login"
const adminServiceMethodPrefix = "/" + adminapi.ServiceName + "/"

type compiledKey struct {
	hash         [32]byte
	salt         []byte
	roleMask     roleMask
	userDisabled bool
	keyDisabled  bool
	keyRevoked   bool
	principal    Principal
}

type compiledState struct {
	keys      map[string]compiledKey
	dummySalt []byte
	dummyHash [32]byte
}

type connectionSession struct {
	principal Principal
	roleMask  roleMask
	keyID     string
	createdAt time.Time
	lastSeen  time.Time
}

// AuthConfig defines request-path authentication behavior.
type AuthConfig struct {
	Enabled      bool
	RequireAuth  bool
	APIKeyHeader string
}

// Authenticator validates login credentials and enforces per-method authorization.
// Session identity is attached to the underlying gRPC transport connection.
type Authenticator struct {
	enabled     bool
	requireAuth bool
	apiKeyKey   string
	storePath   string

	state atomic.Pointer[compiledState]

	reloadInterval  time.Duration
	lastCheckUnixNs atomic.Int64
	lastModUnixNs   atomic.Int64
	reloadMu        sync.Mutex

	sessionsMu    sync.RWMutex
	sessions      map[string]connectionSession
	sessionsByKey map[string]map[string]struct{}
}

var methodPermissions = map[string]Permission{
	"/databasa.Databasa/ListCollections": PermissionRead,
	"/databasa.Databasa/Get":             PermissionRead,
	"/databasa.Databasa/Search":          PermissionRead,

	"/databasa.Databasa/Insert":      PermissionWrite,
	"/databasa.Databasa/Delete":      PermissionWrite,
	"/databasa.Databasa/BatchInsert": PermissionWrite,

	"/databasa.Databasa/CreateCollection": PermissionAdmin,
	"/databasa.Databasa/DeleteCollection": PermissionAdmin,
	"/databasa.Databasa/CreateIndex":      PermissionAdmin,
	"/databasa.Databasa/DropIndex":        PermissionAdmin,

	adminapi.FullMethodCreateUser:        PermissionAdmin,
	adminapi.FullMethodAlterUserPassword: PermissionAdmin,
	adminapi.FullMethodDropUser:          PermissionAdmin,
	adminapi.FullMethodListUsers:         PermissionAdmin,
	adminapi.FullMethodBootstrapStatus:   PermissionAdmin,
}

func NormalizeAuthConfig(cfg AuthConfig) AuthConfig {
	header := strings.ToLower(strings.TrimSpace(cfg.APIKeyHeader))
	if header == "" {
		header = "authorization"
	}
	cfg.APIKeyHeader = header
	return cfg
}

func NewAuthenticator(cfg AuthConfig, storePath string) (*Authenticator, error) {
	cfg = NormalizeAuthConfig(cfg)
	auth := &Authenticator{
		enabled:        cfg.Enabled,
		requireAuth:    cfg.RequireAuth,
		apiKeyKey:      cfg.APIKeyHeader,
		storePath:      strings.TrimSpace(storePath),
		reloadInterval: 0,
		sessions:       make(map[string]connectionSession),
		sessionsByKey:  make(map[string]map[string]struct{}),
	}
	baseState := newCompiledState()
	auth.state.Store(&baseState)

	if !auth.enabled {
		return auth, nil
	}

	store, err := loadIdentityStore(storePath)
	if err != nil {
		return nil, err
	}
	state, err := compileIdentityStore(store)
	if err != nil {
		return nil, err
	}
	auth.state.Store(&state)

	if info, err := os.Stat(auth.storePath); err == nil {
		auth.lastModUnixNs.Store(info.ModTime().UTC().UnixNano())
	}
	return auth, nil
}

func newCompiledState() compiledState {
	salt := []byte("databasa-auth-dummy-salt")
	return compiledState{
		keys:      make(map[string]compiledKey),
		dummySalt: salt,
		dummyHash: hashSecret("dummy-token", salt),
	}
}

func compileIdentityStore(store identityStore) (compiledState, error) {
	state := newCompiledState()
	for _, user := range store.Users {
		mask, err := roleMaskFromRoles(user.Roles)
		if err != nil {
			return compiledState{}, fmt.Errorf("user %q has invalid roles: %w", user.Name, err)
		}
		roles := append([]Role(nil), user.Roles...)
		for _, key := range user.Keys {
			if key == nil {
				return compiledState{}, fmt.Errorf("user %q has null key record", user.Name)
			}
			if _, exists := state.keys[key.ID]; exists {
				return compiledState{}, fmt.Errorf("duplicate api key id %q", key.ID)
			}

			hashBytes, err := base64.RawURLEncoding.DecodeString(key.Hash)
			if err != nil || len(hashBytes) != 32 {
				return compiledState{}, fmt.Errorf("key %q for user %q has invalid hash", key.ID, user.Name)
			}
			saltBytes, err := base64.RawURLEncoding.DecodeString(key.Salt)
			if err != nil || len(saltBytes) == 0 {
				return compiledState{}, fmt.Errorf("key %q for user %q has invalid salt", key.ID, user.Name)
			}

			var hash [32]byte
			copy(hash[:], hashBytes)
			state.keys[key.ID] = compiledKey{
				hash:         hash,
				salt:         append([]byte(nil), saltBytes...),
				roleMask:     mask,
				userDisabled: user.Disabled,
				keyDisabled:  key.Disabled,
				keyRevoked:   key.RevokedAt != nil,
				principal: Principal{
					UserID:   user.ID,
					UserName: user.Name,
					KeyID:    key.ID,
					Roles:    roles,
				},
			}
		}
	}
	return state, nil
}

func (s compiledState) burnCredential(token string) {
	sum := hashSecret(token, s.dummySalt)
	_ = subtle.ConstantTimeCompare(sum[:], s.dummyHash[:])
}

// UnaryServerInterceptor enforces connection-bound authentication and per-method authorization.
func (a *Authenticator) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		authorizedCtx, err := a.authorizeUnary(ctx, info.FullMethod, req)
		if err != nil {
			return nil, err
		}
		return handler(authorizedCtx, req)
	}
}

// StreamServerInterceptor enforces connection-bound authentication and per-method authorization.
func (a *Authenticator) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		authorizedCtx, err := a.authorizeStream(stream.Context(), info.FullMethod)
		if err != nil {
			return err
		}
		if authorizedCtx == stream.Context() {
			return handler(srv, stream)
		}
		return handler(srv, &contextOverrideStream{
			ServerStream: stream,
			ctx:          authorizedCtx,
		})
	}
}

func (a *Authenticator) authorizeUnary(ctx context.Context, fullMethod string, req any) (context.Context, error) {
	if isAdminMethod(fullMethod) && !isLocalPeer(ctx) {
		return nil, status.Error(codes.PermissionDenied, "admin RPCs are local-only")
	}
	if isLoginMethod(fullMethod) {
		return a.authorizeLogin(ctx, req)
	}
	if fullMethod == adminapi.FullMethodBootstrapStatus {
		authorizedCtx, err := a.authorizeEstablishedSession(ctx, fullMethod)
		if err == nil {
			return authorizedCtx, nil
		}
		if status.Code(err) == codes.Unauthenticated {
			return ctx, nil
		}
		return nil, err
	}
	if fullMethod == adminapi.FullMethodCreateUser {
		authorizedCtx, err := a.authorizeEstablishedSession(ctx, fullMethod)
		if err == nil {
			return authorizedCtx, nil
		}
		if status.Code(err) != codes.Unauthenticated {
			return nil, err
		}
		if !a.allowBootstrapCreateUser(req) {
			return nil, err
		}
		return ctx, nil
	}
	return a.authorizeEstablishedSession(ctx, fullMethod)
}

func (a *Authenticator) authorizeStream(ctx context.Context, fullMethod string) (context.Context, error) {
	if isAdminMethod(fullMethod) && !isLocalPeer(ctx) {
		return nil, status.Error(codes.PermissionDenied, "admin RPCs are local-only")
	}
	if isLoginMethod(fullMethod) {
		return nil, status.Error(codes.Unimplemented, "Login is unary-only")
	}
	return a.authorizeEstablishedSession(ctx, fullMethod)
}

func (a *Authenticator) authorizeLogin(ctx context.Context, req any) (context.Context, error) {
	if !a.enabled {
		return nil, status.Error(codes.FailedPrecondition, "authentication is disabled")
	}
	a.maybeReload()

	connID, err := connectionIDFromContext(ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "connection authentication context unavailable")
	}

	token, err := extractLoginCredentialToken(ctx, req, a.apiKeyKey, a.currentState())
	if err != nil {
		if errors.Is(err, errNoCredentials) {
			return nil, status.Error(codes.Unauthenticated, "api key is required for Login")
		}
		return nil, status.Error(codes.Unauthenticated, "invalid api key")
	}

	authn, err := authenticateToken(a.currentState(), token)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid api key")
	}

	a.setSession(connID, *authn)
	return context.WithValue(ctx, principalContextKey{}, authn.principal), nil
}

func (a *Authenticator) authorizeEstablishedSession(ctx context.Context, fullMethod string) (context.Context, error) {
	required := requiredPermission(fullMethod)

	if !a.enabled {
		if a.allowAnonymous(required) {
			return ctx, nil
		}
		return nil, status.Error(codes.Unauthenticated, "authentication required")
	}

	a.maybeReload()

	connID, err := connectionIDFromContext(ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "connection authentication context unavailable")
	}
	session, ok := a.sessionForConnection(connID)
	if !ok {
		if a.allowAnonymous(required) {
			return ctx, nil
		}
		return nil, status.Error(codes.Unauthenticated, "connection is not authenticated; call Login first")
	}
	if !session.roleMask.allows(required) {
		return nil, status.Errorf(codes.PermissionDenied, "insufficient permissions: requires %s", required)
	}
	return context.WithValue(ctx, principalContextKey{}, session.principal), nil
}

func (a *Authenticator) allowAnonymous(required Permission) bool {
	return !a.requireAuth && required == PermissionRead
}

func (a *Authenticator) allowBootstrapCreateUser(req any) bool {
	if !createUserBootstrapRequested(req) {
		return false
	}
	state := a.currentState()
	return state != nil && len(state.keys) == 0
}

type authenticatedPrincipal struct {
	principal Principal
	roleMask  roleMask
	keyID     string
}

func (a *Authenticator) authenticate(ctx context.Context) (*authenticatedPrincipal, error) {
	token, err := extractCredentialToken(ctx, a.apiKeyKey)
	if err != nil {
		return nil, err
	}
	return authenticateToken(a.currentState(), token)
}

func authenticateToken(state *compiledState, token string) (*authenticatedPrincipal, error) {
	if state == nil {
		return nil, errors.New("auth state is unavailable")
	}

	keyID, secret, err := parseAPIKey(token)
	if err != nil {
		state.burnCredential(token)
		return nil, errors.New("malformed api key")
	}

	record, exists := state.keys[keyID]
	if !exists {
		state.burnCredential(secret)
		return nil, errors.New("key not found")
	}

	sum := hashSecret(secret, record.salt)
	if subtle.ConstantTimeCompare(sum[:], record.hash[:]) != 1 {
		return nil, errors.New("hash mismatch")
	}
	if record.userDisabled || record.keyDisabled || record.keyRevoked {
		return nil, errors.New("credential disabled")
	}
	return &authenticatedPrincipal{
		principal: record.principal,
		roleMask:  record.roleMask,
		keyID:     record.principal.KeyID,
	}, nil
}

func (a *Authenticator) currentState() *compiledState {
	state := a.state.Load()
	if state != nil {
		return state
	}
	empty := newCompiledState()
	a.state.Store(&empty)
	return &empty
}

func (a *Authenticator) maybeReload() {
	if !a.enabled || a.storePath == "" {
		return
	}
	now := time.Now().UTC().UnixNano()
	last := a.lastCheckUnixNs.Load()
	if last != 0 && now-last < a.reloadInterval.Nanoseconds() {
		return
	}
	if !a.lastCheckUnixNs.CompareAndSwap(last, now) {
		return
	}

	a.reloadMu.Lock()
	defer a.reloadMu.Unlock()

	info, err := os.Stat(a.storePath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return
		}
		if a.lastModUnixNs.Load() == 0 {
			return
		}
		// File was removed. Switch to empty credentials and clear derived sessions.
		empty := newCompiledState()
		a.state.Store(&empty)
		a.lastModUnixNs.Store(0)
		a.pruneSessions(&empty)
		return
	}

	modUnix := info.ModTime().UTC().UnixNano()
	if modUnix <= a.lastModUnixNs.Load() {
		return
	}

	store, err := loadIdentityStore(a.storePath)
	if err != nil {
		return
	}
	compiled, err := compileIdentityStore(store)
	if err != nil {
		return
	}
	a.state.Store(&compiled)
	a.lastModUnixNs.Store(modUnix)
	a.pruneSessions(&compiled)
}

func extractCredentialToken(ctx context.Context, headerKey string) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", errNoCredentials
	}

	headerKey = strings.ToLower(strings.TrimSpace(headerKey))
	keys := []string{"authorization"}
	if headerKey != "" && headerKey != "authorization" {
		keys = append(keys, headerKey)
	}

	for _, key := range keys {
		values := md.Get(key)
		for _, raw := range values {
			value := strings.TrimSpace(raw)
			if value == "" {
				continue
			}
			if token, ok := extractTokenValue(value); ok {
				return token, nil
			}
		}
	}
	return "", errNoCredentials
}

func extractLoginCredentialToken(ctx context.Context, req any, headerKey string, state *compiledState) (string, error) {
	if token, ok := tokenFromLoginRequest(req, state); ok {
		return token, nil
	}
	if loginCredentialProvided(req) {
		return "", errors.New("invalid login credential")
	}
	return extractCredentialToken(ctx, headerKey)
}

func tokenFromLoginRequest(req any, state *compiledState) (string, bool) {
	if req == nil {
		return "", false
	}
	if withValue, ok := req.(interface{ GetValue() string }); ok {
		if token, ok := extractTokenValue(withValue.GetValue()); ok {
			return token, true
		}
	}
	if withAPIKey, ok := req.(interface{ GetApiKey() string }); ok {
		if token, ok := extractTokenValue(withAPIKey.GetApiKey()); ok {
			return token, true
		}
	}
	if reqStruct, ok := req.(*structpb.Struct); ok {
		if token, ok := tokenFromStructField(reqStruct, "api_key"); ok {
			return token, true
		}
		if token, ok := tokenFromStructField(reqStruct, "value"); ok {
			return token, true
		}
		if token, ok := tokenFromUsernameSecret(reqStruct, state); ok {
			return token, true
		}
	}
	return "", false
}

func loginCredentialProvided(req any) bool {
	if req == nil {
		return false
	}
	if withValue, ok := req.(interface{ GetValue() string }); ok {
		if strings.TrimSpace(withValue.GetValue()) != "" {
			return true
		}
	}
	if withAPIKey, ok := req.(interface{ GetApiKey() string }); ok {
		if strings.TrimSpace(withAPIKey.GetApiKey()) != "" {
			return true
		}
	}
	if reqStruct, ok := req.(*structpb.Struct); ok {
		for _, key := range []string{"api_key", "value", "username", "secret"} {
			if strings.TrimSpace(structStringField(reqStruct, key)) != "" {
				return true
			}
		}
	}
	return false
}

func tokenFromStructField(req *structpb.Struct, field string) (string, bool) {
	if req == nil {
		return "", false
	}
	value := req.GetFields()[field]
	if value == nil {
		return "", false
	}
	return extractTokenValue(value.GetStringValue())
}

func tokenFromUsernameSecret(req *structpb.Struct, state *compiledState) (string, bool) {
	if req == nil || state == nil {
		return "", false
	}
	user := strings.TrimSpace(structStringField(req, "username"))
	secret := strings.TrimSpace(structStringField(req, "secret"))
	if user == "" || secret == "" {
		return "", false
	}

	for _, candidate := range state.keys {
		if candidate.userDisabled || candidate.keyDisabled || candidate.keyRevoked {
			continue
		}
		if candidate.principal.UserName != user {
			continue
		}
		sum := hashSecret(secret, candidate.salt)
		if subtle.ConstantTimeCompare(sum[:], candidate.hash[:]) == 1 {
			return formatAPIKey(candidate.principal.KeyID, secret), true
		}
	}
	state.burnCredential(secret)
	return "", false
}

func structStringField(req *structpb.Struct, field string) string {
	if req == nil {
		return ""
	}
	value := req.GetFields()[field]
	if value == nil {
		return ""
	}
	return value.GetStringValue()
}

func extractTokenValue(value string) (string, bool) {
	v := strings.TrimSpace(value)
	if v == "" {
		return "", false
	}
	if len(v) >= 7 && strings.EqualFold(v[:7], "bearer ") {
		token := strings.TrimSpace(v[7:])
		if token == "" {
			return "", false
		}
		return token, true
	}
	return v, true
}

func requiredPermission(fullMethod string) Permission {
	if permission, ok := methodPermissions[fullMethod]; ok {
		return permission
	}
	// Unknown methods are treated as admin-only to avoid accidental exposure.
	return PermissionAdmin
}

func (a *Authenticator) setSession(connID string, authn authenticatedPrincipal) {
	a.sessionsMu.Lock()
	defer a.sessionsMu.Unlock()

	a.deleteSessionLocked(connID)

	now := time.Now().UTC()
	session := connectionSession{
		principal: authn.principal,
		roleMask:  authn.roleMask,
		keyID:     authn.keyID,
		createdAt: now,
		lastSeen:  now,
	}
	a.sessions[connID] = session
	byKey, ok := a.sessionsByKey[session.keyID]
	if !ok {
		byKey = make(map[string]struct{})
		a.sessionsByKey[session.keyID] = byKey
	}
	byKey[connID] = struct{}{}
}

func (a *Authenticator) sessionForConnection(connID string) (connectionSession, bool) {
	a.sessionsMu.Lock()
	defer a.sessionsMu.Unlock()
	session, ok := a.sessions[connID]
	if ok {
		session.lastSeen = time.Now().UTC()
		a.sessions[connID] = session
	}
	return session, ok
}

func (a *Authenticator) clearSession(connID string) {
	a.sessionsMu.Lock()
	defer a.sessionsMu.Unlock()
	a.deleteSessionLocked(connID)
}

func (a *Authenticator) deleteSessionLocked(connID string) {
	session, exists := a.sessions[connID]
	if !exists {
		return
	}
	delete(a.sessions, connID)
	if byKey, ok := a.sessionsByKey[session.keyID]; ok {
		delete(byKey, connID)
		if len(byKey) == 0 {
			delete(a.sessionsByKey, session.keyID)
		}
	}
}

func (a *Authenticator) pruneSessions(state *compiledState) {
	if state == nil {
		return
	}
	a.sessionsMu.Lock()
	defer a.sessionsMu.Unlock()

	for connID, session := range a.sessions {
		record, ok := state.keys[session.keyID]
		if !ok ||
			record.userDisabled ||
			record.keyDisabled ||
			record.keyRevoked ||
			record.principal.UserID != session.principal.UserID ||
			record.roleMask != session.roleMask {
			a.deleteSessionLocked(connID)
		}
	}
}

func connectionIDFromContext(ctx context.Context) (string, error) {
	connID, ok := ctx.Value(connectionContextKey{}).(string)
	if !ok || connID == "" {
		return "", errors.New("missing connection id")
	}
	return connID, nil
}

// TagConn attaches a unique server-side connection id used to bind sessions to transport connections.
func (a *Authenticator) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	connID, err := generateConnID()
	if err != nil {
		// Entropy failure is a hard security failure: keep this connection unauthenticated.
		return ctx
	}
	return context.WithValue(ctx, connectionContextKey{}, connID)
}

// HandleConn removes the connection session when the transport connection closes.
func (a *Authenticator) HandleConn(ctx context.Context, connStats stats.ConnStats) {
	if _, ok := connStats.(*stats.ConnEnd); !ok {
		return
	}
	connID, err := connectionIDFromContext(ctx)
	if err != nil {
		return
	}
	a.clearSession(connID)
}

// TagRPC satisfies grpc/stats.Handler and preserves the tagged connection context.
func (a *Authenticator) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	connID, err := connectionIDFromContext(ctx)
	if err != nil {
		return ctx
	}
	// Keep the connection id explicitly attached to the RPC context used by interceptors.
	return context.WithValue(ctx, connectionContextKey{}, connID)
}

// HandleRPC satisfies grpc/stats.Handler. Auth logic is handled by interceptors.
func (a *Authenticator) HandleRPC(context.Context, stats.RPCStats) {}

type contextOverrideStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *contextOverrideStream) Context() context.Context {
	return s.ctx
}

// PrincipalFromContext returns caller identity after successful authentication.
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(principalContextKey{}).(Principal)
	return principal, ok
}

func generateConnID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

func isLoginMethod(fullMethod string) bool {
	return fullMethod == loginRPCFullMethod ||
		fullMethod == authServiceLoginRPCFullMethod ||
		fullMethod == adminapi.FullMethodLogin
}

func isAdminMethod(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, adminServiceMethodPrefix)
}

func createUserBootstrapRequested(req any) bool {
	in, ok := req.(*structpb.Struct)
	if !ok || in == nil {
		return false
	}
	value := in.GetFields()["admin"]
	if value == nil {
		return false
	}
	switch kind := value.Kind.(type) {
	case *structpb.Value_BoolValue:
		return kind.BoolValue
	case *structpb.Value_StringValue:
		switch strings.ToLower(strings.TrimSpace(kind.StringValue)) {
		case "1", "true", "yes", "on":
			return true
		}
	case *structpb.Value_NumberValue:
		return kind.NumberValue != 0
	}
	return false
}

func isLocalPeer(ctx context.Context) bool {
	info, ok := peer.FromContext(ctx)
	if !ok || info.Addr == nil {
		return false
	}
	switch info.Addr.(type) {
	case *net.UnixAddr:
		return true
	}

	addr := strings.TrimSpace(info.Addr.String())
	if addr == "" {
		return false
	}
	host := addr
	if parsedHost, _, err := net.SplitHostPort(addr); err == nil {
		host = parsedHost
	}
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
