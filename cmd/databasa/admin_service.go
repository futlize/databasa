package main

import (
	"context"
	"net"
	"strings"

	"github.com/futlize/databasa/internal/adminapi"
	"github.com/futlize/databasa/internal/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	structpb "google.golang.org/protobuf/types/known/structpb"
)

type adminService struct {
	manager         *security.Manager
	requireDeadline bool
}

type adminServiceServer interface {
	Login(context.Context, *structpb.Struct) (*emptypb.Empty, error)
	CreateUser(context.Context, *structpb.Struct) (*structpb.Struct, error)
	AlterUserPassword(context.Context, *structpb.Struct) (*structpb.Struct, error)
	DropUser(context.Context, *structpb.Struct) (*emptypb.Empty, error)
	ListUsers(context.Context, *emptypb.Empty) (*structpb.Struct, error)
	BootstrapStatus(context.Context, *emptypb.Empty) (*structpb.Struct, error)
}

func registerAdminService(reg grpc.ServiceRegistrar, manager *security.Manager, requireDeadline bool) {
	reg.RegisterService(&adminServiceDesc, &adminService{
		manager:         manager,
		requireDeadline: requireDeadline,
	})
}

func (a *adminService) Login(ctx context.Context, req *structpb.Struct) (*emptypb.Empty, error) {
	if err := a.validateContext(ctx); err != nil {
		return nil, err
	}
	_ = req
	return &emptypb.Empty{}, nil
}

func (a *adminService) BootstrapStatus(ctx context.Context, req *emptypb.Empty) (*structpb.Struct, error) {
	if err := a.validateContext(ctx); err != nil {
		return nil, err
	}
	_ = req
	records, err := a.manager.ListUsers()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list users: %v", err)
	}
	return structpb.NewStruct(map[string]any{
		"has_users": len(records) > 0,
	})
}

func (a *adminService) CreateUser(ctx context.Context, req *structpb.Struct) (*structpb.Struct, error) {
	if err := a.validateContext(ctx); err != nil {
		return nil, err
	}
	name := requiredStructString(req, "name")
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	secret := requiredStructString(req, "secret")
	if secret == "" {
		return nil, status.Error(codes.InvalidArgument, "secret is required")
	}
	admin := structBool(req, "admin")

	if err := a.ensureCreateUserAuthorization(ctx, admin); err != nil {
		return nil, err
	}

	roles := []security.Role{security.RoleRead}
	if admin {
		roles = []security.Role{security.RoleAdmin}
	}
	user, generated, err := a.manager.CreateUserWithPassword(name, roles, "password", secret)
	if err != nil {
		return nil, mapManagerError(err)
	}
	return structpb.NewStruct(map[string]any{
		"name":    user.Name,
		"roles":   rolesToAny(user.Roles),
		"key_id":  generated.KeyID,
		"api_key": generated.Plaintext,
	})
}

func (a *adminService) AlterUserPassword(ctx context.Context, req *structpb.Struct) (*structpb.Struct, error) {
	if err := a.validateContext(ctx); err != nil {
		return nil, err
	}
	if err := requireAdminPrincipal(ctx); err != nil {
		return nil, err
	}
	name := requiredStructString(req, "name")
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	secret := requiredStructString(req, "secret")
	if secret == "" {
		return nil, status.Error(codes.InvalidArgument, "secret is required")
	}

	generated, err := a.manager.AlterUserPassword(name, "password", secret)
	if err != nil {
		return nil, mapManagerError(err)
	}
	return structpb.NewStruct(map[string]any{
		"name":    name,
		"key_id":  generated.KeyID,
		"api_key": generated.Plaintext,
	})
}

func (a *adminService) DropUser(ctx context.Context, req *structpb.Struct) (*emptypb.Empty, error) {
	if err := a.validateContext(ctx); err != nil {
		return nil, err
	}
	principal, ok := security.PrincipalFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "admin authentication required")
	}
	if !principalHasRole(principal, security.RoleAdmin) {
		return nil, status.Error(codes.PermissionDenied, "admin authentication required")
	}

	name := requiredStructString(req, "name")
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if strings.EqualFold(strings.TrimSpace(name), strings.TrimSpace(principal.UserName)) {
		return nil, status.Error(codes.InvalidArgument, "cannot drop currently authenticated user")
	}
	if err := a.manager.DeleteUser(name); err != nil {
		return nil, mapManagerError(err)
	}
	return &emptypb.Empty{}, nil
}

func (a *adminService) ListUsers(ctx context.Context, req *emptypb.Empty) (*structpb.Struct, error) {
	if err := a.validateContext(ctx); err != nil {
		return nil, err
	}
	_ = req
	if err := requireAdminPrincipal(ctx); err != nil {
		return nil, err
	}

	records, err := a.manager.ListUsers()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list users: %v", err)
	}
	users := make([]any, 0, len(records))
	for _, record := range records {
		users = append(users, map[string]any{
			"name":        record.Name,
			"roles":       rolesToAny(record.Roles),
			"disabled":    record.Disabled,
			"active_keys": float64(record.ActiveKeys),
			"total_keys":  float64(record.TotalKeys),
		})
	}
	return structpb.NewStruct(map[string]any{"users": users})
}

func (a *adminService) validateContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if a.requireDeadline {
		if _, ok := ctx.Deadline(); !ok {
			return status.Error(codes.InvalidArgument, "rpc deadline is required")
		}
	}
	return nil
}

func (a *adminService) ensureCreateUserAuthorization(ctx context.Context, admin bool) error {
	principal, ok := security.PrincipalFromContext(ctx)
	if ok {
		if principalHasRole(principal, security.RoleAdmin) {
			return nil
		}
		return status.Error(codes.PermissionDenied, "admin authentication required")
	}

	records, err := a.manager.ListUsers()
	if err != nil {
		return status.Errorf(codes.Internal, "list users: %v", err)
	}
	if len(records) != 0 {
		return status.Error(codes.Unauthenticated, "admin authentication required")
	}
	if !isLocalPeer(ctx) {
		return status.Error(codes.Unauthenticated, "admin authentication required")
	}
	if !admin {
		return status.Error(codes.InvalidArgument, "first user bootstrap must include admin role")
	}
	return nil
}

func mapManagerError(err error) error {
	if err == nil {
		return nil
	}
	msg := strings.TrimSpace(err.Error())
	switch {
	case strings.Contains(msg, "already exists"):
		return status.Error(codes.AlreadyExists, msg)
	case strings.Contains(msg, "not found"):
		return status.Error(codes.NotFound, msg)
	default:
		return status.Error(codes.InvalidArgument, msg)
	}
}

func requireAdminPrincipal(ctx context.Context) error {
	principal, ok := security.PrincipalFromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "admin authentication required")
	}
	if !principalHasRole(principal, security.RoleAdmin) {
		return status.Error(codes.PermissionDenied, "admin authentication required")
	}
	return nil
}

func principalHasRole(principal security.Principal, role security.Role) bool {
	for _, current := range principal.Roles {
		if current == role {
			return true
		}
	}
	return false
}

func requiredStructString(req *structpb.Struct, key string) string {
	if req == nil {
		return ""
	}
	value := req.Fields[key]
	if value == nil {
		return ""
	}
	return strings.TrimSpace(value.GetStringValue())
}

func structBool(req *structpb.Struct, key string) bool {
	if req == nil {
		return false
	}
	value := req.Fields[key]
	if value == nil {
		return false
	}
	switch kind := value.Kind.(type) {
	case *structpb.Value_BoolValue:
		return kind.BoolValue
	case *structpb.Value_NumberValue:
		return kind.NumberValue != 0
	case *structpb.Value_StringValue:
		switch strings.ToLower(strings.TrimSpace(kind.StringValue)) {
		case "1", "true", "yes", "on":
			return true
		default:
			return false
		}
	default:
		return false
	}
}

func rolesToAny(roles []security.Role) []any {
	out := make([]any, 0, len(roles))
	for _, role := range roles {
		out = append(out, string(role))
	}
	return out
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

	address := strings.TrimSpace(info.Addr.String())
	if address == "" {
		return false
	}
	host := address
	if parsedHost, _, err := net.SplitHostPort(address); err == nil {
		host = parsedHost
	}
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

var adminServiceDesc = grpc.ServiceDesc{
	ServiceName: adminapi.ServiceName,
	HandlerType: (*adminServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: adminapi.MethodLogin, Handler: adminServiceLoginHandler},
		{MethodName: adminapi.MethodCreateUser, Handler: adminServiceCreateUserHandler},
		{MethodName: adminapi.MethodAlterUserPassword, Handler: adminServiceAlterUserPasswordHandler},
		{MethodName: adminapi.MethodDropUser, Handler: adminServiceDropUserHandler},
		{MethodName: adminapi.MethodListUsers, Handler: adminServiceListUsersHandler},
		{MethodName: adminapi.MethodBootstrapStatus, Handler: adminServiceBootstrapStatusHandler},
	},
}

func adminServiceLoginHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(structpb.Struct)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(adminServiceServer).Login(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: adminapi.FullMethodLogin}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(adminServiceServer).Login(ctx, req.(*structpb.Struct))
	}
	return interceptor(ctx, in, info, handler)
}

func adminServiceCreateUserHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(structpb.Struct)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(adminServiceServer).CreateUser(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: adminapi.FullMethodCreateUser}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(adminServiceServer).CreateUser(ctx, req.(*structpb.Struct))
	}
	return interceptor(ctx, in, info, handler)
}

func adminServiceAlterUserPasswordHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(structpb.Struct)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(adminServiceServer).AlterUserPassword(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: adminapi.FullMethodAlterUserPassword}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(adminServiceServer).AlterUserPassword(ctx, req.(*structpb.Struct))
	}
	return interceptor(ctx, in, info, handler)
}

func adminServiceDropUserHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(structpb.Struct)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(adminServiceServer).DropUser(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: adminapi.FullMethodDropUser}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(adminServiceServer).DropUser(ctx, req.(*structpb.Struct))
	}
	return interceptor(ctx, in, info, handler)
}

func adminServiceListUsersHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(adminServiceServer).ListUsers(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: adminapi.FullMethodListUsers}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(adminServiceServer).ListUsers(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func adminServiceBootstrapStatusHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(adminServiceServer).BootstrapStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: adminapi.FullMethodBootstrapStatus}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(adminServiceServer).BootstrapStatus(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}
