package security

import (
	"path/filepath"
	"strings"
	"testing"

	structpb "google.golang.org/protobuf/types/known/structpb"
)

func TestTokenFromLoginRequestWithUsernameSecret(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "auth.json")
	manager := NewManager(storePath)
	if _, _, err := manager.CreateUserWithPassword("adminuser", []Role{RoleAdmin}, "password", "secret123"); err != nil {
		t.Fatalf("create user: %v", err)
	}

	store, err := loadIdentityStore(storePath)
	if err != nil {
		t.Fatalf("load store: %v", err)
	}
	state, err := compileIdentityStore(store)
	if err != nil {
		t.Fatalf("compile store: %v", err)
	}

	req, err := structpb.NewStruct(map[string]any{
		"username": "adminuser",
		"secret":   "secret123",
	})
	if err != nil {
		t.Fatalf("new login payload: %v", err)
	}
	token, ok := tokenFromLoginRequest(req, &state)
	if !ok {
		t.Fatalf("expected token to be resolved from username+secret")
	}
	if !strings.HasPrefix(token, "dbs1.") {
		t.Fatalf("expected api key format, got %q", token)
	}

	authn, err := authenticateToken(&state, token)
	if err != nil {
		t.Fatalf("authenticate token: %v", err)
	}
	if authn.principal.UserName != "adminuser" {
		t.Fatalf("unexpected principal user: %q", authn.principal.UserName)
	}
}

func TestExtractLoginCredentialTokenRejectsInvalidProvidedSecret(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "auth.json")
	manager := NewManager(storePath)
	if _, _, err := manager.CreateUserWithPassword("adminuser", []Role{RoleAdmin}, "password", "secret123"); err != nil {
		t.Fatalf("create user: %v", err)
	}

	store, err := loadIdentityStore(storePath)
	if err != nil {
		t.Fatalf("load store: %v", err)
	}
	state, err := compileIdentityStore(store)
	if err != nil {
		t.Fatalf("compile store: %v", err)
	}

	req, err := structpb.NewStruct(map[string]any{
		"username": "adminuser",
		"secret":   "wrong-secret",
	})
	if err != nil {
		t.Fatalf("new login payload: %v", err)
	}

	_, err = extractLoginCredentialToken(t.Context(), req, "authorization", &state)
	if err == nil {
		t.Fatalf("expected invalid credential error")
	}
	if strings.Contains(strings.ToLower(err.Error()), "no credentials") {
		t.Fatalf("expected explicit invalid credential error, got %v", err)
	}
}
