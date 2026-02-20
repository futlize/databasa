package security

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const (
	storeSchemaVersion = 1

	defaultAuthStoreDirName  = "security"
	defaultAuthStoreFileName = "auth.json"
)

type identityStore struct {
	Version int           `json:"version"`
	Users   []*storedUser `json:"users"`
}

type storedUser struct {
	ID        string       `json:"id"`
	Name      string       `json:"name"`
	Roles     []Role       `json:"roles"`
	Disabled  bool         `json:"disabled"`
	CreatedAt time.Time    `json:"created_at"`
	UpdatedAt time.Time    `json:"updated_at"`
	Keys      []*storedKey `json:"keys"`
}

type storedKey struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	Salt      string     `json:"salt"`
	Hash      string     `json:"hash"`
	Disabled  bool       `json:"disabled"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
}

func defaultIdentityStore() identityStore {
	return identityStore{
		Version: storeSchemaVersion,
		Users:   make([]*storedUser, 0),
	}
}

func AuthStorePath(dataDir string) string {
	base := strings.TrimSpace(dataDir)
	if base == "" {
		base = "."
	}
	return filepath.Join(base, defaultAuthStoreDirName, defaultAuthStoreFileName)
}

func loadIdentityStore(path string) (identityStore, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return defaultIdentityStore(), nil
		}
		return identityStore{}, fmt.Errorf("read auth store: %w", err)
	}
	if len(data) == 0 {
		return defaultIdentityStore(), nil
	}

	var store identityStore
	if err := json.Unmarshal(data, &store); err != nil {
		return identityStore{}, fmt.Errorf("parse auth store: %w", err)
	}
	if store.Version == 0 {
		store.Version = storeSchemaVersion
	}
	if store.Version != storeSchemaVersion {
		return identityStore{}, fmt.Errorf("unsupported auth store version %d", store.Version)
	}
	if store.Users == nil {
		store.Users = make([]*storedUser, 0)
	}

	for i, user := range store.Users {
		if user == nil {
			return identityStore{}, fmt.Errorf("invalid auth store: user[%d] is null", i)
		}
		if strings.TrimSpace(user.ID) == "" {
			return identityStore{}, fmt.Errorf("invalid auth store: user %q missing id", user.Name)
		}
		if strings.TrimSpace(user.Name) == "" {
			return identityStore{}, fmt.Errorf("invalid auth store: user[%d] missing name", i)
		}
		if _, err := roleMaskFromRoles(user.Roles); err != nil {
			return identityStore{}, fmt.Errorf("invalid auth store roles for user %q: %w", user.Name, err)
		}
		if user.Keys == nil {
			user.Keys = make([]*storedKey, 0)
		}
		for j, key := range user.Keys {
			if key == nil {
				return identityStore{}, fmt.Errorf("invalid auth store: user %q key[%d] is null", user.Name, j)
			}
			if strings.TrimSpace(key.ID) == "" || strings.TrimSpace(key.Hash) == "" || strings.TrimSpace(key.Salt) == "" {
				return identityStore{}, fmt.Errorf("invalid auth store: user %q has malformed key record", user.Name)
			}
		}
	}
	return store, nil
}

func saveIdentityStore(path string, store identityStore) error {
	store.Version = storeSchemaVersion
	if store.Users == nil {
		store.Users = make([]*storedUser, 0)
	}
	slices.SortFunc(store.Users, func(a, b *storedUser) int {
		return strings.Compare(a.Name, b.Name)
	})
	for _, user := range store.Users {
		if user.Keys == nil {
			user.Keys = make([]*storedKey, 0)
			continue
		}
		slices.SortFunc(user.Keys, func(a, b *storedKey) int {
			return strings.Compare(a.ID, b.ID)
		})
	}

	payload, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal auth store: %w", err)
	}
	payload = append(payload, '\n')

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create auth store dir: %w", err)
	}

	tmpFile, err := os.CreateTemp(dir, "auth-*.tmp")
	if err != nil {
		return fmt.Errorf("create auth store temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	cleanup := true
	defer func() {
		_ = tmpFile.Close()
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()

	if err := tmpFile.Chmod(0o600); err != nil {
		return fmt.Errorf("set auth store temp permissions: %w", err)
	}
	if _, err := tmpFile.Write(payload); err != nil {
		return fmt.Errorf("write auth store temp file: %w", err)
	}
	if err := tmpFile.Sync(); err != nil {
		return fmt.Errorf("sync auth store temp file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("close auth store temp file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace auth store: %w", err)
	}
	if err := os.Chmod(path, 0o640); err != nil {
		return fmt.Errorf("set auth store permissions: %w", err)
	}

	cleanup = false
	return nil
}
