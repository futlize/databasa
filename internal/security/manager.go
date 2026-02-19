package security

import (
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
)

var (
	userNamePattern = regexp.MustCompile(`^[A-Za-z0-9._-]{3,64}$`)
	keyNamePattern  = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,64}$`)
)

// Manager provides local admin operations for identities and API keys.
type Manager struct {
	path string
	mu   sync.Mutex
}

type UserView struct {
	ID       string
	Name     string
	Roles    []Role
	Disabled bool
}

type GeneratedKey struct {
	KeyID     string
	Plaintext string
	CreatedAt time.Time
}

type UserRecord struct {
	ID          string
	Name        string
	Roles       []Role
	Disabled    bool
	ActiveKeys  int
	TotalKeys   int
	LastUpdated time.Time
}

func NewManager(path string) *Manager {
	return &Manager{path: path}
}

// CreateUserWithAPIKey creates a user identity and issues the first API key.
func (m *Manager) CreateUserWithAPIKey(userName string, roles []Role, keyName string) (UserView, GeneratedKey, error) {
	normalizedName, err := validateUserName(userName)
	if err != nil {
		return UserView{}, GeneratedKey{}, err
	}
	normalizedRoles, err := normalizeRoleSlice(roles)
	if err != nil {
		return UserView{}, GeneratedKey{}, err
	}
	normalizedKeyName, err := validateKeyName(keyName)
	if err != nil {
		return UserView{}, GeneratedKey{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	store, err := loadIdentityStore(m.path)
	if err != nil {
		return UserView{}, GeneratedKey{}, err
	}
	if findUserByName(store.Users, normalizedName) != nil {
		return UserView{}, GeneratedKey{}, fmt.Errorf("user %q already exists", normalizedName)
	}

	userID, err := generateUserID()
	if err != nil {
		return UserView{}, GeneratedKey{}, err
	}
	now := time.Now().UTC()

	keyRecord, generatedKey, err := newStoredKey(normalizedKeyName, now)
	if err != nil {
		return UserView{}, GeneratedKey{}, err
	}

	user := &storedUser{
		ID:        userID,
		Name:      normalizedName,
		Roles:     normalizedRoles,
		Disabled:  false,
		CreatedAt: now,
		UpdatedAt: now,
		Keys:      []*storedKey{keyRecord},
	}
	store.Users = append(store.Users, user)

	if err := saveIdentityStore(m.path, store); err != nil {
		return UserView{}, GeneratedKey{}, err
	}
	return userView(user), generatedKey, nil
}

// CreateAPIKey rotates/creates a new API key for an existing user.
func (m *Manager) CreateAPIKey(userName, keyName string) (GeneratedKey, error) {
	normalizedName, err := validateUserName(userName)
	if err != nil {
		return GeneratedKey{}, err
	}
	normalizedKeyName, err := validateKeyName(keyName)
	if err != nil {
		return GeneratedKey{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	store, err := loadIdentityStore(m.path)
	if err != nil {
		return GeneratedKey{}, err
	}
	user := findUserByName(store.Users, normalizedName)
	if user == nil {
		return GeneratedKey{}, fmt.Errorf("user %q not found", normalizedName)
	}

	now := time.Now().UTC()
	keyRecord, generatedKey, err := newStoredKey(normalizedKeyName, now)
	if err != nil {
		return GeneratedKey{}, err
	}
	user.Keys = append(user.Keys, keyRecord)
	user.UpdatedAt = now

	if err := saveIdentityStore(m.path, store); err != nil {
		return GeneratedKey{}, err
	}
	return generatedKey, nil
}

// CreateUserWithPassword creates a user with a deterministic secret that can be used as a password in CLI login.
func (m *Manager) CreateUserWithPassword(userName string, roles []Role, keyName, password string) (UserView, GeneratedKey, error) {
	normalizedName, err := validateUserName(userName)
	if err != nil {
		return UserView{}, GeneratedKey{}, err
	}
	normalizedRoles, err := normalizeRoleSlice(roles)
	if err != nil {
		return UserView{}, GeneratedKey{}, err
	}
	normalizedKeyName, err := validateKeyName(keyName)
	if err != nil {
		return UserView{}, GeneratedKey{}, err
	}
	secret, err := normalizePasswordSecret(password)
	if err != nil {
		return UserView{}, GeneratedKey{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	store, err := loadIdentityStore(m.path)
	if err != nil {
		return UserView{}, GeneratedKey{}, err
	}
	if findUserByName(store.Users, normalizedName) != nil {
		return UserView{}, GeneratedKey{}, fmt.Errorf("user %q already exists", normalizedName)
	}

	userID, err := generateUserID()
	if err != nil {
		return UserView{}, GeneratedKey{}, err
	}
	now := time.Now().UTC()

	keyRecord, generatedKey, err := newStoredKeyWithSecret(normalizedKeyName, secret, now)
	if err != nil {
		return UserView{}, GeneratedKey{}, err
	}

	user := &storedUser{
		ID:        userID,
		Name:      normalizedName,
		Roles:     normalizedRoles,
		Disabled:  false,
		CreatedAt: now,
		UpdatedAt: now,
		Keys:      []*storedKey{keyRecord},
	}
	store.Users = append(store.Users, user)

	if err := saveIdentityStore(m.path, store); err != nil {
		return UserView{}, GeneratedKey{}, err
	}
	return userView(user), generatedKey, nil
}

// AlterUserPassword invalidates existing keys and creates a replacement key using the provided secret.
func (m *Manager) AlterUserPassword(userName, keyName, password string) (GeneratedKey, error) {
	normalizedName, err := validateUserName(userName)
	if err != nil {
		return GeneratedKey{}, err
	}
	normalizedKeyName, err := validateKeyName(keyName)
	if err != nil {
		return GeneratedKey{}, err
	}
	secret, err := normalizePasswordSecret(password)
	if err != nil {
		return GeneratedKey{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	store, err := loadIdentityStore(m.path)
	if err != nil {
		return GeneratedKey{}, err
	}
	user := findUserByName(store.Users, normalizedName)
	if user == nil {
		return GeneratedKey{}, fmt.Errorf("user %q not found", normalizedName)
	}

	now := time.Now().UTC()
	for _, key := range user.Keys {
		if key == nil {
			continue
		}
		key.Disabled = true
		if key.RevokedAt == nil {
			key.RevokedAt = &now
		}
	}

	keyRecord, generatedKey, err := newStoredKeyWithSecret(normalizedKeyName, secret, now)
	if err != nil {
		return GeneratedKey{}, err
	}
	user.Keys = append(user.Keys, keyRecord)
	user.UpdatedAt = now

	if err := saveIdentityStore(m.path, store); err != nil {
		return GeneratedKey{}, err
	}
	return generatedKey, nil
}

// DeleteUser removes a user and all associated keys.
func (m *Manager) DeleteUser(userName string) error {
	normalizedName, err := validateUserName(userName)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	store, err := loadIdentityStore(m.path)
	if err != nil {
		return err
	}

	idx := -1
	for i, user := range store.Users {
		if user != nil && user.Name == normalizedName {
			idx = i
			break
		}
	}
	if idx == -1 {
		return fmt.Errorf("user %q not found", normalizedName)
	}

	store.Users = append(store.Users[:idx], store.Users[idx+1:]...)
	return saveIdentityStore(m.path, store)
}

// ListUsers returns deterministic user records, including active/total key counts.
func (m *Manager) ListUsers() ([]UserRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	store, err := loadIdentityStore(m.path)
	if err != nil {
		return nil, err
	}

	out := make([]UserRecord, 0, len(store.Users))
	for _, user := range store.Users {
		if user == nil {
			continue
		}
		activeKeys := 0
		for _, key := range user.Keys {
			if key == nil || key.Disabled || key.RevokedAt != nil {
				continue
			}
			activeKeys++
		}
		out = append(out, UserRecord{
			ID:          user.ID,
			Name:        user.Name,
			Roles:       append([]Role(nil), user.Roles...),
			Disabled:    user.Disabled,
			ActiveKeys:  activeKeys,
			TotalKeys:   len(user.Keys),
			LastUpdated: user.UpdatedAt,
		})
	}
	slices.SortFunc(out, func(a, b UserRecord) int {
		return strings.Compare(a.Name, b.Name)
	})
	return out, nil
}

// ActiveKeyIDsForUser returns active (non-disabled, non-revoked) key IDs for a user.
func (m *Manager) ActiveKeyIDsForUser(userName string) ([]string, error) {
	normalizedName, err := validateUserName(userName)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	store, err := loadIdentityStore(m.path)
	if err != nil {
		return nil, err
	}
	user := findUserByName(store.Users, normalizedName)
	if user == nil {
		return nil, fmt.Errorf("user %q not found", normalizedName)
	}
	if user.Disabled {
		return nil, fmt.Errorf("user %q is disabled", normalizedName)
	}

	out := make([]string, 0, len(user.Keys))
	for _, key := range user.Keys {
		if key == nil || key.Disabled || key.RevokedAt != nil {
			continue
		}
		out = append(out, key.ID)
	}
	slices.Sort(out)
	if len(out) == 0 {
		return nil, fmt.Errorf("user %q has no active keys", normalizedName)
	}
	return out, nil
}

// RevokeAPIKey permanently revokes a key.
func (m *Manager) RevokeAPIKey(userName, keyID string) error {
	normalizedName, err := validateUserName(userName)
	if err != nil {
		return err
	}
	if strings.TrimSpace(keyID) == "" {
		return errors.New("key id is required")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	store, err := loadIdentityStore(m.path)
	if err != nil {
		return err
	}
	user := findUserByName(store.Users, normalizedName)
	if user == nil {
		return fmt.Errorf("user %q not found", normalizedName)
	}

	key := findKeyByID(user.Keys, keyID)
	if key == nil {
		return fmt.Errorf("key %q not found for user %q", keyID, normalizedName)
	}
	now := time.Now().UTC()
	key.Disabled = true
	if key.RevokedAt == nil {
		key.RevokedAt = &now
	}
	user.UpdatedAt = now

	return saveIdentityStore(m.path, store)
}

// SetAPIKeyDisabled toggles whether a key can be used.
func (m *Manager) SetAPIKeyDisabled(userName, keyID string, disabled bool) error {
	normalizedName, err := validateUserName(userName)
	if err != nil {
		return err
	}
	if strings.TrimSpace(keyID) == "" {
		return errors.New("key id is required")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	store, err := loadIdentityStore(m.path)
	if err != nil {
		return err
	}
	user := findUserByName(store.Users, normalizedName)
	if user == nil {
		return fmt.Errorf("user %q not found", normalizedName)
	}
	key := findKeyByID(user.Keys, keyID)
	if key == nil {
		return fmt.Errorf("key %q not found for user %q", keyID, normalizedName)
	}
	if key.RevokedAt != nil && !disabled {
		return fmt.Errorf("key %q is revoked and cannot be re-enabled", keyID)
	}

	key.Disabled = disabled
	user.UpdatedAt = time.Now().UTC()
	return saveIdentityStore(m.path, store)
}

func userView(user *storedUser) UserView {
	if user == nil {
		return UserView{}
	}
	roles := append([]Role(nil), user.Roles...)
	return UserView{
		ID:       user.ID,
		Name:     user.Name,
		Roles:    roles,
		Disabled: user.Disabled,
	}
}

func newStoredKey(keyName string, now time.Time) (*storedKey, GeneratedKey, error) {
	keyID, err := generateKeyID()
	if err != nil {
		return nil, GeneratedKey{}, err
	}
	secret, err := generateSecret()
	if err != nil {
		return nil, GeneratedKey{}, err
	}
	salt, err := generateSalt()
	if err != nil {
		return nil, GeneratedKey{}, err
	}

	sum := hashSecret(secret, salt)
	record := &storedKey{
		ID:        keyID,
		Name:      keyName,
		Salt:      base64.RawURLEncoding.EncodeToString(salt),
		Hash:      base64.RawURLEncoding.EncodeToString(sum[:]),
		Disabled:  false,
		CreatedAt: now,
	}
	return record, GeneratedKey{
		KeyID:     keyID,
		Plaintext: formatAPIKey(keyID, secret),
		CreatedAt: now,
	}, nil
}

func newStoredKeyWithSecret(keyName, secret string, now time.Time) (*storedKey, GeneratedKey, error) {
	keyID, err := generateKeyID()
	if err != nil {
		return nil, GeneratedKey{}, err
	}
	salt, err := generateSalt()
	if err != nil {
		return nil, GeneratedKey{}, err
	}

	sum := hashSecret(secret, salt)
	record := &storedKey{
		ID:        keyID,
		Name:      keyName,
		Salt:      base64.RawURLEncoding.EncodeToString(salt),
		Hash:      base64.RawURLEncoding.EncodeToString(sum[:]),
		Disabled:  false,
		CreatedAt: now,
	}
	return record, GeneratedKey{
		KeyID:     keyID,
		Plaintext: formatAPIKey(keyID, secret),
		CreatedAt: now,
	}, nil
}

func generateUserID() (string, error) {
	return generateKeyID()
}

func normalizeRoleSlice(input []Role) ([]Role, error) {
	if len(input) == 0 {
		return nil, errors.New("at least one role is required")
	}
	seen := make(map[Role]struct{}, len(input))
	roles := make([]Role, 0, len(input))
	for _, role := range input {
		clean := Role(strings.ToLower(strings.TrimSpace(string(role))))
		switch clean {
		case RoleRead, RoleWrite, RoleAdmin:
		default:
			return nil, fmt.Errorf("unsupported role %q", role)
		}
		if _, exists := seen[clean]; exists {
			continue
		}
		seen[clean] = struct{}{}
		roles = append(roles, clean)
	}
	return roles, nil
}

func validateUserName(raw string) (string, error) {
	userName := strings.TrimSpace(raw)
	if !userNamePattern.MatchString(userName) {
		return "", errors.New("user name must match ^[A-Za-z0-9._-]{3,64}$")
	}
	return userName, nil
}

func validateKeyName(raw string) (string, error) {
	keyName := strings.TrimSpace(raw)
	if keyName == "" {
		return "default", nil
	}
	if !keyNamePattern.MatchString(keyName) {
		return "", errors.New("key name must match ^[A-Za-z0-9._:-]{1,64}$")
	}
	return keyName, nil
}

func normalizePasswordSecret(raw string) (string, error) {
	secret := strings.TrimSpace(raw)
	if secret == "" {
		return "", errors.New("password cannot be empty")
	}
	if strings.Contains(secret, ".") {
		return "", errors.New("password cannot contain '.'")
	}
	return secret, nil
}

func findUserByName(users []*storedUser, name string) *storedUser {
	for _, user := range users {
		if user != nil && user.Name == name {
			return user
		}
	}
	return nil
}

func findKeyByID(keys []*storedKey, keyID string) *storedKey {
	for _, key := range keys {
		if key != nil && key.ID == keyID {
			return key
		}
	}
	return nil
}
