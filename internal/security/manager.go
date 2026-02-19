package security

import (
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
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
