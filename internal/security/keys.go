package security

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

const (
	apiKeyPrefix = "dbs1"
)

func generateKeyID() (string, error) {
	var raw [10]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate key id entropy: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

func generateSecret() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate api key secret entropy: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

func generateSalt() ([]byte, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("generate salt entropy: %w", err)
	}
	return salt, nil
}

func formatAPIKey(keyID, secret string) string {
	return apiKeyPrefix + "." + keyID + "." + secret
}

func parseAPIKey(token string) (string, string, error) {
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) != 3 {
		return "", "", errors.New("invalid api key format")
	}
	if parts[0] != apiKeyPrefix {
		return "", "", errors.New("invalid api key prefix")
	}
	keyID := strings.TrimSpace(parts[1])
	secret := strings.TrimSpace(parts[2])
	if keyID == "" || secret == "" {
		return "", "", errors.New("invalid api key payload")
	}
	return keyID, secret, nil
}

func hashSecret(secret string, salt []byte) [32]byte {
	h := sha256.New()
	_, _ = h.Write(salt)
	_, _ = h.Write([]byte(secret))

	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum
}
