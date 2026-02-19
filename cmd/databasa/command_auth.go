package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/futlize/databasa/internal/security"
)

func runAuthCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: databasa auth <create-user|create-key|revoke-key|disable-key|enable-key>")
	}

	switch args[0] {
	case "create-user":
		return runAuthCreateUser(args[1:])
	case "create-key":
		return runAuthCreateKey(args[1:])
	case "revoke-key":
		return runAuthRevokeKey(args[1:])
	case "disable-key":
		return runAuthDisableEnableKey(args[1:], true)
	case "enable-key":
		return runAuthDisableEnableKey(args[1:], false)
	default:
		return fmt.Errorf("unknown auth subcommand %q", args[0])
	}
}

func runAuthCreateUser(args []string) error {
	flagSet := flag.NewFlagSet("auth create-user", flag.ContinueOnError)
	flagSet.SetOutput(os.Stdout)

	configPath := flagSet.String("config", resolvedDefaultConfigPath(), "path to databasa.toml")
	userName := flagSet.String("user", "", "logical user/account name")
	rolesRaw := flagSet.String("roles", "read", "comma-separated roles: read,write,admin")
	keyName := flagSet.String("key-name", "primary", "api key display name")
	if err := flagSet.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*userName) == "" {
		return errors.New("create-user requires --user")
	}

	roles, err := parseRolesCSV(*rolesRaw)
	if err != nil {
		return err
	}

	manager, storePath, err := securityManagerFromConfig(*configPath)
	if err != nil {
		return err
	}
	user, key, err := manager.CreateUserWithAPIKey(*userName, roles, *keyName)
	if err != nil {
		return err
	}

	fmt.Printf("auth store: %s\n", storePath)
	fmt.Printf("created user: %s (id=%s roles=%s)\n", user.Name, user.ID, rolesToCSV(user.Roles))
	fmt.Printf("key id: %s\n", key.KeyID)
	fmt.Printf("api key (shown once): %s\n", key.Plaintext)
	fmt.Println("store this api key securely. Databasa never stores plaintext keys.")
	return nil
}

func runAuthCreateKey(args []string) error {
	flagSet := flag.NewFlagSet("auth create-key", flag.ContinueOnError)
	flagSet.SetOutput(os.Stdout)

	configPath := flagSet.String("config", resolvedDefaultConfigPath(), "path to databasa.toml")
	userName := flagSet.String("user", "", "logical user/account name")
	keyName := flagSet.String("key-name", "rotated", "api key display name")
	if err := flagSet.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*userName) == "" {
		return errors.New("create-key requires --user")
	}

	manager, storePath, err := securityManagerFromConfig(*configPath)
	if err != nil {
		return err
	}
	key, err := manager.CreateAPIKey(*userName, *keyName)
	if err != nil {
		return err
	}

	fmt.Printf("auth store: %s\n", storePath)
	fmt.Printf("created api key for user %s\n", strings.TrimSpace(*userName))
	fmt.Printf("key id: %s\n", key.KeyID)
	fmt.Printf("api key (shown once): %s\n", key.Plaintext)
	fmt.Println("store this api key securely. Databasa never stores plaintext keys.")
	return nil
}

func runAuthRevokeKey(args []string) error {
	flagSet := flag.NewFlagSet("auth revoke-key", flag.ContinueOnError)
	flagSet.SetOutput(os.Stdout)

	configPath := flagSet.String("config", resolvedDefaultConfigPath(), "path to databasa.toml")
	userName := flagSet.String("user", "", "logical user/account name")
	keyID := flagSet.String("key-id", "", "api key id")
	if err := flagSet.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*userName) == "" {
		return errors.New("revoke-key requires --user")
	}
	if strings.TrimSpace(*keyID) == "" {
		return errors.New("revoke-key requires --key-id")
	}

	manager, storePath, err := securityManagerFromConfig(*configPath)
	if err != nil {
		return err
	}
	if err := manager.RevokeAPIKey(*userName, *keyID); err != nil {
		return err
	}

	fmt.Printf("auth store: %s\n", storePath)
	fmt.Printf("revoked api key %s for user %s\n", strings.TrimSpace(*keyID), strings.TrimSpace(*userName))
	return nil
}

func runAuthDisableEnableKey(args []string, disable bool) error {
	flagSet := flag.NewFlagSet("auth disable-enable-key", flag.ContinueOnError)
	flagSet.SetOutput(os.Stdout)

	configPath := flagSet.String("config", resolvedDefaultConfigPath(), "path to databasa.toml")
	userName := flagSet.String("user", "", "logical user/account name")
	keyID := flagSet.String("key-id", "", "api key id")
	if err := flagSet.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*userName) == "" {
		return errors.New("command requires --user")
	}
	if strings.TrimSpace(*keyID) == "" {
		return errors.New("command requires --key-id")
	}

	manager, storePath, err := securityManagerFromConfig(*configPath)
	if err != nil {
		return err
	}
	if err := manager.SetAPIKeyDisabled(*userName, *keyID, disable); err != nil {
		return err
	}

	action := "enabled"
	if disable {
		action = "disabled"
	}
	fmt.Printf("auth store: %s\n", storePath)
	fmt.Printf("%s api key %s for user %s\n", action, strings.TrimSpace(*keyID), strings.TrimSpace(*userName))
	return nil
}

func parseRolesCSV(raw string) ([]security.Role, error) {
	parts := strings.Split(strings.TrimSpace(raw), ",")
	roles := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		roles = append(roles, trimmed)
	}
	if len(roles) == 0 {
		return nil, errors.New("at least one role is required")
	}
	return security.ParseRoles(roles)
}

func rolesToCSV(roles []security.Role) string {
	parts := make([]string, 0, len(roles))
	for _, role := range roles {
		parts = append(parts, string(role))
	}
	return strings.Join(parts, ",")
}

func securityManagerFromConfig(configPath string) (*security.Manager, string, error) {
	cfg, _, err := LoadOrCreateConfig(configPath)
	if err != nil {
		return nil, "", err
	}
	storePath := security.AuthStorePath(cfg.Storage.DataDir)
	return security.NewManager(storePath), storePath, nil
}

func resolvedDefaultConfigPath() string {
	if envPath := strings.TrimSpace(os.Getenv("DATABASA_CONFIG")); envPath != "" {
		return envPath
	}
	return defaultConfigPath
}
