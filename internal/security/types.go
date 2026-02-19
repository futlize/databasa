package security

import (
	"errors"
	"fmt"
	"strings"
)

// Role defines a permission set attached to a logical user account.
type Role string

const (
	RoleRead  Role = "read"
	RoleWrite Role = "write"
	RoleAdmin Role = "admin"
)

type roleMask uint8

const (
	roleMaskRead roleMask = 1 << iota
	roleMaskWrite
	roleMaskAdmin
)

var errNoCredentials = errors.New("no credentials")

// Permission represents the operation class required by an API endpoint.
type Permission string

const (
	PermissionRead  Permission = "read"
	PermissionWrite Permission = "write"
	PermissionAdmin Permission = "admin"
)

// Principal is the authenticated caller identity attached to request context.
type Principal struct {
	UserID   string
	UserName string
	KeyID    string
	Roles    []Role
}

// ParseRoles parses and validates role names from CLI/config inputs.
func ParseRoles(raw []string) ([]Role, error) {
	if len(raw) == 0 {
		return nil, errors.New("at least one role is required")
	}

	seen := make(map[Role]struct{}, len(raw))
	roles := make([]Role, 0, len(raw))
	for _, value := range raw {
		role := Role(strings.ToLower(strings.TrimSpace(value)))
		switch role {
		case RoleRead, RoleWrite, RoleAdmin:
		default:
			return nil, fmt.Errorf("unsupported role %q", value)
		}
		if _, exists := seen[role]; exists {
			continue
		}
		seen[role] = struct{}{}
		roles = append(roles, role)
	}
	return roles, nil
}

func roleMaskFromRoles(roles []Role) (roleMask, error) {
	if len(roles) == 0 {
		return 0, errors.New("roles cannot be empty")
	}
	var mask roleMask
	for _, role := range roles {
		switch role {
		case RoleRead:
			mask |= roleMaskRead
		case RoleWrite:
			mask |= roleMaskWrite
		case RoleAdmin:
			mask |= roleMaskAdmin
		default:
			return 0, fmt.Errorf("unsupported role %q", role)
		}
	}
	return mask, nil
}

func (m roleMask) allows(permission Permission) bool {
	if m&roleMaskAdmin != 0 {
		return true
	}
	switch permission {
	case PermissionRead:
		return m&roleMaskRead != 0
	case PermissionWrite:
		return m&roleMaskWrite != 0
	case PermissionAdmin:
		return false
	default:
		return false
	}
}
