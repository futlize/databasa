# Databasa Security Guide

This document defines production security behavior for Databasa server deployments.

## 1) Authentication Model (Connection-Bound Sessions)

Databasa uses API keys, but authentication is now session-based and bound to the gRPC transport connection.

Default behavior:
- `security.auth_enabled = true`
- `security.require_auth = true`
- non-`Login` RPCs are rejected unless the connection already has an authenticated session

Handshake flow:
1. Client opens gRPC channel (TCP/TLS connection).
2. Client calls `Login` once with API key.
3. Server authenticates API key and stores session bound to that exact connection.
4. Subsequent RPCs on that same connection do not require API key metadata.
5. When connection closes, session is destroyed.

Connection binding details:
- server assigns a cryptographically random `ConnID` in gRPC `stats.TagConn`
- `ConnID` is copied into RPC context before interceptor authorization checks
- session map key is `ConnID`, with session data including user id, key id, role mask, created timestamp, and last-seen timestamp

`Login` credential input:
- preferred: `Login` request payload (`google.protobuf.StringValue.value`)
- optional compatibility fallback: gRPC metadata (`authorization: Bearer <API_KEY>` or configured `security.api_key_header`)

Security properties:
- API keys are never stored in plaintext
- stored key material uses per-key salt + SHA-256 hash
- key verification uses constant-time comparison
- secrets are never logged by server code

Auth store location:
- `<storage.data_dir>/security/auth.json`

Runtime reload:
- key/user changes in `auth.json` are hot-reloaded by the server (no restart required)
- revoked/disabled/removed keys invalidate active sessions derived from those keys immediately for subsequent RPCs

## 2) Roles and Permissions

Supported roles:
- `read`
- `write`
- `admin`

Permission mapping by RPC:
- `read`: `ListCollections`, `Get`, `Search`
- `write`: `Insert`, `Delete`, `BatchInsert`
- `admin`: `CreateCollection`, `DeleteCollection`, `CreateIndex`, `DropIndex`

Special RPC:
- `Login`: performs authentication handshake and creates/refreshes connection session

Rules:
- `admin` implies all permissions
- unknown methods are treated as admin-only
- authorization is evaluated before handler/storage/index logic runs

Failure codes:
- not logged in / invalid key: gRPC `UNAUTHENTICATED`
- authenticated but missing role: gRPC `PERMISSION_DENIED`

## 3) Anonymous Mode Safety

If `security.require_auth = false` is used, Databasa allows anonymous `read` methods only.

Anonymous mode does not allow:
- `write` methods
- `admin` methods

To perform `write`/`admin` operations in optional-auth mode, client must still call `Login`.

## 4) User and API Key Management (Interactive CLI)

Open the shell:

```bash
databasa --cli --addr 127.0.0.1:50051 --tls off
```

The interactive shell now connects and prompts authentication at startup when auth is required.
You can still use `\login <username>` manually after `\logout` or connection changes.

Manage users from inside the shell:

```sql
CREATE USER admin PASSWORD ['change-me'] ADMIN;
ALTER USER admin PASSWORD ['new-secret'];
DROP USER tempuser;
LIST USERS;
```

In CLI grammar, `PASSWORD ['<value>']` is used as the API key secret.
When `'<value>'` is omitted, CLI prompts for the secret with hidden input and confirmation.
The resulting token format remains:

```text
dbs1.<key_id>.<secret>
```

Notes:
- user/admin management commands are exposed in interactive CLI mode
- for a brand-new auth store, the first `CREATE USER ... ADMIN` runs in bootstrap mode
- key creation/rotation commands print the generated API key once
- statements containing `PASSWORD` are excluded from CLI history persistence
- API keys are still validated by the server in `dbs1.<key_id>.<secret>` format
- Databasa stores only salted hash material in `auth.json`

## 5) TLS / mTLS Configuration

Configure in `databasa.toml`:

```toml
[security]
tls_enabled = true
tls_cert_file = "/etc/databasa/certs/server.crt"
tls_key_file = "/etc/databasa/certs/server.key"
tls_client_auth = "none"
```

Supported `tls_client_auth` values:
- `none` (default): no client cert required
- `request`: request client cert, do not require
- `require_any`: require client cert, do not validate CA chain
- `verify_if_given`: validate client cert if present
- `require`: require and validate client cert (mTLS)

TLS behavior:
- when enabled, server uses TLS for gRPC transport
- minimum version is TLS 1.2
- invalid/mismatched server cert/key fails startup

## 6) Login + Request Examples

Validate `Login` with grpcurl:

```bash
grpcurl \
  -d '{"value":"dbs1.<key_id>.<secret>"}' \
  db.example.com:50051 databasa.Databasa/Login
```

Important:
- sessions are channel-bound, not process-bound
- each standalone `grpcurl` invocation opens a new connection, so it does not reuse a previous login session
- for normal workflows, use a generated client, call `Login` once, then reuse the same channel for all RPCs

If you reconnect with a new channel, call `Login` again for that channel.

## 7) Certificate Generation

Built-in self-signed generation:

```bash
databasa cert generate -config databasa.toml -cert-file ./certs/server.crt -key-file ./certs/server.key
```

Regenerate (overwrite):

```bash
databasa cert generate -config /etc/databasa/databasa.toml -force
```
