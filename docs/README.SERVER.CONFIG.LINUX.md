# Databasa - Server Configuration (Ubuntu/Linux)

This guide covers only Databasa server configuration on Linux.

## 1) Configuration file

Format: `TOML` (sections + `key = value`)

Recommended path:

```bash
/etc/databasa/databasa.toml
```

## 2) Recommended configuration (production)

```ini
# Databasa server configuration (Ubuntu/Linux recommended)
# Suggested path: /etc/databasa/databasa.toml

[server]
port = 50051
grpc_max_recv_mb = 64
grpc_max_send_mb = 64
enable_reflection = false

[storage]
data_dir = /var/lib/databasa/data
shards = 8
compression = int8
write_mode = strict
wal_sync_mode = auto
wal_sync_interval_ms = 10
wal_batch_wait_ms = 1
wal_batch_max_ops = 1024
wal_queue_size = 8192

[guardrails]
max_top_k = 200
max_batch_size = 512
max_ef_search = 1024
max_collection_dim = 8192
max_data_dir_mb = 102400
require_rpc_deadline = true

[security]
auth_enabled = true
require_auth = true
api_key_header = authorization
tls_enabled = true
tls_cert_file = /etc/databasa/certs/server.crt
tls_key_file = /etc/databasa/certs/server.key
tls_client_auth = none
```

## 3) Accepted fields and values

### `[server]`

- `port`: integer > 0. Default: `50051`.
- `grpc_max_recv_mb`: integer > 0. Default: `32`.
- `grpc_max_send_mb`: integer > 0. Default: `32`.
- `enable_reflection`: bool (`true/false`, `1/0`, `yes/no`, `on/off`).

### `[storage]`

- `data_dir`: on-disk database path.
- `shards`: integer > 0. Default: `8`.
- `compression`: `none` or `int8`. Default: `int8`.
- `write_mode`: `strict` or `performance`. Default: `strict`.
- `wal_sync_mode`: `auto`, `always`, `periodic` or `none`. Default: `auto`.
- `wal_sync_interval_ms`: integer > 0. Default: `10`.
- `wal_batch_wait_ms`: integer > 0. Default: `1`.
- `wal_batch_max_ops`: integer > 0. Default: `1024`.
- `wal_queue_size`: integer > 0. Default: `8192`.

With `wal_sync_mode = auto`:
- `write_mode = strict` => WAL `always`.
- `write_mode = performance` => WAL `periodic`.

Note:
- The server no longer applies an internal request-slot concurrency limit.

### `[guardrails]`

- `max_top_k`: integer > 0. Default: `256`.
- `max_batch_size`: integer > 0. Default: `1000`.
- `max_ef_search`: integer > 0. Default: `4096`.
- `max_collection_dim`: integer > 0. Default: `8192`.
- `max_data_dir_mb`: integer >= 0.  
  - `0`: unlimited.
- `require_rpc_deadline`: bool.

### `[security]`

- `auth_enabled`: bool. Default: `true`.
- `require_auth`: bool. Default: `true`.
- `api_key_header`: header/metadata key carrying API key. Default: `authorization`.
- `tls_enabled`: bool. Default: `false`.
- `tls_cert_file`: server certificate path.
- `tls_key_file`: server private key path.
- `tls_client_auth`: `none`, `request`, `require_any`, `verify_if_given`, `require`.

## 4) Override via environment variable

`DATABASA_CONFIG` can define an alternative path for the config file.

## 5) Ubuntu preparation

```bash
sudo mkdir -p /etc/databasa
sudo mkdir -p /var/lib/databasa/data
sudo chown -R $USER:$USER /var/lib/databasa
```

## 6) Server startup

```bash
./databasa -config /etc/databasa/databasa.toml
```
