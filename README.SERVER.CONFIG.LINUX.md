# Databasa - Configuracao de Servidor (Ubuntu/Linux)

Este guia cobre somente configuracao do servidor Databasa em Linux.

## 1) Arquivo de configuracao

Formato: `TOML` (secoes + `chave = valor`)

Caminho recomendado:

```bash
/etc/databasa/databasa.toml
```

## 2) Configuracao recomendada (producao)

```ini
# Databasa server configuration (Ubuntu/Linux recommended)
# Path sugerido: /etc/databasa/databasa.toml

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
```

## 3) Campos e valores aceitos

### `[server]`

- `port`: inteiro > 0. Default: `50051`.
- `grpc_max_recv_mb`: inteiro > 0. Default: `32`.
- `grpc_max_send_mb`: inteiro > 0. Default: `32`.
- `enable_reflection`: bool (`true/false`, `1/0`, `yes/no`, `on/off`).

### `[storage]`

- `data_dir`: caminho da base em disco.
- `shards`: inteiro > 0. Default: `8`.
- `compression`: `none` ou `int8`. Default: `int8`.
- `write_mode`: `strict` ou `performance`. Default: `strict`.
- `wal_sync_mode`: `auto`, `always`, `periodic` ou `none`. Default: `auto`.
- `wal_sync_interval_ms`: inteiro > 0. Default: `10`.
- `wal_batch_wait_ms`: inteiro > 0. Default: `1`.
- `wal_batch_max_ops`: inteiro > 0. Default: `1024`.
- `wal_queue_size`: inteiro > 0. Default: `8192`.

Com `wal_sync_mode = auto`:
- `write_mode = strict` => WAL `always`.
- `write_mode = performance` => WAL `periodic`.

Observacao:
- O servidor nao aplica mais limite interno de slots de concorrencia por request.

### `[guardrails]`

- `max_top_k`: inteiro > 0. Default: `256`.
- `max_batch_size`: inteiro > 0. Default: `1000`.
- `max_ef_search`: inteiro > 0. Default: `4096`.
- `max_collection_dim`: inteiro > 0. Default: `8192`.
- `max_data_dir_mb`: inteiro >= 0.  
  - `0`: sem limite.
- `require_rpc_deadline`: bool.

## 4) Override por variavel de ambiente

`DATABASA_CONFIG` pode definir um caminho alternativo para o arquivo de config.

## 5) Preparacao no Ubuntu

```bash
sudo mkdir -p /etc/databasa
sudo mkdir -p /var/lib/databasa/data
sudo chown -R $USER:$USER /var/lib/databasa
```

## 6) Inicializacao do servidor

```bash
./databasa -config /etc/databasa/databasa.toml
```
