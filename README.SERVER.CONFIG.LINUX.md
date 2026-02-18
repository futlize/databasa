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
index_flush_ops = 4096
compression = int8

[guardrails]
max_top_k = 200
max_batch_size = 512
max_ef_search = 1024
max_collection_dim = 8192
max_concurrent_search = 32
max_concurrent_write = 64
max_data_dir_mb = 102400
require_rpc_deadline = true

[resources]
# 0 = auto (min(NumCPU/2, 8))
max_workers = 0
memory_budget_percent = 80
insert_queue_size = 4096
# Em carga massiva de ingestao: true (e depois volte para false)
bulk_load_mode = false
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
- `index_flush_ops`: inteiro >= 0.  
  - `0`: nao faz flush periodico do indice por operacoes.
- `compression`: `none` ou `int8`. Default: `int8`.

### `[guardrails]`

- `max_top_k`: inteiro > 0. Default: `256`.
- `max_batch_size`: inteiro > 0. Default: `1000`.
- `max_ef_search`: inteiro > 0. Default: `4096`.
- `max_collection_dim`: inteiro > 0. Default: `8192`.
- `max_concurrent_search`: inteiro > 0. Default: `64`.
- `max_concurrent_write`: inteiro > 0. Default: `128`.
- `max_data_dir_mb`: inteiro >= 0.  
  - `0`: sem limite.
- `require_rpc_deadline`: bool.

### `[resources]`

- `max_workers`: inteiro > 0, ou `0` para auto.  
  - Auto: `min(runtime.NumCPU()/2, 8)`.
- `memory_budget_percent`: inteiro de `10` a `95`. Default: `80`.
- `insert_queue_size`: inteiro > 0. Default: `4096`.
- `bulk_load_mode`: bool.

## 4) Override por variavel de ambiente

`BULK_LOAD_MODE` sobrescreve o valor do arquivo.
`DATABASA_CONFIG` pode definir um caminho alternativo para o arquivo de config.

Exemplo:

```bash
export BULK_LOAD_MODE=true
```

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

## 7) Perfil de ingestao massiva (bulk load)

Durante ingestao grande:

1. Defina `bulk_load_mode = true`.
2. Execute ingestao.
3. Volte para `bulk_load_mode = false` ao terminar.

Isso desativa update online de indice durante insert e faz rebuild depois.
