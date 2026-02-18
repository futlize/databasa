# Databasa

Databasa e um banco vetorial com API gRPC para armazenamento e busca por similaridade.

## Proposta

O projeto existe para oferecer uma base vetorial simples de operar, com foco em:

- execucao local ou self-hosted
- API gRPC direta para integracao com qualquer stack
- persistencia em disco
- operacao com Docker ou service Linux (systemd)
- guardrails de API e limites operacionais

## Escopo atual

- Protocolo: gRPC
- Service API: `databasa.Databasa`
- Proto: `proto/databasa.proto`
- Config padrao: `databasa.toml`
- Porta padrao: `50051`
- Metricas: `COSINE`, `L2`, `DOT_PRODUCT`
- Operacoes principais: colecoes, insert/get/delete, batch insert e search

## Arquitetura de ingestao e busca

- Write path desacoplado: `upsert -> WAL (duravel conforme politica) -> active partition`.
- Busca sempre consulta `active partition` + `sealed partitions`.
- ANN (HNSW) e merge/compaction rodam em background por particao, sem bloquear ACK de escrita.
- Depois de `Insert`/`BatchInsert` bem-sucedido, o vetor fica imediatamente consultavel.

## Uso basico

### 1) Rodar localmente

```bash
go run ./cmd/databasa
```

Com arquivo de configuracao explicito:

```bash
go run ./cmd/databasa -config ./databasa.toml
```

### 2) Build do binario

```bash
go build -o ./bin/databasa ./cmd/databasa
./bin/databasa -config ./databasa.toml
```

### 2.1) Build de release para Ubuntu (linux/amd64)

PowerShell:

```powershell
New-Item -ItemType Directory -Force -Path .\dist | Out-Null
$env:CGO_ENABLED="0"; $env:GOOS="linux"; $env:GOARCH="amd64"; go build -trimpath -ldflags "-s -w" -o ./dist/databasa_linux_amd64 ./cmd/databasa
```

Bash:

```bash
mkdir -p ./dist
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "-s -w" -o ./dist/databasa_linux_amd64 ./cmd/databasa
```

### 3) Rodar com Docker

```bash
cp .env.example .env
docker compose up --build -d
docker compose down
```

### 4) Smoke test rapido (grpcurl)

```bash
grpcurl -plaintext -d '{"name":"embeddings","dimension":3,"metric":"COSINE"}' \
  localhost:50051 databasa.Databasa/CreateCollection

grpcurl -plaintext -d '{"collection":"embeddings","key":"doc1","embedding":[0.1,0.2,0.3]}' \
  localhost:50051 databasa.Databasa/Insert

grpcurl -plaintext -d '{"collection":"embeddings","embedding":[0.1,0.2,0.3],"top_k":5,"ef_search":50}' \
  localhost:50051 databasa.Databasa/Search
```

## Benchmark (200k fixo)

O benchmark de insert agora usa contagem fixa de `200000` vetores.

```bash
go run ./cmd/benchmark -embedded=true -workers 4 -batch-size 1000
```

PowerShell:

```powershell
pwsh ./scripts/benchmark.ps1 -Embedded
```

## Configuracao minima

Arquivo `databasa.toml`:

```toml
[server]
port = 50051
grpc_max_recv_mb = 32
grpc_max_send_mb = 32
enable_reflection = false

[storage]
data_dir = ./data
shards = 8
compression = int8
write_mode = strict
wal_sync_mode = auto
wal_sync_interval_ms = 10
wal_batch_wait_ms = 1
wal_batch_max_ops = 1024
wal_queue_size = 8192
active_partition_max_ops = 20000
active_partition_max_bytes_mb = 64
partition_max_count = 12
partition_merge_fan_in = 4
partition_index_min_rows = 512
index_workers = 2
merge_workers = 1
optimizer_queue_cap = 256
index_m = 16
index_ef_construction = 200
index_ef_search = 50

[guardrails]
max_top_k = 256
max_batch_size = 1000
max_ef_search = 4096
max_collection_dim = 8192
max_data_dir_mb = 0
require_rpc_deadline = false
```

`write_mode`:
- `strict`: perfil de durabilidade WAL estrito (default `always`).
- `performance`: perfil de durabilidade WAL orientado a throughput (default `periodic`).

`wal_sync_mode` (`auto|always|periodic|none`) pode sobrescrever a politica de durabilidade derivada do `write_mode`.

## Linux service (basico)

```bash
# Sem Go (usa release precompilada por padrao):
sudo ./scripts/install.sh

# Opcional: fixar uma tag de release especifica (default = latest)
# sudo env DATABASA_RELEASE_TAG=v0.1.2 ./scripts/install.sh
#
# Opcional: URL explicita do binario/arquivo .tar.gz/.tgz
# export DATABASA_BIN_URL="https://.../databasa_linux_amd64"
# sudo ./scripts/install.sh

# Opcional: compilar localmente (fluxo de desenvolvimento)
go build -o ./bin/databasa ./cmd/databasa
sudo ./scripts/install.sh
databasa status
databasa logs --follow
```

## Documentacao adicional

- API detalhada: `README.API.md`
- Service Linux: `docs/LINUX_SERVICE.md`
- Configuracao Linux: `README.SERVER.CONFIG.LINUX.md`
- Arquitetura interna: `ARCHITECTURE.md`
- Performance/benchmarks: `docs/PERFORMANCE.md`
