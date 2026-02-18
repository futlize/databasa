# Databasa

Databasa e um banco vetorial com API gRPC para armazenamento e busca por similaridade.

## Proposta

O projeto existe para oferecer uma base vetorial simples de operar, com foco em:

- execucao local ou self-hosted
- API gRPC direta para integracao com qualquer stack
- persistencia em disco
- operacao com Docker ou service Linux (systemd)
- guardrails de recursos e limites operacionais

## Escopo atual

- Protocolo: gRPC
- Service API: `databasa.Databasa`
- Proto: `proto/databasa.proto`
- Config padrao: `databasa.toml`
- Porta padrao: `50051`
- Metricas: `COSINE`, `L2`, `DOT_PRODUCT`
- Operacoes principais: colecoes, insert/get/delete, batch insert e search

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
index_flush_ops = 4096
compression = int8

[guardrails]
max_top_k = 256
max_batch_size = 1000
max_ef_search = 4096
max_collection_dim = 8192
max_concurrent_search = 64
max_concurrent_write = 128
max_data_dir_mb = 0
require_rpc_deadline = false
```

## Linux service (basico)

```bash
go build -o ./bin/databasa ./cmd/databasa
sudo ./scripts/install.sh
databasa status
databasa logs --follow
```

## Documentacao adicional

- API detalhada: `README.API.md`
- Service Linux: `docs/LINUX_SERVICE.md`
- Configuracao Linux: `README.SERVER.CONFIG.LINUX.md`
