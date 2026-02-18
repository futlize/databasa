# Databasa (Vector DB) - Guia para usar em projeto Node.js

Este repositorio expoe um banco vetorial via gRPC.
Use este README como handoff para outra IA integrar o Databasa em um backend Node.js.
Para referencia completa da API (todas as linguagens), veja `README.API.md`.

## Resumo rapido

- Protocolo: gRPC (sem REST)
- Service: `kekdb.KekDB`
- Proto source: `proto/kekdb.proto`
- Compatibilidade: nome de pacote/servico da API gRPC mantido em `kekdb.KekDB`
- Porta padrao: `50051`
- Configuracao: `databasa.toml` (formato TOML simples)
- Persistencia local: pasta `data/`
- Metricas: `COSINE`, `L2`, `DOT_PRODUCT`
- Persistencia binaria: `collection.meta`, `meta.bin`, `wal-active.log`, `segments/*.seg`, `hnsw.graph`
- Compressao de vetor: `int8` (padrao)

## Subindo o servidor Databasa

### Opcao 1: sem Docker

```bash
go run ./cmd/databasa-server
```

Ou explicitando o arquivo de configuracao:

```bash
go run ./cmd/databasa-server -config ./databasa.toml
```

Exemplo de configuracao (`databasa.toml`):

```ini
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

Para carga de ingestao pesada, use `index_flush_ops = 0` no arquivo para desabilitar flush periodico do HNSW durante inserts (o indice continua sendo salvo no shutdown gracioso).

### Opcao 2: com Docker

Copie e ajuste os limites/porta:

```bash
cp .env.example .env
```

Depois suba:

```bash
docker compose up --build -d
```

Se quiser parar:

```bash
docker compose down
```

`docker-compose.yml` ja vem com:
- `restart: unless-stopped`
- controle de bind de rede (`DATABASA_BIND_IP` + `DATABASA_PORT`)
- limites de CPU/RAM/PIDs
- rotacao de logs para limitar uso de disco

Para limitar o tamanho total da pasta de dados do banco, configure `max_data_dir_mb` em `databasa.toml`.

## Instalacao (Ubuntu/Linux com systemd)

Scripts oficiais de install/uninstall:
- `scripts/install.sh`
- `scripts/uninstall.sh`

Comandos exatos:

```bash
go build -o ./bin/databasa ./cmd/databasa-server
sudo ./scripts/install.sh
```

Para remover o service e os binarios (mantem config/dados/log dir):

```bash
sudo ./scripts/uninstall.sh
```

Para remocao completa (inclui `/etc/databasa/`, `/var/lib/databasa/`, `/var/log/databasa/`):

```bash
sudo ./scripts/uninstall.sh --purge
```

Layout FHS usado pela instalacao:
- Config: `/etc/databasa/` (arquivo principal: `/etc/databasa/databasa.toml`)
- Env file: `/etc/default/databasa` (define `DATABASA_CONFIG`)
- Data: `/var/lib/databasa/` (dados em `/var/lib/databasa/data`)
- Logs dir: `/var/log/databasa/`
- Runtime dir: `/run/databasa/`
- Runtime logs: `journald` por baixo dos panos (`journalctl -u databasa`)

Comandos de operacao via CLI helper (`<dbname>` = `databasa` neste projeto):

```bash
databasa status          # <dbname> status
databasa start           # <dbname> start
databasa stop            # <dbname> stop
databasa restart         # <dbname> restart
databasa logs --follow   # <dbname> logs --follow
```

Observacao: o helper usa `systemctl` e `journalctl -u databasa` internamente.
Guia Linux completo: `docs/LINUX_SERVICE.md`.

## Rodando como servico do sistema (Windows)

Arquivos prontos:
- Windows Service: `deploy/windows/install-service.ps1`
- Firewall Windows: `deploy/windows/configure-firewall.ps1`

## API disponivel (gRPC)

Service: `kekdb.KekDB`

- `CreateCollection`
- `DeleteCollection`
- `ListCollections`
- `CreateIndex`
- `DropIndex`
- `Insert`
- `Get`
- `Delete`
- `BatchInsert` (unary, nao streaming)
- `Search`

## Integracao Node.js (grpc-js + proto-loader)

No projeto Node:

```bash
npm i @grpc/grpc-js @grpc/proto-loader
```

Crie `databasaClient.js`:

```js
const path = require("path");
const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");

const PROTO_PATH = path.resolve(__dirname, "./proto/kekdb.proto");

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true, // mantem nomes snake_case do proto (top_k, ef_search...)
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const proto = grpc.loadPackageDefinition(packageDefinition).kekdb;

function createDatabasaClient(address = "localhost:50051") {
  return new proto.KekDB(address, grpc.credentials.createInsecure());
}

function unary(client, method, payload) {
  return new Promise((resolve, reject) => {
    client[method](payload, (err, res) => {
      if (err) return reject(err);
      resolve(res);
    });
  });
}

async function demo() {
  const client = createDatabasaClient();

  await unary(client, "CreateCollection", {
    name: "embeddings",
    dimension: 3,
    metric: "COSINE",
  });

  await unary(client, "Insert", {
    collection: "embeddings",
    key: "doc:1",
    embedding: [0.1, 0.2, 0.3],
  });

  await unary(client, "Insert", {
    collection: "embeddings",
    key: "doc:2",
    embedding: [0.11, 0.19, 0.29],
  });

  await unary(client, "CreateIndex", {
    collection: "embeddings",
    m: 16,
    ef_construction: 200,
  });

  const result = await unary(client, "Search", {
    collection: "embeddings",
    embedding: [0.1, 0.2, 0.3],
    top_k: 5,
    ef_search: 50,
    include_embedding: false,
  });

  console.log("Search:", result);
}

demo().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

## Formato dos payloads mais usados

### CreateCollection

```json
{
  "name": "embeddings",
  "dimension": 768,
  "metric": "COSINE"
}
```

### CreateCollection (campos e valores aceitos)

- `name` (`string`, obrigatorio): nome da colecao. Nao pode ser vazio.
- `dimension` (`uint32`, obrigatorio): tamanho do vetor. Deve ser `> 0` e `<= max_collection_dim` (guardrail do servidor; padrao `8192`).
- `metric` (`enum Metric`, opcional): valores aceitos `COSINE`, `L2`, `DOT_PRODUCT`. Se omitido, usa `COSINE` (valor default do proto3).

Comportamento de cada metrica:

- `COSINE`: normaliza vetores automaticamente (norma L2=1) no write path e normaliza a query no `Search`. A comparacao usa dot product em vetores normalizados (estilo Qdrant).
- `L2`: distancia euclidiana quadratica, sem normalizacao automatica.
- `DOT_PRODUCT`: produto escalar bruto, sem normalizacao automatica.

Campos que nao fazem parte do `CreateCollection`:

- `shards`: vem de `[storage].shards` no `databasa.toml`.
- `compression`: vem de `[storage].compression` no `databasa.toml` (`none` ou `int8`).

Erros comuns no `CreateCollection`:

- `INVALID_ARGUMENT`: nome vazio, dimensao invalida ou dimensao acima do limite.
- `ALREADY_EXISTS`: colecao com mesmo nome ja existe.
- `RESOURCE_EXHAUSTED`: limite de `max_data_dir_mb` atingido.

### Insert

```json
{
  "collection": "embeddings",
  "key": "doc:123",
  "embedding": [0.1, 0.2, 0.3]
}
```

### BatchInsert

```json
{
  "collection": "embeddings",
  "items": [
    { "key": "doc:1", "embedding": [0.1, 0.2, 0.3] },
    { "key": "doc:2", "embedding": [0.2, 0.3, 0.4] }
  ]
}
```

### Search

```json
{
  "collection": "embeddings",
  "embedding": [0.1, 0.2, 0.3],
  "top_k": 10,
  "ef_search": 50,
  "include_embedding": false
}
```

## Regras importantes para integracao

- `embedding.length` precisa bater com `dimension` da colecao.
- `top_k` deve ser maior que zero.
- `top_k` e `ef_search` tambem respeitam limites de guardrail do servidor (`max_top_k`, `max_ef_search` no `databasa.toml`).
- `BatchInsert` respeita limite maximo por chamada (`max_batch_size` no `databasa.toml`).
- `SearchRequest.include_embedding=true` pede para incluir `embedding` em cada `SearchResult`.
- `key` nao pode ser vazia.
- embeddings com `NaN`/`Inf` sao rejeitados com `InvalidArgument`.
- `score` em `SearchResult` representa distancia (quanto menor, mais similar).
- Se sobrescrever a mesma key com novo embedding, o valor antigo e invalidado.

## Smoke test rapido via grpcurl

```bash
grpcurl -plaintext -d '{"name":"embeddings","dimension":3,"metric":"COSINE"}' \
  localhost:50051 kekdb.KekDB/CreateCollection

grpcurl -plaintext -d '{"collection":"embeddings","key":"doc1","embedding":[0.1,0.2,0.3]}' \
  localhost:50051 kekdb.KekDB/Insert

grpcurl -plaintext -d '{"collection":"embeddings","embedding":[0.1,0.2,0.3],"top_k":5,"ef_search":50}' \
  localhost:50051 kekdb.KekDB/Search
```

## Prompt pronto para outra IA (copiar e colar)

Use este texto em outra IA para acelerar a integracao:

```text
Integre meu backend Node.js com o Databasa via gRPC.

Contexto:
- Proto: ./proto/kekdb.proto
- Service: kekdb.KekDB
- Endereco: localhost:50051
- Biblioteca Node: @grpc/grpc-js + @grpc/proto-loader
- keepCase=true para usar campos top_k, ef_search, ef_construction

Objetivo:
1) Criar modulo databasaClient com metodos CreateCollection, Insert, BatchInsert, Search, Get, Delete.
2) Adicionar retries para erros transientes de gRPC.
3) Criar endpoint HTTP no meu backend para chamar Search e retornar top_k resultados.
4) Validar dimension antes de Insert/BatchInsert.
5) Adicionar testes unitarios para serializacao e chamadas gRPC.
```
