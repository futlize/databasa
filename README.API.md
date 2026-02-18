# Databasa gRPC API Reference (Any Language)

This file is the canonical reference for integrating Databasa from any language.

## 1) Transport and contract

- Protocol: gRPC
- Service: `kekdb.KekDB`
- Proto file: `proto/kekdb.proto`
- Default address: `localhost:50051`
- Package: `kekdb`
- Compatibility: API package/service identifiers remain `kekdb.KekDB`.

## 2) Integration flow (language-agnostic)

1. Load `proto/kekdb.proto` in your gRPC toolchain.
2. Generate client stubs for your language (or use dynamic proto loading).
3. Connect to Databasa server over gRPC.
4. Call methods from service `kekdb.KekDB`.

Notes:
- `top_k`, `ef_search`, batch size and other limits are enforced by server config in `databasa.toml`.
- `Search` can optionally return embeddings when requested (`include_embedding=true`).

## 3) Enums

### `Metric`
- `COSINE = 0`
- `L2 = 1`
- `DOT_PRODUCT = 2`

## 4) RPC methods

### 4.1 `CreateCollection`
Signature:
```proto
rpc CreateCollection(CreateCollectionRequest) returns (CreateCollectionResponse);
```

Request:
- `name` (`string`, required)
- `dimension` (`uint32`, required, must be `> 0`)
- `metric` (`Metric`, optional, defaults to `COSINE` when omitted by proto3 zero value)

Accepted metric values:
- `COSINE`
- `L2`
- `DOT_PRODUCT`

Metric behavior:
- `COSINE`: vectors are L2-normalized on write and query vectors are L2-normalized on search; scoring uses dot product over normalized vectors.
- `L2`: squared Euclidean distance.
- `DOT_PRODUCT`: raw dot product (scored as negative dot distance).

Collection-scoped values not passed in this request:
- shard count comes from server storage config (`[storage].shards` in `databasa.toml`)
- compression comes from server storage config (`[storage].compression`, accepted: `none`, `int8`)

Validation limits:
- `dimension` must also be `<= max_collection_dim` (from guardrails config)

Response:
- Empty (`CreateCollectionResponse`)

Common errors:
- `INVALID_ARGUMENT` (name missing, invalid dimension, dimension above `max_collection_dim`)
- `ALREADY_EXISTS` (collection already exists)
- `RESOURCE_EXHAUSTED` (data dir limit reached: `max_data_dir_mb`)

---

### 4.2 `DeleteCollection`
Signature:
```proto
rpc DeleteCollection(DeleteCollectionRequest) returns (DeleteCollectionResponse);
```

Request:
- `name` (`string`, required)

Response:
- Empty (`DeleteCollectionResponse`)

Common errors:
- `NOT_FOUND` (collection not found)

---

### 4.3 `ListCollections`
Signature:
```proto
rpc ListCollections(ListCollectionsRequest) returns (ListCollectionsResponse);
```

Request:
- Empty (`ListCollectionsRequest`)

Response:
- `collections` (`repeated CollectionInfo`)
  - `name`
  - `dimension`
  - `metric`
  - `count` (active vectors)
  - `has_index`

---

### 4.4 `CreateIndex`
Signature:
```proto
rpc CreateIndex(CreateIndexRequest) returns (CreateIndexResponse);
```

Request:
- `collection` (`string`, required)
- `m` (`uint32`, optional, HNSW connectivity parameter)
- `ef_construction` (`uint32`, optional, HNSW build beam width)

Response:
- Empty (`CreateIndexResponse`)

Common errors:
- `NOT_FOUND` (collection not found)
- `ALREADY_EXISTS` (index already exists)

---

### 4.5 `DropIndex`
Signature:
```proto
rpc DropIndex(DropIndexRequest) returns (DropIndexResponse);
```

Request:
- `collection` (`string`, required)

Response:
- Empty (`DropIndexResponse`)

Common errors:
- `NOT_FOUND` (collection not found)

---

### 4.6 `Insert`
Signature:
```proto
rpc Insert(InsertRequest) returns (InsertResponse);
```

Request:
- `collection` (`string`, required)
- `key` (`string`, required)
- `embedding` (`repeated float`, required, must match collection dimension)

Behavior:
- If `key` already exists, vector is overwritten.

Response:
- Empty (`InsertResponse`)

Common errors:
- `NOT_FOUND` (collection not found)
- `INVALID_ARGUMENT` (missing key, dimension mismatch, NaN/Inf)

---

### 4.7 `Get`
Signature:
```proto
rpc Get(GetRequest) returns (GetResponse);
```

Request:
- `collection` (`string`, required)
- `key` (`string`, required)

Response:
- `key`
- `embedding`

Common errors:
- `NOT_FOUND` (collection or key not found)

---

### 4.8 `Delete`
Signature:
```proto
rpc Delete(DeleteRequest) returns (DeleteResponse);
```

Request:
- `collection` (`string`, required)
- `key` (`string`, required)

Response:
- Empty (`DeleteResponse`)

Common errors:
- `NOT_FOUND` (collection or key not found)

---

### 4.9 `BatchInsert`
Signature:
```proto
rpc BatchInsert(BatchInsertRequest) returns (BatchInsertResponse);
```

Request:
- `collection` (`string`, required)
- `items` (`repeated BatchInsertItem`, required, non-empty)
  - `key` (`string`, required)
  - `embedding` (`repeated float`, required, dimension must match collection)

Response:
- `inserted` (`uint64`)

Common errors:
- `NOT_FOUND` (collection not found)
- `INVALID_ARGUMENT` (empty batch, per-item key missing, dimension mismatch, NaN/Inf, batch > `max_batch_size`)

---

### 4.10 `Search`
Signature:
```proto
rpc Search(SearchRequest) returns (SearchResponse);
```

Request:
- `collection` (`string`, required)
- `embedding` (`repeated float`, required, query vector)
- `top_k` (`uint32`, required, must be `> 0`)
- `ef_search` (`uint32`, optional, index beam width)
- `include_embedding` (`bool`, optional; when `true`, response can include result embedding payload)

Response:
- `results` (`repeated SearchResult`)
  - `key`
  - `score` (distance; lower is better)
    - `L2`: squared Euclidean distance
    - `COSINE`: negative dot product over normalized vectors (best value is `-1`)
    - `DOT_PRODUCT`: negative dot product
  - `embedding` (present when requested)

Common errors:
- `NOT_FOUND` (collection not found)
- `INVALID_ARGUMENT` (top_k invalid, ef_search limit exceeded, dimension mismatch, NaN/Inf)

Compatibility note:
- The current server also accepts gRPC metadata `x-return-embedding: true` (or `x-include-embedding: true`) to request embeddings.
- Use `include_embedding` as the main contract for newly generated clients.

## 5) Guardrails and limits (`databasa.toml`)

Main settings that affect API behavior:

```ini
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

When concurrency limits are exceeded, server returns:
- `RESOURCE_EXHAUSTED`

When `max_data_dir_mb` is reached, write operations also return:
- `RESOURCE_EXHAUSTED`

## 6) Minimal grpcurl examples

Create collection:
```bash
grpcurl -plaintext -d '{"name":"docs","dimension":3,"metric":"COSINE"}' \
  localhost:50051 kekdb.KekDB/CreateCollection
```

Insert:
```bash
grpcurl -plaintext -d '{"collection":"docs","key":"doc:1","embedding":[0.1,0.2,0.3]}' \
  localhost:50051 kekdb.KekDB/Insert
```

Get:
```bash
grpcurl -plaintext -d '{"collection":"docs","key":"doc:1"}' \
  localhost:50051 kekdb.KekDB/Get
```

BatchInsert:
```bash
grpcurl -plaintext -d '{"collection":"docs","items":[{"key":"doc:2","embedding":[0.2,0.3,0.4]}]}' \
  localhost:50051 kekdb.KekDB/BatchInsert
```

CreateIndex:
```bash
grpcurl -plaintext -d '{"collection":"docs","m":16,"ef_construction":200}' \
  localhost:50051 kekdb.KekDB/CreateIndex
```

Search (without embedding):
```bash
grpcurl -plaintext -d '{"collection":"docs","embedding":[0.1,0.2,0.3],"top_k":5,"ef_search":50,"include_embedding":false}' \
  localhost:50051 kekdb.KekDB/Search
```

Search (with embedding):
```bash
grpcurl -plaintext -d '{"collection":"docs","embedding":[0.1,0.2,0.3],"top_k":5,"include_embedding":true}' \
  localhost:50051 kekdb.KekDB/Search
```

Delete:
```bash
grpcurl -plaintext -d '{"collection":"docs","key":"doc:1"}' \
  localhost:50051 kekdb.KekDB/Delete
```

DropIndex:
```bash
grpcurl -plaintext -d '{"collection":"docs"}' \
  localhost:50051 kekdb.KekDB/DropIndex
```

DeleteCollection:
```bash
grpcurl -plaintext -d '{"name":"docs"}' \
  localhost:50051 kekdb.KekDB/DeleteCollection
```
