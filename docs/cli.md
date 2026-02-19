# Databasa Interactive CLI

Databasa includes an interactive shell inside the same `databasa` binary.

Start with either form:

```bash
databasa --cli
# or
databasa cli
```

## Connection flags

```bash
databasa --cli \
  --addr 127.0.0.1:50051 \
  --timeout 5s \
  --tls on \
  --ca /path/to/ca.crt \
  --cert /path/to/client.crt \
  --key /path/to/client.key \
  --insecure
```

- `--tls on|off`: force TLS mode (`--tls` alone is treated as `on`).
- `--insecure`: skips TLS verification (local development only).
- History is persisted per user in the OS config directory under `databasa/cli_history`.
- The shell connects immediately on startup and, when auth is required, prompts credentials before opening the prompt.
- If no users exist in the local auth store, startup enters bootstrap flow to create the first admin user.

## Startup flow

1. CLI connects to server using the provided flags.
2. If auth is disabled, prompt opens immediately.
3. If auth is enabled and no users exist in local auth store, bootstrap flow creates first admin user.
4. If users already exist, CLI prompts `user` and `password or api key` and authenticates before opening the prompt.

## Prompt

The prompt reflects connection and active collection:

```text
databasa(127.0.0.1:50051/feed)>
```

Commands support multiline input and execute when terminated with `;`.

## Meta commands

- `\help`
- `\quit` / `\q`
- `\connect <addr>`
- `\use <collection>`
- `\collections`
- `\users` (admin)
- `\login <username>`
- `\logout`
- `\whoami`
- `\timing on|off`
- `\format table|json|csv`

## Core commands

Users (CLI-only admin flow):

```sql
CREATE USER <name> PASSWORD ['<pass>'] [ADMIN];
ALTER USER <name> PASSWORD ['<pass>'];
DROP USER <name>;
LIST USERS;
```

`CREATE USER` and `ALTER USER ... PASSWORD ...` print the generated API key once.

If `'<pass>'` is omitted, the CLI prompts for the secret with hidden input and confirmation.

`PASSWORD ['<value>']` is treated as the API key secret in CLI grammar.
Generated key format:

```text
dbs1.<key_id>.<secret>
```

Security note: statements containing `PASSWORD` are not persisted in CLI history.

Collections:

```sql
CREATE COLLECTION <name> DIM <n> [METRIC cosine|dot|l2];
DROP COLLECTION <name>;
DESCRIBE COLLECTION <name>;
LIST COLLECTIONS;
USE <name>;
```

Data:

```sql
INSERT KEY '<k>' EMBEDDING [0.1, 0.2, 0.3];
BATCH INSERT FILE '/path/items.jsonl';
BATCH INSERT STDIN;
SEARCH TOPK 10 EMBEDDING [0.1, 0.2, 0.3];
DELETE KEY '<k>';
GET KEY '<k>';
```

## JSONL format for batch insert

One object per line:

```json
{"key":"doc-1","embedding":[0.1,0.2,0.3]}
{"key":"doc-2","embedding":[0.4,0.5,0.6]}
```

For `BATCH INSERT STDIN`, finish input with `\end`.
