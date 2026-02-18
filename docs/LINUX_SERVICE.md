# Databasa Linux Service (Ubuntu/systemd)

Este guia documenta a instalacao e operacao do Databasa como servico systemd no Ubuntu/Linux.

## Scripts oficiais

- `scripts/install.sh`
- `scripts/uninstall.sh`

## Instalacao

Comandos exatos:

```bash
go build -o ./bin/databasa ./cmd/databasa
sudo ./scripts/install.sh
```

O instalador cria:

- usuario/grupo dedicado: `databasa:databasa`
- unit systemd com restart automatico em falha e habilitado no boot
- helper CLI em `/usr/local/bin/databasa`

## Layout FHS

- Config: `/etc/databasa/` (arquivo principal: `/etc/databasa/databasa.toml`)
- Env file: `/etc/default/databasa` (`DATABASA_CONFIG`)
- Data: `/var/lib/databasa/` (dados em `/var/lib/databasa/data`)
- Logs dir: `/var/log/databasa/`
- Runtime dir: `/run/databasa/`
- Logs de runtime: `journald` (`journalctl -u databasa`) por baixo dos panos

## Operacao via CLI helper

`<dbname>` neste repo e `databasa`, portanto:

```bash
databasa status          # <dbname> status
databasa start           # <dbname> start
databasa stop            # <dbname> stop
databasa restart         # <dbname> restart
databasa logs --follow   # <dbname> logs --follow
```

## Desinstalacao

Remove service e binarios (mantem config/dados/log dir):

```bash
sudo ./scripts/uninstall.sh
```

Remocao completa (inclui config/dados/log dir e usuario/grupo dedicados):

```bash
sudo ./scripts/uninstall.sh --purge
```
