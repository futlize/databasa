# Databasa Linux Service (Ubuntu/systemd)

Este guia documenta a instalacao e operacao do Databasa como servico systemd no Ubuntu/Linux.

## Scripts oficiais

- `scripts/install.sh`
- `scripts/uninstall.sh`

## Instalacao

Comandos exatos:

```bash
sudo ./scripts/install.sh
```

`scripts/install.sh` resolve o binario nesta ordem:

1. `./bin/databasa` (quando ja existe no repo)
2. `DATABASA_BIN_URL` (URL direta para binario ou `.tar.gz`/`.tgz`)
3. Release GitHub precompilada (`latest` por padrao)
4. Build local com Go (fallback)

Variaveis opcionais para controlar download:

- `DATABASA_BIN_URL`: URL explicita de binario/arquivo.
- `DATABASA_RELEASE_REPO`: repo para download de release (default: `futlize/databasa`).
- `DATABASA_RELEASE_TAG`: tag da release (default: `latest`).
- `DATABASA_RELEASE_ASSET`: nome exato do asset (quando quiser fixar um artefato especifico).

Exemplo sem Go, fixando uma tag:

```bash
export DATABASA_RELEASE_TAG=v0.1.0
sudo ./scripts/install.sh
```

O instalador cria:

- usuario/grupo dedicado: `databasa:databasa`
- unit systemd com restart automatico em falha e habilitado no boot
- helper CLI em `/usr/local/bin/databasa`

## Atualizacao

Para atualizar em servidor ja instalado, execute novamente o instalador.
Ele atualiza o binario e reinicia o servico, mantendo a config existente em `/etc/databasa/databasa.toml`.

Atualizar para a release mais recente (`latest`):

```bash
cd ~/databasa/scripts
sudo ./install.sh
```

Atualizar para uma tag especifica:

```bash
cd ~/databasa/scripts
sudo DATABASA_RELEASE_TAG=v0.1.1 ./install.sh
```

Validar depois da atualizacao:

```bash
databasa status
databasa logs --follow
```

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
