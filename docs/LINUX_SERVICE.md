# Databasa Linux Service (Ubuntu/systemd)

This guide documents the installation and operation of Databasa as a systemd service on Ubuntu/Linux.

## Official scripts

- `scripts/install.sh`
- `scripts/uninstall.sh`

## Installation

Exact commands:

```bash
sudo ./scripts/install.sh
```

`scripts/install.sh` resolves the binary in this order:

1. `./bin/databasa` (when it already exists in the repo)
2. `DATABASA_BIN_URL` (direct URL for a binary or `.tar.gz`/`.tgz`)
3. Precompiled GitHub Release (`latest` by default)
4. Local build with Go (fallback)

Optional variables to control download:

- `DATABASA_BIN_URL`: explicit binary/file URL.
- `DATABASA_RELEASE_REPO`: repo for release download (default: `futlize/databasa`).
- `DATABASA_RELEASE_TAG`: release tag (default: `latest`).
- `DATABASA_RELEASE_ASSET`: exact asset name (when you want to pin a specific artifact).

Example without Go, pinning a tag:

```bash
export DATABASA_RELEASE_TAG=v0.1.0
sudo ./scripts/install.sh
```

The installer creates:

- dedicated user/group: `databasa:databasa`
- systemd unit with automatic restart on failure and enabled at boot
- CLI helper at `/usr/local/bin/databasa`

## Update

To update an already installed server, run the installer again.
It updates the binary and restarts the service, keeping the existing config at `/etc/databasa/databasa.toml`.

Update to the latest release (`latest`):

```bash
cd ~/databasa/scripts
sudo ./install.sh
```

Update to a specific tag:

```bash
cd ~/databasa/scripts
sudo DATABASA_RELEASE_TAG=v0.1.1 ./install.sh
```

Validate after the update:

```bash
databasa status
databasa logs --follow
```

## FHS Layout

- Config: `/etc/databasa/` (main file: `/etc/databasa/databasa.toml`)
- Env file: `/etc/default/databasa` (`DATABASA_CONFIG`)
- Data: `/var/lib/databasa/` (data in `/var/lib/databasa/data`)
- Logs dir: `/var/log/databasa/`
- Runtime dir: `/run/databasa/`
- Runtime logs: `journald` (`journalctl -u databasa`) under the hood

## Operation via CLI helper

`<dbname>` in this repo is `databasa`, therefore:

```bash
databasa status          # <dbname> status
databasa start           # <dbname> start
databasa stop            # <dbname> stop
databasa restart         # <dbname> restart
databasa logs --follow   # <dbname> logs --follow
databasa --cli           # open interactive DB shell
databasa cli             # same as --cli
```

Important:
- `/usr/local/bin/databasa` installed by `scripts/install.sh` proxies service controls and `--cli`/`cli` to the real binary at `/usr/local/lib/databasa/databasa`.
- Installer adds the invoking user to group `databasa` to allow `databasa --cli` without sudo.
- After install/update, start a new shell session (or run `newgrp databasa`) once.
- CLI connection target is loopback-only (`127.0.0.1`, `localhost`, `[::1]`).
- CLI uses `127.0.0.1:50051` by default. For non-default server port, use `--port`.
- For certificate operations, use the real binary directly:
  `/usr/local/lib/databasa/databasa cert ... -config /etc/databasa/databasa.toml`

## Uninstallation

Remove service and binaries (keeps config/data/log dir):

```bash
sudo ./scripts/uninstall.sh
```

Complete removal (includes config/data/log dir and dedicated user/group):

```bash
sudo ./scripts/uninstall.sh --purge
```
