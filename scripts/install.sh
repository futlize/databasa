#!/usr/bin/env bash
set -euo pipefail

DB_NAME="${DB_NAME:-databasa}"
SERVICE_NAME="${DB_NAME}"
SERVICE_USER="${DB_NAME}"
SERVICE_GROUP="${DB_NAME}"
BIN_NAME="${DB_NAME}"
PATH="/usr/sbin:/usr/bin:/sbin:/bin"
umask 022

if [[ ! "${DB_NAME}" =~ ^[a-z][a-z0-9_-]*$ ]]; then
  echo "Invalid DB_NAME=${DB_NAME}. Allowed: ^[a-z][a-z0-9_-]*$" >&2
  exit 1
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

SOURCE_BIN="${REPO_ROOT}/bin/${BIN_NAME}"
SOURCE_CONF="${REPO_ROOT}/databasa.toml"

INSTALL_ROOT="/usr/local/lib/${DB_NAME}"
INSTALL_BIN="${INSTALL_ROOT}/${BIN_NAME}"
HELPER_BIN="/usr/local/bin/${DB_NAME}"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
CONF_DIR="/etc/${DB_NAME}"
CONF_FILE="${CONF_DIR}/${DB_NAME}.toml"
ENV_FILE="/etc/default/${DB_NAME}"
DATA_ROOT="/var/lib/${DB_NAME}"
DATA_DIR="${DATA_ROOT}/data"
LOG_DIR="/var/log/${DB_NAME}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run as root. Example: sudo ./scripts/install.sh" >&2
  exit 1
fi

if ! command -v systemctl >/dev/null 2>&1; then
  echo "systemd is required (systemctl not found)." >&2
  exit 1
fi

if [[ ! -f "${SOURCE_CONF}" ]]; then
  echo "Missing config file: ${SOURCE_CONF}" >&2
  exit 1
fi

BIN_TO_INSTALL=""
TMP_BIN=""

cleanup() {
  if [[ -n "${TMP_BIN}" && -f "${TMP_BIN}" ]]; then
    rm -f "${TMP_BIN}"
  fi
}
trap cleanup EXIT

if [[ -x "${SOURCE_BIN}" ]]; then
  BIN_TO_INSTALL="${SOURCE_BIN}"
else
  if ! command -v go >/dev/null 2>&1; then
    echo "Binary not found at ${SOURCE_BIN} and Go toolchain is unavailable." >&2
    echo "Build it first with: go build -o ./bin/${BIN_NAME} ./cmd/databasa" >&2
    exit 1
  fi
  TMP_BIN="$(mktemp "/tmp/${BIN_NAME}.XXXXXX")"
  (cd "${REPO_ROOT}" && go build -o "${TMP_BIN}" ./cmd/databasa)
  BIN_TO_INSTALL="${TMP_BIN}"
fi

if ! getent group "${SERVICE_GROUP}" >/dev/null 2>&1; then
  groupadd --system "${SERVICE_GROUP}"
fi

if ! id -u "${SERVICE_USER}" >/dev/null 2>&1; then
  useradd \
    --system \
    --gid "${SERVICE_GROUP}" \
    --home-dir /nonexistent \
    --no-create-home \
    --shell /usr/sbin/nologin \
    "${SERVICE_USER}"
fi

install -d -m 0755 /usr/local/bin "${INSTALL_ROOT}"
install -m 0755 "${BIN_TO_INSTALL}" "${INSTALL_BIN}"

if [[ -L "${CONF_DIR}" || -L "${DATA_ROOT}" || -L "${LOG_DIR}" || -L "${INSTALL_ROOT}" ]]; then
  echo "Refusing to continue: one of ${CONF_DIR}, ${DATA_ROOT}, ${LOG_DIR}, or ${INSTALL_ROOT} is a symlink." >&2
  exit 1
fi
if [[ -e "${SERVICE_FILE}" && -L "${SERVICE_FILE}" ]]; then
  echo "Refusing to use symlinked service file: ${SERVICE_FILE}" >&2
  exit 1
fi
if [[ -e "${ENV_FILE}" && -L "${ENV_FILE}" ]]; then
  echo "Refusing to use symlinked environment file: ${ENV_FILE}" >&2
  exit 1
fi
install -d -m 0750 "${CONF_DIR}" "${DATA_ROOT}" "${DATA_DIR}" "${LOG_DIR}"
if [[ -e "${CONF_FILE}" && -L "${CONF_FILE}" ]]; then
  echo "Refusing to use symlinked config file: ${CONF_FILE}" >&2
  exit 1
fi
if [[ -e "${CONF_FILE}" && ! -f "${CONF_FILE}" ]]; then
  echo "Refusing to use non-regular config file: ${CONF_FILE}" >&2
  exit 1
fi
if [[ ! -f "${CONF_FILE}" ]]; then
  install -m 0644 "${SOURCE_CONF}" "${CONF_FILE}"
  if grep -Eq '^[[:space:]]*data_dir[[:space:]]*=' "${CONF_FILE}"; then
    sed -i -E "s|^[[:space:]]*data_dir[[:space:]]*=.*$|data_dir = ${DATA_DIR}|" "${CONF_FILE}"
  fi
else
  echo "Keeping existing config: ${CONF_FILE}"
  if grep -Eq '^[[:space:]]*data_dir[[:space:]]*=' "${CONF_FILE}"; then
    current_data_dir="$(sed -nE 's|^[[:space:]]*data_dir[[:space:]]*=[[:space:]]*([^#;]+).*$|\1|p' "${CONF_FILE}" | head -n1 | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//')"
    if [[ -n "${current_data_dir}" && "${current_data_dir}" != "${DATA_DIR}" ]]; then
      echo "Warning: config data_dir is '${current_data_dir}'. Expected '${DATA_DIR}' for FHS layout." >&2
    fi
  fi
fi

if [[ ! -f "${ENV_FILE}" ]]; then
  cat > "${ENV_FILE}" <<EOF
# Databasa service environment
DATABASA_CONFIG=${CONF_FILE}
EOF
fi

chown root:"${SERVICE_GROUP}" "${CONF_DIR}" "${CONF_FILE}" "${ENV_FILE}"
chmod 0750 "${CONF_DIR}"
chmod 0640 "${CONF_FILE}"
chmod 0640 "${ENV_FILE}"

chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "${DATA_ROOT}" "${LOG_DIR}"
chmod 0750 "${DATA_ROOT}" "${DATA_DIR}" "${LOG_DIR}"

cat > "${SERVICE_FILE}" <<EOF
[Unit]
Description=Databasa gRPC Vector Database
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_GROUP}
Environment="DATABASA_CONFIG=${CONF_FILE}"
EnvironmentFile=-${ENV_FILE}
WorkingDirectory=${DATA_ROOT}
RuntimeDirectory=${DB_NAME}
RuntimeDirectoryMode=0755
ExecStart=${INSTALL_BIN} -config \${DATABASA_CONFIG}
Restart=on-failure
RestartSec=3s
SyslogIdentifier=${SERVICE_NAME}
UMask=0027

NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=${DATA_ROOT} ${LOG_DIR} /run/${DB_NAME}

[Install]
WantedBy=multi-user.target
EOF

cat > "${HELPER_BIN}" <<EOF
#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="${SERVICE_NAME}"

run_root_cmd() {
  if [[ "\${EUID}" -eq 0 ]]; then
    "\$@"
    return
  fi
  if command -v sudo >/dev/null 2>&1; then
    sudo "\$@"
    return
  fi
  echo "Root privileges are required for this command." >&2
  exit 1
}

usage() {
  cat <<USAGE
Usage: ${DB_NAME} <command>

Commands:
  status
  start
  stop
  restart
  logs [--follow]
USAGE
}

cmd="\${1:-}"
case "\${cmd}" in
  status)
    run_root_cmd systemctl status "\${SERVICE_NAME}" --no-pager
    ;;
  start|stop|restart)
    run_root_cmd systemctl "\${cmd}" "\${SERVICE_NAME}"
    ;;
  logs)
    shift || true
    follow=""
    if [[ "\${1:-}" == "--follow" ]]; then
      follow="-f"
      shift
    fi
    if [[ "\${#}" -gt 0 ]]; then
      usage
      exit 1
    fi
    if [[ -n "\${follow}" ]]; then
      run_root_cmd journalctl -u "\${SERVICE_NAME}" -f -n 200
    else
      run_root_cmd journalctl -u "\${SERVICE_NAME}" -n 200 --no-pager
    fi
    ;;
  *)
    usage
    exit 1
    ;;
esac
EOF

chmod 0644 "${SERVICE_FILE}"
chmod 0755 "${HELPER_BIN}"

systemctl daemon-reload
systemctl enable "${SERVICE_NAME}" >/dev/null
if systemctl is-active --quiet "${SERVICE_NAME}"; then
  systemctl restart "${SERVICE_NAME}"
else
  systemctl start "${SERVICE_NAME}"
fi

echo "Installed ${DB_NAME}."
echo "Service: ${SERVICE_NAME}"
echo "Config: ${CONF_FILE}"
echo "Environment file: ${ENV_FILE}"
echo "Data: ${DATA_DIR}"
echo "Runtime dir: /run/${DB_NAME}"
echo "Logs directory: ${LOG_DIR} (runtime logs are in journald)"
echo "Helper: ${HELPER_BIN}"
