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
RELEASE_REPO="${DATABASA_RELEASE_REPO:-futlize/databasa}"
RELEASE_TAG="${DATABASA_RELEASE_TAG:-latest}"
RELEASE_ASSET="${DATABASA_RELEASE_ASSET:-}"
BIN_URL="${DATABASA_BIN_URL:-}"

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
CERT_DIR="${CONF_DIR}/certs"
DEFAULT_CERT_FILE="${CERT_DIR}/server.crt"
DEFAULT_KEY_FILE="${CERT_DIR}/server.key"

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
TMP_DIR=""

cleanup() {
  if [[ -n "${TMP_BIN}" && -f "${TMP_BIN}" ]]; then
    rm -f "${TMP_BIN}"
  fi
  if [[ -n "${TMP_DIR}" && -d "${TMP_DIR}" ]]; then
    rm -rf "${TMP_DIR}"
  fi
}
trap cleanup EXIT

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

ensure_tmp_bin() {
  if [[ -z "${TMP_BIN}" ]]; then
    TMP_BIN="$(mktemp "/tmp/${BIN_NAME}.XXXXXX")"
  fi
}

ensure_tmp_dir() {
  if [[ -z "${TMP_DIR}" ]]; then
    TMP_DIR="$(mktemp -d "/tmp/${BIN_NAME}.XXXXXX")"
  fi
}

download_file() {
  local url="$1"
  local output="$2"
  if have_cmd curl; then
    curl -fsSL "${url}" -o "${output}" >/dev/null 2>&1
    return $?
  fi
  if have_cmd wget; then
    wget -qO "${output}" "${url}" >/dev/null 2>&1
    return $?
  fi
  return 1
}

download_binary_from_url() {
  local url="$1"
  local archive=""
  local extract_dir=""
  local extracted_bin=""

  if [[ "${url}" == *.tar.gz || "${url}" == *.tgz ]]; then
    ensure_tmp_dir
    archive="${TMP_DIR}/asset.tgz"
    extract_dir="${TMP_DIR}/extract"
    rm -f "${archive}"
    rm -rf "${extract_dir}"
    mkdir -p "${extract_dir}"
    if ! download_file "${url}" "${archive}"; then
      return 1
    fi
    if ! tar -xzf "${archive}" -C "${extract_dir}" >/dev/null 2>&1; then
      return 1
    fi
    extracted_bin="$(find "${extract_dir}" -type f -name "${BIN_NAME}" 2>/dev/null | head -n1 || true)"
    if [[ -z "${extracted_bin}" ]]; then
      return 1
    fi
    ensure_tmp_bin
    install -m 0755 "${extracted_bin}" "${TMP_BIN}"
  else
    ensure_tmp_bin
    if ! download_file "${url}" "${TMP_BIN}"; then
      return 1
    fi
    chmod 0755 "${TMP_BIN}"
  fi

  BIN_TO_INSTALL="${TMP_BIN}"
  return 0
}

linux_arch() {
  local machine
  machine="$(uname -m)"
  case "${machine}" in
    x86_64|amd64) echo "amd64" ;;
    aarch64|arm64) echo "arm64" ;;
    *) echo "" ;;
  esac
}

download_release_binary() {
  local arch
  local base_url
  local assets=()
  local asset

  if ! have_cmd curl && ! have_cmd wget; then
    return 1
  fi

  arch="$(linux_arch)"
  if [[ -z "${arch}" ]]; then
    return 1
  fi

  if [[ "${RELEASE_TAG}" == "latest" ]]; then
    base_url="https://github.com/${RELEASE_REPO}/releases/latest/download"
  else
    base_url="https://github.com/${RELEASE_REPO}/releases/download/${RELEASE_TAG}"
  fi

  if [[ -n "${RELEASE_ASSET}" ]]; then
    assets=("${RELEASE_ASSET}")
  else
    assets=(
      "${BIN_NAME}_linux_${arch}"
      "${BIN_NAME}-linux-${arch}"
      "${BIN_NAME}_${arch}"
      "${BIN_NAME}_linux_${arch}.tar.gz"
      "${BIN_NAME}-linux-${arch}.tar.gz"
      "${BIN_NAME}_${arch}.tar.gz"
      "${BIN_NAME}_linux_${arch}.tgz"
      "${BIN_NAME}-linux-${arch}.tgz"
      "${BIN_NAME}_${arch}.tgz"
    )
  fi

  for asset in "${assets[@]}"; do
    if download_binary_from_url "${base_url}/${asset}"; then
      echo "Downloaded release asset: ${asset}"
      return 0
    fi
  done

  return 1
}

build_binary_with_go() {
  if ! have_cmd go; then
    return 1
  fi
  ensure_tmp_bin
  (cd "${REPO_ROOT}" && go build -o "${TMP_BIN}" ./cmd/databasa)
  BIN_TO_INSTALL="${TMP_BIN}"
  return 0
}

trim_toml_value() {
  local raw="$1"
  raw="$(printf "%s" "${raw}" | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//')"
  if [[ "${raw}" =~ ^\".*\"$ || "${raw}" =~ ^\'.*\'$ ]]; then
    raw="${raw:1:${#raw}-2}"
  fi
  printf "%s" "${raw}"
}

read_toml_key() {
  local file="$1"
  local key="$2"
  local value
  value="$(sed -nE "s|^[[:space:]]*${key}[[:space:]]*=[[:space:]]*([^#;]+).*$|\\1|p" "${file}" | head -n1 || true)"
  trim_toml_value "${value}"
}

toml_quote() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  printf "\"%s\"" "${value}"
}

upsert_toml_key() {
  local file="$1"
  local section="$2"
  local key="$3"
  local value="$4"
  local tmp="${file}.tmp.$$"

  awk -v section="${section}" -v key="${key}" -v value="${value}" '
    BEGIN {
      in_section = 0
      section_seen = 0
      key_written = 0
    }
    function emit_key() {
      print key " = " value
    }
    {
      if ($0 ~ "^[[:space:]]*\\[" section "\\][[:space:]]*$") {
        section_seen = 1
        in_section = 1
        print $0
        next
      }
      if (in_section && $0 ~ "^[[:space:]]*\\[.*\\][[:space:]]*$") {
        if (!key_written) {
          emit_key()
          key_written = 1
        }
        in_section = 0
        print $0
        next
      }
      if (in_section && $0 ~ "^[[:space:]]*" key "[[:space:]]*=") {
        if (!key_written) {
          emit_key()
          key_written = 1
        }
        next
      }
      print $0
    }
    END {
      if (!section_seen) {
        print ""
        print "[" section "]"
        emit_key()
      } else if (in_section && !key_written) {
        emit_key()
      }
    }
  ' "${file}" > "${tmp}"

  mv "${tmp}" "${file}"
}

resolve_config_relative_path() {
  local value="$1"
  if [[ -z "${value}" ]]; then
    printf "%s" ""
    return
  fi
  if [[ "${value}" = /* ]]; then
    printf "%s" "${value}"
    return
  fi
  value="${value#./}"
  printf "%s" "${CONF_DIR}/${value}"
}

if [[ -n "${BIN_URL}" ]]; then
  if download_binary_from_url "${BIN_URL}"; then
    echo "Downloaded binary from DATABASA_BIN_URL."
  else
    echo "Warning: failed to download DATABASA_BIN_URL=${BIN_URL}" >&2
  fi
fi

if [[ -z "${BIN_TO_INSTALL}" ]]; then
  if ! download_release_binary; then
    echo "Warning: failed to download GitHub release asset from ${RELEASE_REPO} (${RELEASE_TAG})." >&2
  fi
fi

if [[ -z "${BIN_TO_INSTALL}" && -x "${SOURCE_BIN}" ]]; then
  BIN_TO_INSTALL="${SOURCE_BIN}"
  echo "Using local fallback binary: ${SOURCE_BIN}"
fi

if [[ -z "${BIN_TO_INSTALL}" ]]; then
  if build_binary_with_go; then
    echo "Built fallback binary with local Go toolchain."
  fi
fi

if [[ -z "${BIN_TO_INSTALL}" ]]; then
  echo "Unable to resolve ${BIN_NAME} binary." >&2
  echo "Tried:" >&2
  echo "  1) DATABASA_BIN_URL (if provided)" >&2
  echo "  2) GitHub release asset from ${RELEASE_REPO} (${RELEASE_TAG})" >&2
  echo "  3) Local executable at ${SOURCE_BIN}" >&2
  echo "  4) Local Go toolchain build" >&2
  echo "Provide one of:" >&2
  echo "  - ./bin/${BIN_NAME} executable, or" >&2
  echo "  - DATABASA_BIN_URL with a direct binary or .tar.gz/.tgz archive URL." >&2
  exit 1
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
install -d -m 0750 "${CONF_DIR}" "${CERT_DIR}" "${DATA_ROOT}" "${DATA_DIR}" "${LOG_DIR}"
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

configured_cert="$(read_toml_key "${CONF_FILE}" "tls_cert_file")"
configured_key="$(read_toml_key "${CONF_FILE}" "tls_key_file")"
configured_client_auth="$(read_toml_key "${CONF_FILE}" "tls_client_auth")"
configured_api_key_header="$(read_toml_key "${CONF_FILE}" "api_key_header")"

cert_target="${configured_cert}"
key_target="${configured_key}"
if [[ -z "${cert_target}" ]]; then
  cert_target="${DEFAULT_CERT_FILE}"
fi
if [[ -z "${key_target}" ]]; then
  key_target="${DEFAULT_KEY_FILE}"
fi

active_cert_file="$(resolve_config_relative_path "${cert_target}")"
active_key_file="$(resolve_config_relative_path "${key_target}")"
if [[ -z "${active_cert_file}" || -z "${active_key_file}" ]]; then
  echo "Unable to resolve TLS cert/key paths from config." >&2
  exit 1
fi

cert_exists=0
key_exists=0
if [[ -f "${active_cert_file}" ]]; then
  cert_exists=1
fi
if [[ -f "${active_key_file}" ]]; then
  key_exists=1
fi

generated_cert=0
if [[ "${cert_exists}" -eq 1 && "${key_exists}" -eq 1 ]]; then
  echo "Keeping existing TLS certificate/key:"
  echo "  cert: ${active_cert_file}"
  echo "  key:  ${active_key_file}"
elif [[ "${cert_exists}" -eq 1 || "${key_exists}" -eq 1 ]]; then
  echo "Refusing to continue: only one of TLS cert/key exists." >&2
  echo "cert exists=${cert_exists}, key exists=${key_exists}" >&2
  echo "Fix files or remove both to let installer regenerate them." >&2
  exit 1
else
  echo "Generating default self-signed TLS certificate..."
  "${INSTALL_BIN}" cert generate -config "${CONF_FILE}" -cert-file "${active_cert_file}" -key-file "${active_key_file}"
  generated_cert=1
fi

if [[ -z "${configured_api_key_header}" ]]; then
  configured_api_key_header="authorization"
fi
if [[ -z "${configured_client_auth}" ]]; then
  configured_client_auth="none"
fi

upsert_toml_key "${CONF_FILE}" "security" "auth_enabled" "true"
upsert_toml_key "${CONF_FILE}" "security" "require_auth" "true"
upsert_toml_key "${CONF_FILE}" "security" "api_key_header" "$(toml_quote "${configured_api_key_header}")"
upsert_toml_key "${CONF_FILE}" "security" "tls_enabled" "true"
upsert_toml_key "${CONF_FILE}" "security" "tls_cert_file" "$(toml_quote "${active_cert_file}")"
upsert_toml_key "${CONF_FILE}" "security" "tls_key_file" "$(toml_quote "${active_key_file}")"
upsert_toml_key "${CONF_FILE}" "security" "tls_client_auth" "$(toml_quote "${configured_client_auth}")"

if [[ "${generated_cert}" -eq 1 ]]; then
  install -d -m 0750 "$(dirname "${active_cert_file}")" "$(dirname "${active_key_file}")"
  chown root:"${SERVICE_GROUP}" "$(dirname "${active_cert_file}")" "$(dirname "${active_key_file}")" || true
  chown root:"${SERVICE_GROUP}" "${active_cert_file}"
  chmod 0644 "${active_cert_file}"
  chown "${SERVICE_USER}:${SERVICE_GROUP}" "${active_key_file}"
  chmod 0600 "${active_key_file}"
fi

if [[ ! -f "${ENV_FILE}" ]]; then
  cat > "${ENV_FILE}" <<EOF
# Databasa service environment
DATABASA_CONFIG=${CONF_FILE}
EOF
fi

chown root:"${SERVICE_GROUP}" "${CONF_DIR}" "${CERT_DIR}" "${CONF_FILE}" "${ENV_FILE}"
chmod 0750 "${CONF_DIR}"
chmod 0750 "${CERT_DIR}"
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
echo "TLS cert: ${active_cert_file}"
echo "TLS key: ${active_key_file}"
echo "TLS mode: enabled by default (security.tls_enabled=true)"
echo "To replace with CA-signed certs:"
echo "  1) overwrite ${active_cert_file} and ${active_key_file} with your issued cert/key"
echo "  2) keep key permissions restricted to ${SERVICE_USER} (chmod 600)"
echo "  3) restart service: ${DB_NAME} restart"
echo "To regenerate self-signed certs:"
echo "  ${INSTALL_BIN} cert generate -config ${CONF_FILE} -cert-file ${active_cert_file} -key-file ${active_key_file} -force"
