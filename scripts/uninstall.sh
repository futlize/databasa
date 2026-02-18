#!/usr/bin/env bash
set -euo pipefail

DB_NAME="${DB_NAME:-databasa}"
SERVICE_NAME="${DB_NAME}"
SERVICE_USER="${DB_NAME}"
SERVICE_GROUP="${DB_NAME}"
BIN_NAME="${DB_NAME}"
PATH="/usr/sbin:/usr/bin:/sbin:/bin"

if [[ ! "${DB_NAME}" =~ ^[a-z][a-z0-9_-]*$ ]]; then
  echo "Invalid DB_NAME=${DB_NAME}. Allowed: ^[a-z][a-z0-9_-]*$" >&2
  exit 1
fi

SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
INSTALL_ROOT="/usr/local/lib/${DB_NAME}"
INSTALL_BIN="${INSTALL_ROOT}/${BIN_NAME}"
HELPER_BIN="/usr/local/bin/${DB_NAME}"
ENV_FILE="/etc/default/${DB_NAME}"
CONF_DIR="/etc/${DB_NAME}"
DATA_DIR="/var/lib/${DB_NAME}"
LOG_DIR="/var/log/${DB_NAME}"
RUN_DIR="/run/${DB_NAME}"

usage() {
  cat <<USAGE
Usage: ./scripts/uninstall.sh [--purge]

Options:
  --purge    Also remove /etc/${DB_NAME}, /etc/default/${DB_NAME}, /var/lib/${DB_NAME}, /var/log/${DB_NAME},
             and delete ${SERVICE_USER}:${SERVICE_GROUP} if present.
USAGE
}

PURGE=false
if [[ "${1:-}" == "--purge" ]]; then
  PURGE=true
  shift
fi

if [[ "${#}" -gt 0 ]]; then
  usage
  exit 1
fi

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run as root. Example: sudo ./scripts/uninstall.sh" >&2
  exit 1
fi

if command -v systemctl >/dev/null 2>&1; then
  systemctl disable --now "${SERVICE_NAME}" >/dev/null 2>&1 || true
fi

rm -f "${SERVICE_FILE}" "${INSTALL_BIN}" "${HELPER_BIN}" "${ENV_FILE}"
rmdir "${INSTALL_ROOT}" >/dev/null 2>&1 || true

if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload
  systemctl reset-failed "${SERVICE_NAME}" >/dev/null 2>&1 || true
fi

if [[ "${PURGE}" == "true" ]]; then
  if [[ "${CONF_DIR}" == "/" || "${DATA_DIR}" == "/" || "${LOG_DIR}" == "/" || "${RUN_DIR}" == "/" ]]; then
    echo "Refusing purge due to unsafe path resolution." >&2
    exit 1
  fi
  if [[ -L "${CONF_DIR}" || -L "${DATA_DIR}" || -L "${LOG_DIR}" || -L "${RUN_DIR}" ]]; then
    echo "Refusing purge: one of target directories is a symlink." >&2
    exit 1
  fi
  rm -rf "${CONF_DIR}" "${DATA_DIR}" "${LOG_DIR}" "${RUN_DIR}"
  userdel "${SERVICE_USER}" >/dev/null 2>&1 || true
  groupdel "${SERVICE_GROUP}" >/dev/null 2>&1 || true
  echo "Uninstalled ${DB_NAME} and purged config/data/log/runtime directories."
else
  echo "Uninstalled ${DB_NAME} service and binaries."
  echo "Kept data in ${DATA_DIR} and config in ${CONF_DIR}."
  echo "Use --purge to remove config/data/log directories and service user/group."
fi
