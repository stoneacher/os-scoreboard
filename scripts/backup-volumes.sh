#!/usr/bin/env bash
set -euo pipefail

PROJECT_NAME="${PROJECT_NAME:-sweb-scoreboard-monitor}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BACKUP_ROOT="${BACKUP_ROOT:-$PROJECT_ROOT/backups}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BACKUP_DIR="${BACKUP_ROOT}/${PROJECT_NAME}-${TIMESTAMP}"
KEEP_RUNNING="${KEEP_RUNNING:-false}"
RETENTION_DAYS="${RETENTION_DAYS:-14}"

VOLUMES=(
  "${PROJECT_NAME}_influxdb-data"
  "${PROJECT_NAME}_influxdb-config"
  "${PROJECT_NAME}_grafana-data"
)

log() {
  printf '[backup] %s\n' "$*"
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'missing required command: %s\n' "$1" >&2
    exit 1
  fi
}

running_container_ids() {
  docker ps -q --filter "label=com.docker.compose.project=${PROJECT_NAME}"
}

stop_project_containers() {
  mapfile -t RUNNING_CONTAINERS < <(running_container_ids)
  if [ "${#RUNNING_CONTAINERS[@]}" -eq 0 ]; then
    log "no running containers for project ${PROJECT_NAME}"
    return
  fi

  log "stopping ${#RUNNING_CONTAINERS[@]} running project container(s)"
  docker stop "${RUNNING_CONTAINERS[@]}" >/dev/null
}

start_project_containers() {
  if [ "${#RUNNING_CONTAINERS[@]}" -eq 0 ]; then
    return
  fi

  log "starting previously running project container(s)"
  docker start "${RUNNING_CONTAINERS[@]}" >/dev/null
}

backup_volume() {
  local volume_name="$1"
  local archive_name="$2"

  if ! docker volume inspect "$volume_name" >/dev/null 2>&1; then
    printf 'missing expected Docker volume: %s\n' "$volume_name" >&2
    exit 1
  fi

  log "archiving volume ${volume_name}"
  docker run --rm \
    -v "${volume_name}:/source:ro" \
    -v "${BACKUP_DIR}:/backup" \
    alpine:3.20 \
    sh -c "cd /source && tar czf \"/backup/${archive_name}\" ."
}

write_manifest() {
  local manifest_path="${BACKUP_DIR}/manifest.txt"
  {
    echo "project=${PROJECT_NAME}"
    echo "timestamp=${TIMESTAMP}"
    echo "backup_dir=${BACKUP_DIR}"
    echo "keep_running=${KEEP_RUNNING}"
    echo
    echo "[volumes]"
    for volume_name in "${VOLUMES[@]}"; do
      echo "${volume_name}"
    done
    echo
    echo "[containers]"
    docker ps -a \
      --filter "label=com.docker.compose.project=${PROJECT_NAME}" \
      --format '{{.Names}} {{.Status}}'
  } >"${manifest_path}"
}

cleanup_old_backups() {
  local count
  count=$(find "${BACKUP_ROOT}" -maxdepth 1 -type d -name "${PROJECT_NAME}-*" -mtime "+${RETENTION_DAYS}" | wc -l | tr -d ' ')
  if [ "$count" -eq 0 ]; then
    log "no backups older than ${RETENTION_DAYS} days to clean up"
    return
  fi
  log "removing ${count} backup(s) older than ${RETENTION_DAYS} days"
  find "${BACKUP_ROOT}" -maxdepth 1 -type d -name "${PROJECT_NAME}-*" -mtime "+${RETENTION_DAYS}" -exec rm -rf {} +
}

main() {
  require_command docker
  require_command tar

  mkdir -p "${BACKUP_DIR}"

  RUNNING_CONTAINERS=()
  if [ "${KEEP_RUNNING}" != "true" ]; then
    stop_project_containers
    trap start_project_containers EXIT
  else
    log "KEEP_RUNNING=true, backing up live volumes"
  fi

  for volume_name in "${VOLUMES[@]}"; do
    backup_volume "${volume_name}" "${volume_name}.tar.gz"
  done

  if [ -f "${PROJECT_ROOT}/.env" ]; then
    log "copying .env"
    cp "${PROJECT_ROOT}/.env" "${BACKUP_DIR}/.env"
  else
    log "no .env found at ${PROJECT_ROOT}/.env"
  fi

  write_manifest
  log "backup completed at ${BACKUP_DIR}"
  cleanup_old_backups
}

main "$@"
