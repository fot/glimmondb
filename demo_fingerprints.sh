#!/usr/bin/env bash
set -euo pipefail

# Demonstrate deterministic database fingerprints persisted in build_fingerprints.
# Requires GLIMMONDATA (or SKA_DATA) to point at the glimmon archive and allows glimmondb.py to run.

CUSTOM_DIR="${1:-}"
echo "==> Ensuring environment is set (GLIMMONDATA or SKA_DATA) or custom dir provided..."
if [[ -n "${CUSTOM_DIR}" ]]; then
  GLIMMON_DIR="${CUSTOM_DIR}"
elif [[ -n "${GLIMMONDATA:-}" ]]; then
  GLIMMON_DIR="${GLIMMONDATA}"
elif [[ -n "${SKA_DATA:-}" ]]; then
  GLIMMON_DIR="${SKA_DATA}/glimmon_archive"
else
  echo "ERROR: Provide a directory argument or set GLIMMONDATA (or SKA_DATA)." >&2
  exit 1
fi
GLIMMON_DIR="$(cd "${GLIMMON_DIR}" && pwd)"

DB_PATH="${GLIMMON_DIR}/glimmondb.sqlite3"

if [[ ! -f "${DB_PATH}" ]]; then
  echo "ERROR: Expected database at ${DB_PATH} not found. Create or recreate it first." >&2
  exit 1
fi

echo "==> Latest fingerprint (compact for ~110-char terminal):"
sqlite3 "${DB_PATH}" <<'SQL'
.headers on
.mode column
.width 8 16 16 16 12 12 8 8 8 20
SELECT version,
       substr(limit_hash,1,14) || '..' AS limit_hash,
       substr(state_hash,1,14) || '..' AS state_hash,
       substr(version_hash,1,14) || '..' AS version_hash,
       substr(db_sha256,1,12) || '..' AS db_sha,
       db_size_bytes,
       limit_count,
       state_count,
        version_count,
        created_at
FROM build_fingerprints
ORDER BY version DESC
LIMIT 1;
SQL

echo "==> Latest version record:"
sqlite3 "${DB_PATH}" <<'SQL'
.headers on
.mode column
SELECT version, datesec, date FROM versions ORDER BY version DESC LIMIT 1;
SQL

LOG_PATH="${GLIMMON_DIR}/DB_Commit.log"
if [[ -f "${LOG_PATH}" ]]; then
  echo "==> Recent fingerprint log entries (last few lines):"
  rg -i "fingerprint|hash" -n "${LOG_PATH}" | tail -n 5 || true
else
  echo "Log file not found at ${LOG_PATH}; skipping log tail."
fi

echo "==> Done. Above hashes/counts should remain identical across recreations if inputs are unchanged."
