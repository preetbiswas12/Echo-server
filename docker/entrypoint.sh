#!/bin/sh
set -eu

# Render sets PORT; Conduit reads CONDUIT_PORT/config.port.
export CONDUIT_ADDRESS="${CONDUIT_ADDRESS:-0.0.0.0}"
export CONDUIT_PORT="${CONDUIT_PORT:-${PORT:-6167}}"

# CONDUIT_CONFIG is optional (empty/unset = env/defaults). Provide a path to a TOML file to override.
if [ -n "${CONDUIT_CONFIG:-}" ] && [ ! -f "${CONDUIT_CONFIG}" ]; then
  echo "CONDUIT_CONFIG is set to '${CONDUIT_CONFIG}' but the file does not exist" >&2
  exit 1
fi

exec /usr/local/bin/conduit "$@"
