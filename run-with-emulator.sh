#!/usr/bin/env bash
#
# Run an arbitrary command with the Firestore emulator running in the
# background. The emulator is started on an ephemeral port, exposed to the
# command via FIRESTORE_EMULATOR_HOST, and torn down on exit (pass, fail, or
# interrupt).
#
# Usage:
#   ./run-with-emulator.sh <command> [args...]
#
# Example:
#   ./run-with-emulator.sh go test -v -count=1 ./...
#   ./run-with-emulator.sh ./scripts/seed-dev-data
#
# Requires:
#   - gcloud with the cloud-firestore-emulator component installed
#     (`gcloud components install cloud-firestore-emulator`)
#   - nc (netcat) to probe port readiness

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <command> [args...]" >&2
    exit 64
fi

# Ephemeral range, randomized so parallel invocations don't collide.
PORT=$(awk 'BEGIN{srand(); print 49152 + int(rand()*16000)}')
LOG=$(mktemp -t firestore-emulator.XXXXXX)

echo "Starting Firestore emulator on localhost:${PORT} (log: ${LOG})..."
gcloud emulators firestore start --host-port="localhost:${PORT}" >"${LOG}" 2>&1 &
EMU_PID=$!

cleanup() {
    if kill -0 "${EMU_PID}" 2>/dev/null; then
        echo "Stopping Firestore emulator (pid=${EMU_PID})..."
        kill "${EMU_PID}" 2>/dev/null || true
        wait "${EMU_PID}" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# Wait up to 60s for the emulator to bind the port. If it dies during startup
# (missing component, port in use, etc.) surface the log and bail.
for _ in $(seq 1 60); do
    if nc -z localhost "${PORT}" 2>/dev/null; then
        break
    fi
    if ! kill -0 "${EMU_PID}" 2>/dev/null; then
        echo "Emulator exited before binding port. Log:"
        cat "${LOG}"
        exit 1
    fi
    sleep 1
done

if ! nc -z localhost "${PORT}" 2>/dev/null; then
    echo "Emulator failed to bind port ${PORT} within 60s. Log:"
    cat "${LOG}"
    exit 1
fi

echo "Emulator ready. Running: $*"
FIRESTORE_EMULATOR_HOST="localhost:${PORT}" "$@"
