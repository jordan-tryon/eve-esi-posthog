#!/usr/bin/env bash
set -e

PID_FILE=".app.pid"

# Graceful stop
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "[restart] Stopping PID $PID..."
        kill -TERM "$PID"
        # Wait up to 5s for clean exit
        for i in $(seq 1 10); do
            kill -0 "$PID" 2>/dev/null || break
            sleep 0.5
        done
        # Force if still alive
        kill -0 "$PID" 2>/dev/null && kill -KILL "$PID" && echo "[restart] Force killed."
    fi
    rm -f "$PID_FILE"
fi

echo "[restart] Starting app..."
python app.py &
APP_PID=$!
echo "$APP_PID" > "$PID_FILE"

# Wait for it to bind
for i in $(seq 1 20); do
    sleep 0.5
    curl -sf http://localhost:8080/ -o /dev/null && break
done

echo "[restart] App running (PID $APP_PID) → http://localhost:8080"
