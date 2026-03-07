#!/bin/sh
# Wrapper to run the analysis and ping BetterStack heartbeat on success/failure.
# Reads env var BETTERSTACK_HEARTBEAT_URL or defaults to the provided URL.

set -u

URL="${BETTERSTACK_HEARTBEAT_URL:-https://uptime.betterstack.com/api/v1/heartbeat/QbwFD748cStzMqRGR6CPLcfV}"

echo "[entrypoint] Running analysis at $(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Run the Python analysis
python -m src.main
EXIT_CODE=$?

if [ "$EXIT_CODE" -eq 0 ]; then
  echo "[entrypoint] Analysis succeeded — sending heartbeat to $URL"
  curl -fsS --retry 3 --retry-delay 2 "$URL" || echo "[entrypoint] Warning: heartbeat POST failed"
else
  # Send explicit failure heartbeat — BetterStack accepts /fail or /<code>
  if [ "$EXIT_CODE" -eq 1 ]; then
    HB="$URL/fail"
  else
    HB="$URL/$EXIT_CODE"
  fi
  echo "[entrypoint] Analysis failed (exit=$EXIT_CODE) — sending heartbeat to $HB"
  curl -fsS --retry 3 --retry-delay 2 "$HB" || echo "[entrypoint] Warning: heartbeat POST failed"
fi

exit $EXIT_CODE
