#!/usr/bin/env bash
set -euo pipefail

# Heavy JMH run with sensible defaults and a soft timeout guard.
# Expected wall time: ~80–120s on typical laptops.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SBTCMD=(
  sbt -Dsbt.log.noformat=true \
  "jmh/jmh:run -i 5 -wi 5 -r 2s -w 2s -f1 -t1 -rf json -rff /tmp/jmh.json \
  io.toonformat.toon4s.jmh.EncodeDecodeBench.decode_tabular \
  io.toonformat.toon4s.jmh.EncodeDecodeBench.decode_list \
  io.toonformat.toon4s.jmh.EncodeDecodeBench.decode_nested \
  io.toonformat.toon4s.jmh.EncodeDecodeBench.encode_object"
)

# Soft timeout guard (~150s). Portable without GNU timeout.
(
  sleep 150
  echo "[jmh-heavy] Soft timeout reached; if still running, consider reducing benches or forks." >&2
) & GUARD=$!

"${SBTCMD[@]}" || RC=$?
kill $GUARD >/dev/null 2>&1 || true
exit ${RC:-0}
