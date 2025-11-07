#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[smoke] Building staged CLI..."
sbt --error "cli/stage" >/dev/null

CLI_BIN="$ROOT_DIR/cli/target/universal/stage/bin/toon4s-cli"
if [[ ! -x "$CLI_BIN" ]]; then
  echo "[smoke] CLI binary not found at $CLI_BIN" >&2
  exit 1
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

cat >"$TMP_DIR/sample.json" <<'JSON'
{
  "users": [
    {
      "id": 1,
      "name": "Ada",
      "tags": ["reading", "gaming"],
      "active": true,
      "prefs": []
    },
    {
      "id": 2,
      "name": "Bob",
      "tags": ["writing"],
      "active": false,
      "prefs": [
        {"kind": "newsletter", "frequency": "weekly"}
      ]
    }
  ]
}
JSON

ENCODED="$TMP_DIR/sample.toon"
ROUNDTRIP="$TMP_DIR/roundtrip.json"

echo "[smoke] Encoding JSON -> TOON"
"$CLI_BIN" --encode --indent 2 "$TMP_DIR/sample.json" -o "$ENCODED"

echo "[smoke] Decoding TOON -> JSON"
"$CLI_BIN" --decode "$ENCODED" -o "$ROUNDTRIP"

python3 - "$TMP_DIR" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
orig = json.loads(root.joinpath('sample.json').read_text())
roundtrip = json.loads(root.joinpath('roundtrip.json').read_text())
if orig != roundtrip:
    raise SystemExit('Round-trip mismatch between original JSON and decoded output')
print('Round-trip JSON ✅')
PY

echo "[smoke] Output snapshot:"
cat "$ENCODED"
echo

echo "[smoke] Smoke test completed successfully."
