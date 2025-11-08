# Smoke tests

This directory contains a lightweight round-trip script that exercises the staged CLI binary
against a moderately structured JSON payload. The script verifies that encoding to TOON and
then decoding back to JSON preserves the original data.

## Running

```bash
./smoke-tests/run-smoke.sh
```

The script will:

1. Build the staged CLI via `sbt cli/stage` (cached on subsequent runs).
2. Encode a representative JSON document to TOON.
3. Decode the TOON output back to JSON.
4. Compare the decoded JSON with the original using Python's standard library.
5. Print a snippet of the generated TOON for inspection.

The check fails fast on any mismatch and cleans up temporary files automatically.
