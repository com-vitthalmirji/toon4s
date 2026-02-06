#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SPEC_REPO_URL="${SPEC_REPO_URL:-https://github.com/toon-format/spec.git}"
SPEC_REF="${SPEC_REF:-v3.0.1}"
TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

echo "Checking conformance fixture sync against ${SPEC_REPO_URL}@${SPEC_REF}"
git clone --quiet "$SPEC_REPO_URL" "$TMP_DIR/spec"
git -C "$TMP_DIR/spec" checkout --quiet "$SPEC_REF"

check_pair() {
  local fixture_kind="$1"
  local spec_dir="$TMP_DIR/spec/tests/fixtures/$fixture_kind"
  local local_dir="$ROOT_DIR/core/src/test/resources/conformance/$fixture_kind"

  echo "Comparing $fixture_kind fixtures"
  diff -ru --strip-trailing-cr "$spec_dir" "$local_dir"
}

check_pair "decode"
check_pair "encode"

echo "Conformance fixtures are in sync."
