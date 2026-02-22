#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: scripts/parse-workload-measurement.sh <log_or_note_file>" >&2
  exit 1
fi

input_file="$1"

if [[ ! -f "$input_file" ]]; then
  echo "File not found: $input_file" >&2
  exit 1
fi

extract_metric() {
  local key="$1"
  local line
  line="$(rg -m1 "^(\\- )?${key}:" "$input_file" || true)"
  if [[ -z "${line:-}" ]]; then
    return 0
  fi
  local value
  value="$line"
  value="${value#- ${key}: }"
  value="${value#${key}: }"
  value="${value#\`}"
  value="${value%\`}"
  value="$(printf '%s\n' "$value" | tr -d ',' | sed -E 's/[[:space:]]+$//')"
  printf '%s\n' "$value"
}

rows="$(extract_metric "rows")"
json_bytes="$(extract_metric "jsonBytes")"
toon_bytes="$(extract_metric "toonBytes")"
bytes_savings_pct="$(extract_metric "bytesSavingsPct")"
json_tokens="$(extract_metric "jsonTokens")"
toon_tokens="$(extract_metric "toonTokens")"
token_savings_pct="$(extract_metric "tokenSavingsPct")"
json_encode_ms="$(extract_metric "jsonEncodeMs")"
toon_encode_ms="$(extract_metric "toonEncodeMs")"
overhead_pct_vs_json="$(extract_metric "overheadPctVsJson")"
chunk_count="$(extract_metric "chunkCount")"
avg_chunk_bytes="$(extract_metric "avgChunkBytes")"

required_values=(
  "$rows"
  "$json_bytes"
  "$toon_bytes"
  "$bytes_savings_pct"
  "$json_tokens"
  "$toon_tokens"
  "$token_savings_pct"
  "$json_encode_ms"
  "$toon_encode_ms"
  "$overhead_pct_vs_json"
  "$chunk_count"
  "$avg_chunk_bytes"
)

for value in "${required_values[@]}"; do
  if [[ -z "${value:-}" ]]; then
    echo "Missing required metric in $input_file" >&2
    exit 2
  fi
done

bytes_savings_pct_clean="${bytes_savings_pct%\%}"
token_savings_pct_clean="${token_savings_pct%\%}"
overhead_pct_vs_json_clean="${overhead_pct_vs_json%\%}"

token_winner="JSON"
if awk -v v="$token_savings_pct_clean" 'BEGIN { exit (v+0 > 0 ? 0 : 1) }'; then
  token_winner="TOON"
fi

printf '%s\n' "{"
printf '  "source": "%s",\n' "$input_file"
printf '  "rows": %s,\n' "$rows"
printf '  "jsonBytes": %s,\n' "$json_bytes"
printf '  "toonBytes": %s,\n' "$toon_bytes"
printf '  "bytesSavingsPct": %s,\n' "$bytes_savings_pct_clean"
printf '  "jsonTokens": %s,\n' "$json_tokens"
printf '  "toonTokens": %s,\n' "$toon_tokens"
printf '  "tokenSavingsPct": %s,\n' "$token_savings_pct_clean"
printf '  "jsonEncodeMs": %s,\n' "$json_encode_ms"
printf '  "toonEncodeMs": %s,\n' "$toon_encode_ms"
printf '  "overheadPctVsJson": %s,\n' "$overhead_pct_vs_json_clean"
printf '  "chunkCount": %s,\n' "$chunk_count"
printf '  "avgChunkBytes": %s,\n' "$avg_chunk_bytes"
printf '  "tokenWinner": "%s"\n' "$token_winner"
printf '%s\n' "}"

printf '\n'
printf '%s\n' "| source | rows | json bytes | toon bytes | bytes savings pct | json tokens | toon tokens | token savings pct | json ms | toon ms | overhead pct | chunks | avg chunk bytes | token winner |"
printf '%s\n' "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"
printf '| %s | %s | %s | %s | %s%% | %s | %s | %s%% | %s | %s | %s%% | %s | %s | %s |\n' \
  "$input_file" \
  "$rows" \
  "$json_bytes" \
  "$toon_bytes" \
  "$bytes_savings_pct_clean" \
  "$json_tokens" \
  "$toon_tokens" \
  "$token_savings_pct_clean" \
  "$json_encode_ms" \
  "$toon_encode_ms" \
  "$overhead_pct_vs_json_clean" \
  "$chunk_count" \
  "$avg_chunk_bytes" \
  "$token_winner"
