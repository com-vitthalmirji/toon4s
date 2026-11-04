#!/usr/bin/env bash
set -euo pipefail

# Turns the output of the workload measurement runner (examples.WorkloadMeasurementRunner) into a JSON
# object and one markdown table row, ready for a measurement note or the benchmark docs.
#
# Usage: scripts/parse-workload-measurement.sh [--header] <log-file>
#
#   <log-file>  saved console output of a run; Spark and sbt log lines around the block are ignored
#   --header    also print the markdown header and separator rows before the data row
#
# When the log holds several runs, the last "=== TOON workload measurement ===" block is used.
# Exits non-zero, naming the field, when the block is missing or incomplete.

usage() {
  echo "Usage: $0 [--header] <log-file>" >&2
  exit 2
}

header=false
if [[ "${1:-}" == "--header" ]]; then
  header=true
  shift
fi
[[ $# -eq 1 ]] || usage
log="$1"
[[ -r "$log" ]] || { echo "error: cannot read $log" >&2; exit 1; }

fields=(rows chunkCount avgChunkBytes jsonBytes toonBytes bytesSavingsPct jsonTokens toonTokens
  tokenSavingsPct jsonEncodeMs toonEncodeMs overheadPctVsJson)

# Keep the last block: reset at each header, then collect "name: value" lines. A leading log prefix
# such as "[info] " and a trailing "%" are dropped.
block="$(awk '
  /=== TOON workload measurement ===/ { delete seen; n = 0; found = 1; next }
  found {
    line = $0
    sub(/^\[[a-z]+\][ \t]*/, "", line)
    if (match(line, /^[A-Za-z]+: *-?[0-9][0-9.]*%?[ \t\r]*$/)) {
      split(line, kv, /: */)
      value = kv[2]
      gsub(/[%\r \t]/, "", value)
      if (!(kv[1] in seen)) order[++n] = kv[1]
      seen[kv[1]] = value
    }
  }
  END {
    if (!found) exit 3
    for (i = 1; i <= n; i++) print order[i] "=" seen[order[i]]
  }
' "$log")" || { echo "error: no '=== TOON workload measurement ===' block in $log" >&2; exit 1; }

values=()
for f in "${fields[@]}"; do
  v="$(printf '%s\n' "$block" | awk -F= -v k="$f" '$1 == k { print $2 }' | tail -n 1)"
  [[ -n "$v" ]] || { echo "error: field '$f' missing from the measurement block in $log" >&2; exit 1; }
  values+=("$v")
done

json="{"
for i in "${!fields[@]}"; do
  [[ $i -gt 0 ]] && json+=", "
  json+="\"${fields[$i]}\": ${values[$i]}"
done
json+="}"
echo "$json"

if $header; then
  echo "| Rows | Chunks | Avg chunk bytes | JSON bytes | TOON bytes | Byte savings % | JSON tokens | TOON tokens | Token savings % | JSON encode ms | TOON encode ms | TOON overhead % |"
  echo "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
fi
row="|"
for v in "${values[@]}"; do row+=" $v |"; done
echo "$row"
