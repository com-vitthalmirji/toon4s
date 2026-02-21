# Prompt engineering for TOON payloads

This guide shows prompt patterns that work well with TOON chunks.

## Base system prompt

Use this when sending a TOON chunk to an LLM:

```text
You will receive data encoded in TOON format.
Read it as structured tabular data.
Do not rewrite the input as JSON.
Answer using plain text unless I ask for JSON.
If a field is missing, say "missing field".
```

## Classification prompt

```text
Task: classify each row as HIGH, MEDIUM, or LOW risk.
Use only fields present in the TOON input.
Output one line per row: <id>,<risk>,<reason>.
```

## Aggregation prompt

```text
Task: summarize top 5 patterns from this TOON dataset.
Focus on repeated values and outliers.
Return:
1) key finding
2) supporting rows
3) confidence (low/medium/high)
```

## Extraction prompt with strict schema

```text
Task: extract rows that satisfy the filter below.
Filter: country == "US" and amount > 1000.
Return strict JSON array with fields: id, country, amount.
If no row matches, return [].
```

## Response validation pattern

- Prefer one of:
  - plain text summary
  - strict JSON output with required fields
- Validate the response in your pipeline:
  - JSON schema check for strict JSON mode
  - fallback or retry with smaller chunk on parse failure

## Chunking guidance for prompts

- Use `maxRowsPerChunk` values that keep prompts under model context limits.
- Keep prompt template fixed and vary only chunk content.
- For large jobs, use `toToonDataset` + partition sinks instead of collecting on driver.
