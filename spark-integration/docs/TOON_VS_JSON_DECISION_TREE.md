# TOON vs JSON decision tree

Use this flow before enabling TOON in a Spark pipeline.

## Step 1: data shape

- Mostly tabular and shallow (0-2 nesting)?
  - Yes: go to step 2
  - No: prefer JSON

## Step 2: payload size

- Chunk payload usually above 10KB?
  - Yes: go to step 3
  - No: JSON usually wins due to prompt overhead

## Step 3: usage pattern

- Repeated LLM calls on similar schemas?
  - Yes: TOON is a strong candidate
  - No: either format may work, measure both

## Step 4: safety checks

- Run alignment analysis and metrics first:
  - `ToonAlignmentAnalyzer.analyzeSchema(df.schema)`
  - `df.toonMetrics(ToonSparkOptions(...))`
- If alignment is low or savings are negative, stay on JSON.

## Step 5: production path

- For large datasets use:
  - `toToonDataset`
  - `writeToon`
  - `writeToLlmPartitions`
- Keep `toToon` for bounded interactive usage only.

## Rule of thumb

- Do not assume TOON always wins.
- Measure on your real workload and keep the measurement note with command and output.
