# Migration guide

## Scope

This guide covers migration inside the current minor line and migration from string-arg APIs to `ToonSparkOptions`.

## Preferred API

For new code, prefer `ToonSparkOptions`.

Before:

```scala
df.toToon(key = "users", maxRowsPerChunk = 500)
df.toonMetrics(key = "users", maxRowsPerChunk = 500)
```

After:

```scala
val options = ToonSparkOptions(key = "users", maxRowsPerChunk = 500)
df.toToon(options)
df.toonMetrics(options)
```

## Data source v2

Write:

```scala
df.write
  .format("toon")
  .option("path", "/tmp/toon-output")
  .option("key", "users")
  .option("maxRowsPerFile", "1000")
  .save()
```

Read:

```scala
spark.read
  .format("toon")
  .option("path", "/tmp/toon-output")
  .load()
```

## SQL extension

Set:

```scala
spark.conf.set(
  "spark.sql.extensions",
  "io.toonformat.toon4s.spark.extensions.ToonSparkSessionExtensions"
)
```

## Stable entry points checklist

- DataFrame encode: `toToon(options: ToonSparkOptions)`
- DataFrame metrics: `toonMetrics(options: ToonSparkOptions)`
- Dataset encode: `toToon(options: ToonSparkOptions)`
- Dataset metrics: `toonMetrics(options: ToonSparkOptions)`
- Data source name: `format("toon")`
- SQL extension provider: `io.toonformat.toon4s.spark.extensions.ToonSparkSessionExtensions`

## Breaking behavior list

Current migration path has no required breaking API changes.

Behavior changes to note:
- `toonMetrics` should use the same chunk size as `toToon` for aligned estimates.
- Data source write tests are guarded on Windows CI because `winutils` is missing.

## Behavior notes

- `toonMetrics` chunk size should match production encode chunk size for aligned estimates.
- DataSource writes chunk rows by `maxRowsPerFile` per output file.
- On Windows CI, write path tests are guarded due missing Hadoop `winutils`.
