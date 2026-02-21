# Migration guide

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

## Behavior notes

- `toonMetrics` chunk size should match production encode chunk size for aligned estimates.
- DataSource writes chunk rows by `maxRowsPerFile` per output file.
- On Windows CI, write path tests are guarded due missing Hadoop `winutils`.
