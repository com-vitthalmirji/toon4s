package io.toonformat.toon4s.spark

import io.toonformat.toon4s.spark.testkit.SparkTestSuite
import munit.FunSuite

class AdaptiveChunkingTest extends FunSuite {

  test("calculateOptimalChunkSize: handles huge row count safely") {
    val strategy = AdaptiveChunking.calculateOptimalChunkSize(
      totalRows = Int.MaxValue.toLong + 1000L,
      avgRowSize = 0,
    )

    assert(strategy.useToon)
    assert(strategy.chunkSize >= 100)
    assert(strategy.chunkSize <= 1000)
  }

  test("calculateOptimalChunkSize: keeps large dataset chunk under cap") {
    val strategy = AdaptiveChunking.calculateOptimalChunkSize(
      totalRows = Int.MaxValue.toLong + 1000L,
      avgRowSize = 1,
    )

    assert(strategy.useToon)
    assert(strategy.chunkSize >= 100)
    assert(strategy.chunkSize <= 1000)
  }

  test("calculateOptimalChunkSize: saturates estimated size on overflow") {
    val strategy = AdaptiveChunking.calculateOptimalChunkSize(
      totalRows = Long.MaxValue,
      avgRowSize = Int.MaxValue,
    )

    assert(strategy.useToon)
    assertEquals(strategy.estimatedDataSize, Long.MaxValue)
    assert(strategy.chunkSize >= 100)
    assert(strategy.chunkSize <= 1000)
  }

}

class AdaptiveChunkingSparkTest extends SparkTestSuite {

  test("probeEfficiency: measures bytes on tabular data") {
    val s = spark
    import s.implicits._
    val df = (1 to 50).map(i => (i, s"name_$i", i * 10)).toDF("id", "name", "score")
    val probe = AdaptiveChunking.probeEfficiency(df, key = "users")

    assertEquals(probe.sampleRows, 20)
    assert(probe.jsonBytes > 0L, "JSON bytes must be positive")
    assert(probe.toonBytes > 0L, "TOON bytes must be positive")
    assert(probe.ratio > 0.0, "ratio must be positive")
  }

  test("probeEfficiency: detects TOON overhead on string-heavy rows") {
    val s = spark
    import s.implicits._
    val longStr = "x" * 200
    val df = (1 to 30).map { i =>
      (longStr + i, longStr + i * 2, longStr + i * 3, longStr + i * 4, longStr + i * 5)
    }.toDF("col_a", "col_b", "col_c", "col_d", "col_e")
    val probe = AdaptiveChunking.probeEfficiency(df, key = "data")

    assert(probe.sampleRows > 0)
    assert(probe.jsonBytes > 0L)
    assert(probe.toonBytes > 0L)
    assert(
      !probe.toonEfficient,
      s"TOON must not be efficient on string-heavy data, ratio=${probe.ratio}",
    )
    assert(probe.ratio > 1.0, s"TOON should produce more bytes than JSON, ratio=${probe.ratio}")
  }

  test("probeEfficiency: empty DataFrame returns non-efficient probe") {
    val df = spark.emptyDataFrame
    val probe = AdaptiveChunking.probeEfficiency(df, key = "empty")

    assertEquals(probe.sampleRows, 0)
    assertEquals(probe.jsonBytes, 0L)
    assertEquals(probe.toonBytes, 0L)
    assert(!probe.toonEfficient)
  }

  test("shouldUseToon: rejects string-heavy flat data via probe") {
    val s = spark
    import s.implicits._
    val longStr = "x" * 200
    val df =
      (1 to 50).map(i => (longStr + i, longStr + i * 2, longStr + i * 3)).toDF("a", "b", "c")

    val result = AdaptiveChunking.shouldUseToon(df, key = "data")
    assert(!result, "shouldUseToon must reject string-heavy data where TOON loses on bytes")
  }

  test("shouldUseToon: rejects flat CSV-like tabular data where TOON adds overhead") {
    val s = spark
    import s.implicits._
    // Simulates the CSV aggregation workload from WORKLOAD_MEASUREMENT_2026-02-21.md
    val df = (1 to 100).map(i => (i, s"n$i", i % 5 == 0)).toDF("id", "name", "active")
    val probe = AdaptiveChunking.probeEfficiency(df, key = "users")

    // The probe correctly detects that TOON format produces more bytes than compact JSON
    // for this data shape. This is the expected behavior - TOON format adds per-line
    // indentation overhead that exceeds JSON's per-row key repetition.
    assert(probe.ratio > 1.0, s"Probe must detect TOON overhead, ratio=${probe.ratio}")

    val result = AdaptiveChunking.shouldUseToon(df, key = "users")
    assert(!result, "shouldUseToon must reject data where TOON produces more bytes")
  }

  test("probeEfficiency: respects custom sample size") {
    val s = spark
    import s.implicits._
    val df = (1 to 100).map(i => (i, s"val_$i")).toDF("id", "value")
    val probe = AdaptiveChunking.probeEfficiency(df, sampleSize = 5)
    assertEquals(probe.sampleRows, 5)
  }

}
