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

  test("probeEfficiency: TOON is near parity on string-heavy rows") {
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
    // With tabular encoding working correctly, TOON eliminates per-row key
    // repetition. Even string-heavy data benefits because tabular headers
    // amortize field names across all rows.
    assert(probe.ratio < 1.5, s"TOON ratio should be reasonable, ratio=${probe.ratio}")
  }

  test("probeEfficiency: empty DataFrame returns non-efficient probe") {
    val df = spark.emptyDataFrame
    val probe = AdaptiveChunking.probeEfficiency(df, key = "empty")

    assertEquals(probe.sampleRows, 0)
    assertEquals(probe.jsonBytes, 0L)
    assertEquals(probe.toonBytes, 0L)
    assert(!probe.toonEfficient)
  }

  test("shouldUseToon: accepts string-heavy data when tabular encoding wins") {
    val s = spark
    import s.implicits._
    val longStr = "x" * 200
    val df =
      (1 to 50).map(i => (longStr + i, longStr + i * 2, longStr + i * 3)).toDF("a", "b", "c")

    val result = AdaptiveChunking.shouldUseToon(df, key = "data")
    // With tabular encoding working, TOON eliminates per-row key repetition
    // even for string-heavy data, so it should be accepted.
    assert(result, "shouldUseToon should accept data where tabular TOON wins on bytes")
  }

  test("shouldUseToon: accepts flat CSV-like tabular data where TOON wins") {
    val s = spark
    import s.implicits._
    // Simulates the CSV aggregation workload from WORKLOAD_MEASUREMENT_2026-02-21.md
    val df = (1 to 100).map(i => (i, s"n$i", i % 5 == 0)).toDF("id", "name", "active")
    val probe = AdaptiveChunking.probeEfficiency(df, key = "users")

    // With tabular encoding working correctly, TOON uses compact header+row format
    // (e.g. users[N]{id,name,active}: 1,n1,false) which eliminates per-row key
    // repetition and beats JSON on byte count.
    assert(probe.ratio < 1.0, s"TOON must win on tabular data, ratio=${probe.ratio}")

    val result = AdaptiveChunking.shouldUseToon(df, key = "users")
    assert(result, "shouldUseToon must accept tabular data where TOON wins")
  }

  test("probeEfficiency: respects custom sample size") {
    val s = spark
    import s.implicits._
    val df = (1 to 100).map(i => (i, s"val_$i")).toDF("id", "value")
    val probe = AdaptiveChunking.probeEfficiency(df, sampleSize = 5)
    assertEquals(probe.sampleRows, 5)
  }

}
