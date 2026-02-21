package io.toonformat.toon4s.spark.integrations

import java.time.Instant

import io.toonformat.toon4s.spark.testkit.SparkTestSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

class IcebergTimeTravelTest extends SparkTestSuite {

  test("asOfTimestampOptionValue uses epoch millis") {
    val instant = Instant.parse("2024-12-01T00:00:00Z")
    val optionValue = IcebergTimeTravel.asOfTimestampOptionValue(instant)

    assertEquals(optionValue, "1733011200000")
  }

  test("generateTimestampSequence includes both boundaries") {
    val start = Instant.parse("2024-01-01T00:00:00Z")
    val end = Instant.parse("2024-01-01T00:00:10Z")

    val seq = IcebergTimeTravel.generateTimestampSequence(start, end, intervalSeconds = 5L)

    assertEquals(
      seq,
      Vector(
        Instant.parse("2024-01-01T00:00:00Z"),
        Instant.parse("2024-01-01T00:00:05Z"),
        Instant.parse("2024-01-01T00:00:10Z"),
      ),
    )
  }

  test("encodeWithAdaptiveChunking rejects non-positive configured chunk size") {
    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = false),
    ))
    val rows = Seq(Row(1L, "a"), Row(2L, "b"))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    val config = IcebergTimeTravel.TimeTravelConfig(
      tableName = "ignored",
      maxRowsPerChunk = Some(0),
    )

    val result = IcebergTimeTravel.encodeWithAdaptiveChunking(df, config)

    assert(result.isLeft)
    result.left.foreach(err =>
      assert(err.message.contains("maxRowsPerChunk must be greater than 0"))
    )
  }

  test("encodeWithAdaptiveChunking uses configured chunk size") {
    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = false),
    ))
    val rows = (1L to 6L).map(id => Row(id, s"u$id"))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    val config = IcebergTimeTravel.TimeTravelConfig(
      tableName = "ignored",
      key = "snapshot",
      maxRowsPerChunk = Some(2),
    )

    val result = IcebergTimeTravel.encodeWithAdaptiveChunking(df, config)

    assert(result.isRight)
    result.foreach(chunks => assertEquals(chunks.size, 3))
  }

}
