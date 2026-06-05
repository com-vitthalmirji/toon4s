package io.toonformat.toon4s.spark

import scala.collection.immutable.VectorMap
import scala.jdk.CollectionConverters._

import io.toonformat.toon4s.{Delimiter, EncodeOptions, Toon}
import io.toonformat.toon4s.JsonValue._
import io.toonformat.toon4s.spark.SparkToonOps._
import io.toonformat.toon4s.spark.testkit.SparkTestSuite
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

/**
 * The flat fast path must produce output byte-identical to the general JsonValue path. Reference is
 * computed by converting each row with SparkJsonInterop and encoding the wrapped tabular array.
 */
class SparkDirectEmitterTest extends SparkTestSuite {

  private val delimiters = List(Delimiter.Comma, Delimiter.Tab, Delimiter.Pipe)

  private def reference(
      df: DataFrame,
      key: String,
      maxRowsPerChunk: Int,
      options: EncodeOptions,
  ): Vector[String] = {
    val schema = df.schema
    df.collect()
      .toVector
      .map(r => SparkJsonInterop.rowToJsonValue(r, schema))
      .grouped(maxRowsPerChunk)
      .map(chunk =>
        Toon.encode(JObj(VectorMap(key -> JArray(chunk.toVector))), options).toOption.get
      )
      .toVector
  }

  private val fullSchema = StructType(Seq(
    StructField("s", StringType),
    StructField("i", IntegerType),
    StructField("l", LongType),
    StructField("d", DoubleType),
    StructField("f", FloatType),
    StructField("b", BooleanType),
    StructField("by", ByteType),
    StructField("sh", ShortType),
    StructField("dec", DecimalType(20, 4)),
    StructField("dt", DateType),
    StructField("ts", TimestampType),
    StructField("bin", BinaryType),
  ))

  private val fullData = Seq(
    Row(
      "plain",
      42,
      Long.MaxValue,
      3.14D,
      2.5F,
      true,
      7.toByte,
      9.toShort,
      new java.math.BigDecimal("123.4500"),
      java.sql.Date.valueOf("2026-02-06"),
      java.sql.Timestamp.valueOf("2026-02-06 12:34:56"),
      Array[Byte](1, 2, 3),
    ),
    Row(
      "needs, quote",
      -1,
      -100L,
      Double.NaN,
      -0.0F,
      false,
      -1.toByte,
      -2.toShort,
      new java.math.BigDecimal("-0.5000"),
      java.sql.Date.valueOf("1999-12-31"),
      java.sql.Timestamp.valueOf("2000-01-01 00:00:00"),
      Array[Byte](-1, 0, 127),
    ),
    Row(null, null, null, null, null, null, null, null, null, null, null, null),
  )

  test("flat fast path matches reference for all primitive types and delimiters") {
    val df = spark.createDataFrame(fullData.asJava, fullSchema).coalesce(1)
    delimiters.foreach { d =>
      val options = EncodeOptions(delimiter = d)
      val actual = df.toToon(ToonSparkOptions("rows", maxRowsPerChunk = 1000, options))
      assert(actual.isRight, clues(d))
      val expected = reference(df, "rows", maxRowsPerChunk = 1000, options)
      assertEquals(actual.toOption.get, expected, clues(d))
    }
  }

  test("flat fast path matches reference across chunk boundaries") {
    val schema = StructType(Seq(
      StructField("id", LongType),
      StructField("name", StringType),
    ))
    val data = (1 to 7).map(i => Row(i.toLong, s"user$i"))
    val df = spark.createDataFrame(data.asJava, schema).coalesce(1)
    val options = EncodeOptions()

    val actual = df.toToon(ToonSparkOptions("rows", maxRowsPerChunk = 2, options))
    assert(actual.isRight)
    val expected = reference(df, "rows", maxRowsPerChunk = 2, options)
    assertEquals(actual.toOption.get, expected)
  }

  test("nested schema still uses the general path and round-trips") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("tags", ArrayType(StringType)),
    ))
    val data = Seq(Row(1, Seq("a", "b")), Row(2, Seq("c")))
    val df = spark.createDataFrame(data.asJava, schema).coalesce(1)

    val actual = df.toToon(ToonSparkOptions("rows", maxRowsPerChunk = 1000))
    assert(actual.isRight)
    val expected = reference(df, "rows", maxRowsPerChunk = 1000, EncodeOptions())
    assertEquals(actual.toOption.get, expected)
  }

}
