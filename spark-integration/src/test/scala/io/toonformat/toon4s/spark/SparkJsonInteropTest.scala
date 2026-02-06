package io.toonformat.toon4s.spark

import scala.collection.immutable.VectorMap

import io.toonformat.toon4s.JsonValue
import io.toonformat.toon4s.JsonValue._
import munit.FunSuite
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

class SparkJsonInteropTest extends FunSuite {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkJsonInteropTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("rowToJsonValue: convert simple row") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("active", BooleanType),
    ))

    val row = Row(1, "Alice", true)
    val json = SparkJsonInterop.rowToJsonValue(row, schema)

    assertEquals(
      json,
      JObj(VectorMap(
        "id" -> JNumber(BigDecimal(1)),
        "name" -> JString("Alice"),
        "active" -> JBool(true),
      )),
    )
  }

  test("rowToJsonValue: handle null values") {
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType),
    ))

    val row = Row("Bob", null)
    val json = SparkJsonInterop.rowToJsonValue(row, schema)

    assertEquals(
      json,
      JObj(VectorMap(
        "name" -> JString("Bob"),
        "age" -> JNull,
      )),
    )
  }

  test("rowToJsonValue: handle numeric types") {
    val schema = StructType(Seq(
      StructField("int_val", IntegerType),
      StructField("long_val", LongType),
      StructField("double_val", DoubleType),
      StructField("float_val", FloatType),
    ))

    val row = Row(42, 100L, 3.14, 2.71F)
    val json = SparkJsonInterop.rowToJsonValue(row, schema)

    json match {
    case JObj(fields) =>
      assertEquals(fields("int_val"), JNumber(BigDecimal(42)))
      assertEquals(fields("long_val"), JNumber(BigDecimal(100)))
      assert(fields("double_val").isInstanceOf[JNumber])
      assert(fields("float_val").isInstanceOf[JNumber])
    case _ => fail("Expected JObj")
    }
  }

  test("rowToJsonValue: handle nested struct") {
    val innerSchema = StructType(Seq(
      StructField("city", StringType),
      StructField("zip", IntegerType),
    ))

    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("address", innerSchema),
    ))

    val row = Row("Alice", Row("NYC", 10001))
    val json = SparkJsonInterop.rowToJsonValue(row, schema)

    json match {
    case JObj(fields) =>
      assertEquals(fields("name"), JString("Alice"))
      assert(fields("address").isInstanceOf[JObj])
    case _ => fail("Expected JObj")
    }
  }

  test("rowToJsonValue: handle arrays") {
    val schema = StructType(Seq(
      StructField("tags", ArrayType(StringType))
    ))

    val row = Row(Seq("scala", "spark", "toon"))
    val json = SparkJsonInterop.rowToJsonValue(row, schema)

    json match {
    case JObj(fields) =>
      assertEquals(
        fields("tags"),
        JArray(Vector(
          JString("scala"),
          JString("spark"),
          JString("toon"),
        )),
      )
    case _ => fail("Expected JObj")
    }
  }

  test("jsonValueToRow: convert simple json") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
    ))

    val json = JObj(VectorMap(
      "id" -> JNumber(BigDecimal(1)),
      "name" -> JString("Alice"),
    ))

    val row = SparkJsonInterop.jsonValueToRow(json, schema)
    assertEquals(row.getInt(0), 1)
    assertEquals(row.getString(1), "Alice")
  }

  test("jsonValueToRow: handle missing fields") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("age", IntegerType),
    ))

    val json = JObj(VectorMap(
      "id" -> JNumber(BigDecimal(1)),
      "name" -> JString("Alice"),
    ))

    val row = SparkJsonInterop.jsonValueToRow(json, schema)
    assertEquals(row.getInt(0), 1)
    assertEquals(row.getString(1), "Alice")
    assert(row.isNullAt(2))
  }

  test("rowToJsonValueSafe: handle conversion errors") {
    val schema = StructType(Seq(StructField("id", IntegerType)))
    val row = Row(1)

    val result = SparkJsonInterop.rowToJsonValueSafe(row, schema)
    assert(result.isRight)
  }

  test("jsonValueToRowSafe: handle conversion errors") {
    val schema = StructType(Seq(StructField("id", IntegerType)))
    val json = JObj(VectorMap("id" -> JNumber(BigDecimal(1))))

    val result = SparkJsonInterop.jsonValueToRowSafe(json, schema)
    assert(result.isRight)
  }

  test("jsonValueToRowSafe: fail on non-JObj") {
    val schema = StructType(Seq(StructField("id", IntegerType)))
    val json = JArray(Vector(JNumber(BigDecimal(1))))

    val result = SparkJsonInterop.jsonValueToRowSafe(json, schema)
    assert(result.isLeft)
  }

  test("round-trip: Row -> JsonValue -> Row") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("score", DoubleType),
    ))

    val originalRow = Row(42, "test", 95.5)
    val json = SparkJsonInterop.rowToJsonValue(originalRow, schema)
    val reconstructedRow = SparkJsonInterop.jsonValueToRow(json, schema)

    assertEquals(reconstructedRow.getInt(0), 42)
    assertEquals(reconstructedRow.getString(1), "test")
    assertEquals(reconstructedRow.getDouble(2), 95.5, 0.0001)
  }

  test("rowToJsonValue: encode timestamp as ISO-8601") {
    val schema = StructType(Seq(
      StructField("event_time", TimestampType)
    ))

    val timestamp = java.sql.Timestamp.valueOf("2026-02-06 12:34:56")
    val row = Row(timestamp)

    val json = SparkJsonInterop.rowToJsonValue(row, schema)
    json match {
    case JObj(fields) =>
      fields("event_time") match {
      case JString(value) =>
        val parsed = java.time.Instant.parse(value)
        assertEquals(parsed, timestamp.toInstant)
      case other =>
        fail(s"Expected JString timestamp, got $other")
      }
    case other =>
      fail(s"Expected JObj, got $other")
    }
  }

  test("jsonValueToRow: decode ISO-8601 timestamp string") {
    val schema = StructType(Seq(
      StructField("event_time", TimestampType)
    ))

    val timestamp = java.sql.Timestamp.valueOf("2026-02-06 12:34:56")
    val json = JObj(VectorMap("event_time" -> JString(timestamp.toInstant.toString)))

    val row = SparkJsonInterop.jsonValueToRow(json, schema)
    assertEquals(row.getAs[java.sql.Timestamp](0).toInstant, timestamp.toInstant)
  }

  test("round-trip: timestamp_ntz uses LocalDateTime") {
    val schema = StructType(Seq(
      StructField("event_time_ntz", TimestampNTZType)
    ))

    val localDateTime = java.time.LocalDateTime.of(2026, 2, 6, 12, 34, 56, 123000000)
    val row = Row(localDateTime)

    val json = SparkJsonInterop.rowToJsonValue(row, schema)
    val reconstructed = SparkJsonInterop.jsonValueToRow(json, schema)

    assertEquals(reconstructed.getAs[java.time.LocalDateTime](0), localDateTime)
  }

  test("jsonValueToRowSafe: fail on invalid integer coercion") {
    val schema = StructType(Seq(StructField("id", IntegerType)))
    val json = JObj(VectorMap("id" -> JString("not-a-number")))

    val result = SparkJsonInterop.jsonValueToRowSafe(json, schema)
    assert(result.isLeft)
  }

  test("jsonValueToRowSafe: fail on invalid boolean coercion") {
    val schema = StructType(Seq(StructField("active", BooleanType)))
    val json = JObj(VectorMap("active" -> JString("sometimes")))

    val result = SparkJsonInterop.jsonValueToRowSafe(json, schema)
    assert(result.isLeft)
  }

  test("jsonValueToRow: decode map keys using schema key type") {
    val schema = StructType(Seq(
      StructField("scores", MapType(IntegerType, StringType, valueContainsNull = false))
    ))

    val json = JObj(VectorMap(
      "scores" -> JObj(VectorMap("1" -> JString("low"), "2" -> JString("high")))
    ))

    val row = SparkJsonInterop.jsonValueToRow(json, schema)
    val scores = row.getMap[Int, String](0)
    assertEquals(scores.get(1), Some("low"))
    assertEquals(scores.get(2), Some("high"))
  }

}
