package io.toonformat.toon4s.spark

import java.sql.{Date, Timestamp}
import java.time.Instant

import io.toonformat.toon4s.spark.error.SparkToonError
import munit.ScalaCheckSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

class SparkJsonInteropPropertyTest extends ScalaCheckSuite {

  private val schema = StructType(
    Vector(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("score", DoubleType, nullable = false),
      StructField("active", BooleanType, nullable = false),
      StructField("tag", StringType, nullable = true),
    )
  )

  private val rowGen: Gen[Row] =
    for {
      id <- Gen.choose(-100000, 100000)
      name <- Gen.alphaStr.suchThat(_.nonEmpty)
      score <- Gen.choose(-100000.0, 100000.0)
      active <- Gen.oneOf(true, false)
      tag <- Gen.option(Gen.alphaStr)
    } yield Row(id, name, score, active, tag.orNull)

  property("rowToJsonValueSafe and jsonValueToRowSafe round-trip primitive rows") {
    forAll(rowGen) { row =>
      val result = for {
        json <- SparkJsonInterop.rowToJsonValueSafe(row, schema)
        decoded <- SparkJsonInterop.jsonValueToRowSafe(json, schema)
      } yield decoded

      result match {
      case Right(decoded) =>
        (decoded.getInt(0) == row.getInt(0)) &&
        (decoded.getString(1) == row.getString(1)) &&
        (decoded.getDouble(2) == row.getDouble(2)) &&
        (decoded.getBoolean(3) == row.getBoolean(3)) &&
        (decoded.get(4) == row.get(4))
      case Left(err) =>
        fail(s"Round-trip failed: ${formatError(err)}")
        false
      }
    }
  }

  private val nestedSchema = StructType(
    Vector(
      StructField("id", LongType, nullable = false),
      StructField("tags", ArrayType(StringType, containsNull = false), nullable = false),
      StructField(
        "meta",
        StructType(
          Vector(
            StructField("count", IntegerType, nullable = false),
            StructField("ok", BooleanType, nullable = false),
          )
        ),
        nullable = false,
      ),
      StructField(
        "attrs",
        MapType(StringType, IntegerType, valueContainsNull = false),
        nullable = false,
      ),
    )
  )

  private val nestedRowGen: Gen[Row] =
    for {
      id <- Gen.choose(0L, 100000L)
      rawTags <- Gen.listOfN(3, Gen.alphaStr.map(s => if (s.isEmpty) "x" else s.take(8)))
      count <- Gen.choose(0, 1000)
      ok <- Gen.oneOf(true, false)
      valueA <- Gen.choose(-1000, 1000)
      valueB <- Gen.choose(-1000, 1000)
    } yield {
      val tags = rawTags.toSeq
      val meta = Row(count, ok)
      val attrs = Map("a" -> valueA, "b" -> valueB)
      Row(id, tags, meta, attrs)
    }

  property("rowToJsonValueSafe and jsonValueToRowSafe round-trip nested rows") {
    forAll(nestedRowGen) { row =>
      val result = for {
        json <- SparkJsonInterop.rowToJsonValueSafe(row, nestedSchema)
        decoded <- SparkJsonInterop.jsonValueToRowSafe(json, nestedSchema)
      } yield decoded

      result match {
      case Right(decoded) =>
        val decodedTags = decoded.getSeq[String](1)
        val expectedTags = row.getSeq[String](1)
        val decodedMeta = decoded.getStruct(2)
        val expectedMeta = row.getStruct(2)
        val decodedAttrs = decoded.getMap[String, Int](3)
        val expectedAttrs = row.getMap[String, Int](3)

        (decoded.getLong(0) == row.getLong(0)) &&
        (decodedTags == expectedTags) &&
        (decodedMeta.getInt(0) == expectedMeta.getInt(0)) &&
        (decodedMeta.getBoolean(1) == expectedMeta.getBoolean(1)) &&
        (decodedAttrs == expectedAttrs)
      case Left(err) =>
        fail(s"Round-trip failed: ${formatError(err)}")
        false
      }
    }
  }

  private val scalarSchema = StructType(
    Vector(
      StructField("dec", DecimalType(12, 2), nullable = false),
      StructField("d", DateType, nullable = false),
      StructField("ts", TimestampType, nullable = false),
      StructField("bin", BinaryType, nullable = false),
    )
  )

  private val scalarRowGen: Gen[Row] =
    for {
      cents <- Gen.choose(-100000000L, 100000000L)
      epochDays <- Gen.choose(-10000L, 10000L)
      millis <- Gen.choose(0L, 2000000000000L)
      bytes <- Gen.listOfN(8, Gen.choose(0, 255)).map(_.map(_.toByte).toArray)
    } yield {
      val decimal = java.math.BigDecimal.valueOf(cents, 2)
      val date = Date.valueOf(
        Instant.ofEpochMilli(epochDays * 86400000L).atZone(java.time.ZoneOffset.UTC).toLocalDate
      )
      val timestamp = Timestamp.from(Instant.ofEpochMilli(millis))
      Row(decimal, date, timestamp, bytes)
    }

  property("rowToJsonValueSafe and jsonValueToRowSafe round-trip decimal temporal and binary rows") {
    forAll(scalarRowGen) { row =>
      val result = for {
        json <- SparkJsonInterop.rowToJsonValueSafe(row, scalarSchema)
        decoded <- SparkJsonInterop.jsonValueToRowSafe(json, scalarSchema)
      } yield decoded

      result match {
      case Right(decoded) =>
        val expectedBytes = row.getAs[Array[Byte]](3)
        val decodedBytes = decoded.getAs[Array[Byte]](3)
        (decoded.getDecimal(0) == row.getDecimal(0)) &&
        (decoded.getDate(1) == row.getDate(1)) &&
        (decoded.getTimestamp(2) == row.getTimestamp(2)) &&
        java.util.Arrays.equals(expectedBytes, decodedBytes)
      case Left(err) =>
        fail(s"Round-trip failed: ${formatError(err)}")
        false
      }
    }
  }

  private def formatError(error: SparkToonError): String =
    s"${error.getClass.getSimpleName}: ${error.message}"

}
