package io.toonformat.toon4s.spark

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

  private def formatError(error: SparkToonError): String =
    s"${error.getClass.getSimpleName}: ${error.message}"
}
