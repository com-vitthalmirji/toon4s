package io.toonformat.toon4s.spark.extensions

import scala.util.Using

import munit.FunSuite
import org.apache.spark.sql.SparkSession

class ToonSparkSessionExtensionsTest extends FunSuite {

  test("spark.sql.extensions auto-registers TOON UDFs") {
    val managedSpark = ManagedSparkSession(
      SparkSession
        .builder()
        .master("local[1]")
        .appName("ToonSparkSessionExtensionsTest")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config(
          "spark.sql.extensions",
          "io.toonformat.toon4s.spark.extensions.ToonSparkSessionExtensions",
        )
        .getOrCreate()
    )

    Using.resource(managedSpark) { managed =>
      val spark = managed.spark
      val result = spark.sql(
        """
          |SELECT
          |  toon_encode_string('Alice') AS toon_doc,
          |  toon_estimate_tokens('Alice') AS token_count
          |""".stripMargin
      )

      assertEquals(result.count(), 1L)
      val row = result.collect().head
      val toonDoc = row.getAs[String]("toon_doc")
      assert(toonDoc.nonEmpty)
      assert(toonDoc.contains("Alice"))
      assert(row.getAs[Int]("token_count") > 0)
    }
  }

}

final private case class ManagedSparkSession(spark: SparkSession) extends AutoCloseable {

  override def close(): Unit = spark.stop()

}
