package examples

import io.toonformat.toon4s.spark.{SparkToonOps, ToonSparkOptions}
import org.apache.spark.sql.SparkSession
import SparkToonOps._

/**
 * Minimal batch example for first-time users.
 */
object TryIn5MinutesExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("toon4s-spark-try-in-5")
      .getOrCreate()

    try {
      import spark.implicits._

      val users = Seq(
        (1, "Alice", "US"),
        (2, "Bob", "IN"),
        (3, "Cara", "DE"),
      ).toDF("id", "name", "country")

      users.createOrReplaceTempView("users")
      val df = spark.sql("SELECT id, name, country FROM users ORDER BY id")

      df.toToon(ToonSparkOptions(
        key = "users",
        maxRowsPerChunk = 1000,
      )) match {
      case Right(chunks) =>
        println("First TOON chunk:")
        println(chunks.headOption.getOrElse("<no chunks>"))
      case Left(error) =>
        println(s"TOON encoding failed: ${error.message}")
      }
    } finally {
      spark.stop()
    }
  }

}
