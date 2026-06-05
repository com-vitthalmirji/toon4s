package io.toonformat.toon4s.spark.bench

import io.toonformat.toon4s.spark.SparkToonOps._
import io.toonformat.toon4s.spark.ToonSparkOptions
import io.toonformat.toon4s.spark.testkit.SparkTestSuite
import org.apache.spark.sql.functions._

/**
 * Throughput baseline for the Spark encode path. Reports rows/sec and MB/sec for the executor level
 * encode (toToonDataset) on a uniform flat schema.
 *
 * This is a measurement tool, not a correctness test. It is skipped during normal runs and only
 * executes when TOON4S_BENCH=true is set in the environment (forked test JVMs inherit env vars),
 * for example:
 *
 * TOON4S_BENCH=true sbt "sparkIntegration/testOnly *EncodeThroughputBench"
 */
class EncodeThroughputBench extends SparkTestSuite {

  private val benchEnabled: Boolean =
    sys.env.get("TOON4S_BENCH").contains("true") ||
      sys.props.get("toon4s.bench").contains("true")

  // Single thread gives a stable, comparable baseline.
  override protected def sparkMaster: String = "local[1]"

  private val rowCounts: Seq[Int] = Seq(50000, 200000)

  private val chunkSize: Int = 5000

  private def buildDataset(rows: Int) =
    spark
      .range(rows.toLong)
      .select(
        col("id"),
        concat(lit("user_"), col("id")).as("name"),
        ((col("id") % lit(1000)) / lit(7.0)).as("score"),
        (col("id")  % lit(2) === lit(0)).as("active"),
      )

  private def timeEncode(rows: Int): (Double, Long) = {
    val df = buildDataset(rows).repartition(1)
    val options = ToonSparkOptions(key = "rows", maxRowsPerChunk = chunkSize)

    val start = System.nanoTime()
    val chunks = df.toToonDataset(options).take(1000000)
    val elapsedS = (System.nanoTime() - start).toDouble / 1.0e9
    val bytes = chunks.foldLeft(0L)((acc, chunk) => acc + chunk.length.toLong)
    (elapsedS, bytes)
  }

  test("encode throughput baseline") {
    assume(benchEnabled, "set TOON4S_BENCH=true to run the throughput benchmark")

    // Warm up the JIT and Spark codegen so the measured runs are steady state.
    timeEncode(rowCounts.head)

    println("=== Spark encode throughput baseline (toToonDataset, local[1]) ===")
    rowCounts.foreach { rows =>
      val (elapsedS, bytes) = timeEncode(rows)
      val rowsPerSec = rows.toDouble / elapsedS
      val mbPerSec = (bytes.toDouble / 1.0e6) / elapsedS
      println(
        f"rows=$rows%-8d time=$elapsedS%6.3fs " +
          f"rows/sec=$rowsPerSec%,12.0f MB/sec=$mbPerSec%7.2f bytes=$bytes%,d"
      )
    }
  }

}
