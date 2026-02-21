package examples

import scala.jdk.CollectionConverters._
import scala.util.Try

import io.toonformat.toon4s.spark.{SparkToonOps, ToonMetrics, ToonSparkOptions}
import org.apache.spark.sql.{DataFrame, SparkSession}
import SparkToonOps._

/**
 * Measure JSON vs TOON behavior on one real Spark workload.
 *
 * Usage:
 *   spark-submit --class examples.WorkloadMeasurementExample <jar> --table my_table
 *   spark-submit --class examples.WorkloadMeasurementExample <jar> --parquet /data/events.parquet
 */
object WorkloadMeasurementExample {

  final case class CliConfig(
      table: Option[String] = None,
      parquet: Option[String] = None,
      csv: Option[String] = None,
      json: Option[String] = None,
      key: String = "workload",
      maxRowsPerChunk: Int = 1000,
  )

  final case class MeasurementResult(
      rowCount: Long,
      jsonBytes: Long,
      toonBytes: Long,
      jsonTokens: Int,
      toonTokens: Int,
      tokenSavingsPct: Double,
      bytesSavingsPct: Double,
      jsonEncodeMs: Long,
      toonEncodeMs: Long,
      overheadPctVsJson: Double,
      chunkCount: Int,
      avgChunkBytes: Double,
  )

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args.toVector).getOrElse {
      println(
        "Usage: --table <name> | --parquet <path> | --csv <path> | --json <path> [--key <name>] [--maxRowsPerChunk <n>]"
      )
      return
    }

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("toon4s-spark-workload-measurement")
      .getOrCreate()

    try {
      val sourceDf = loadSource(spark, config)
      val workloadDf = buildWorkload(sourceDf)

      val result = measure(workloadDf, config)
      printReport(config, result)
    } finally {
      spark.stop()
    }
  }

  private def loadSource(spark: SparkSession, config: CliConfig): DataFrame = {
    config.table match {
      case Some(name) => spark.table(name)
      case None =>
        config.parquet.orElse(config.csv).orElse(config.json) match {
          case Some(path) if config.parquet.nonEmpty =>
            spark.read.parquet(path)
          case Some(path) if config.csv.nonEmpty =>
            spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(path)
          case Some(path) =>
            spark.read.option("multiLine", "false").json(path)
          case None =>
            syntheticSource(spark)
        }
    }
  }

  private def syntheticSource(spark: SparkSession): DataFrame = {
    import spark.implicits._
    (1 to 20000)
      .map { i =>
        val region = if (i % 3 == 0) "us" else if (i % 3 == 1) "eu" else "apac"
        val channel = if (i % 2 == 0) "web" else "mobile"
        val amount = (i % 500).toDouble + 0.99
        (region, channel, amount)
      }
      .toDF("region", "channel", "amount")
  }

  private def buildWorkload(input: DataFrame): DataFrame = {
    val columns = input.schema.fields.toVector
    val names = columns.map(_.name)
    val hasCanonicalColumns = Set("region", "channel", "amount").subsetOf(names.toSet)

    if (hasCanonicalColumns) {
      input
        .groupBy("region", "channel")
        .sum("amount")
        .orderBy("region", "channel")
    } else {
      val dimensionColumns = columns
        .filter(f =>
          f.dataType.typeName == "string" ||
            f.dataType.typeName == "integer" ||
            f.dataType.typeName == "long"
        )
        .map(_.name)
        .take(2)
        .toList match {
        case Nil => names.take(1).toList
        case xs  => xs
      }

      val grouped = input.groupBy(dimensionColumns.head, dimensionColumns.drop(1): _*).count()
      grouped.orderBy(dimensionColumns.head, dimensionColumns.drop(1): _*)
    }
  }

  private def measure(df: DataFrame, config: CliConfig): MeasurementResult = {
    val options = ToonSparkOptions(
      key = config.key,
      maxRowsPerChunk = config.maxRowsPerChunk,
    )

    val jsonStart = System.currentTimeMillis()
    val jsonRows = df.toJSON.toLocalIterator().asScala.toVector
    val jsonEncodeMs = System.currentTimeMillis() - jsonStart
    val jsonBytes = jsonRows.map(_.getBytes("UTF-8").length.toLong).sum

    val toonStart = System.currentTimeMillis()
    val toonChunks = df.toToon(options).fold(
      err => throw new RuntimeException(err.message),
      identity,
    )
    val toonEncodeMs = System.currentTimeMillis() - toonStart
    val toonBytes = toonChunks.map(_.getBytes("UTF-8").length.toLong).sum

    val metrics = df.toonMetrics(options).fold(
      err => throw new RuntimeException(err.message),
      identity,
    )

    val rowCount = Try(df.count()).getOrElse(0L)
    val bytesSavingsPct = pct(jsonBytes.toDouble - toonBytes.toDouble, jsonBytes.toDouble)
    val overheadPctVsJson = pct(toonEncodeMs.toDouble - jsonEncodeMs.toDouble, jsonEncodeMs.toDouble)
    val avgChunkBytes = if (toonChunks.nonEmpty) toonBytes.toDouble / toonChunks.size.toDouble else 0.0

    MeasurementResult(
      rowCount = rowCount,
      jsonBytes = jsonBytes,
      toonBytes = toonBytes,
      jsonTokens = metrics.jsonTokenCount,
      toonTokens = metrics.toonTokenCount,
      tokenSavingsPct = metrics.savingsPercent,
      bytesSavingsPct = bytesSavingsPct,
      jsonEncodeMs = jsonEncodeMs,
      toonEncodeMs = toonEncodeMs,
      overheadPctVsJson = overheadPctVsJson,
      chunkCount = toonChunks.size,
      avgChunkBytes = avgChunkBytes,
    )
  }

  private def pct(delta: Double, base: Double): Double = {
    if (base <= 0.0) 0.0
    else (delta / base) * 100.0
  }

  private def printReport(config: CliConfig, result: MeasurementResult): Unit = {
    println("=== TOON workload measurement ===")
    println(s"key: ${config.key}")
    println(s"maxRowsPerChunk: ${config.maxRowsPerChunk}")
    println(s"rows: ${result.rowCount}")
    println(s"jsonBytes: ${result.jsonBytes}")
    println(s"toonBytes: ${result.toonBytes}")
    println(f"bytesSavingsPct: ${result.bytesSavingsPct}%.2f%%")
    println(s"jsonTokens: ${result.jsonTokens}")
    println(s"toonTokens: ${result.toonTokens}")
    println(f"tokenSavingsPct: ${result.tokenSavingsPct}%.2f%%")
    println(s"jsonEncodeMs: ${result.jsonEncodeMs}")
    println(s"toonEncodeMs: ${result.toonEncodeMs}")
    println(f"overheadPctVsJson: ${result.overheadPctVsJson}%.2f%%")
    println(s"chunkCount: ${result.chunkCount}")
    println(f"avgChunkBytes: ${result.avgChunkBytes}%.2f")
    println(
      s"estimatedCostDeltaUSD_per_1kTokens_at_0.002: ${estimateCostDelta(result.jsonTokens, result.toonTokens)}"
    )
  }

  private def estimateCostDelta(jsonTokens: Int, toonTokens: Int): Double = {
    val jsonCost = (jsonTokens.toDouble / 1000.0) * 0.002
    val toonCost = (toonTokens.toDouble / 1000.0) * 0.002
    toonCost - jsonCost
  }

  private def parseArgs(args: Vector[String]): Option[CliConfig] = {
    @annotation.tailrec
    def loop(rest: Vector[String], acc: CliConfig): Option[CliConfig] = {
      rest match {
        case Vector() => Some(acc)
        case Vector("--table", value, tail @ _*) =>
          loop(tail.toVector, acc.copy(table = Some(value)))
        case Vector("--parquet", value, tail @ _*) =>
          loop(tail.toVector, acc.copy(parquet = Some(value)))
        case Vector("--csv", value, tail @ _*) =>
          loop(tail.toVector, acc.copy(csv = Some(value)))
        case Vector("--json", value, tail @ _*) =>
          loop(tail.toVector, acc.copy(json = Some(value)))
        case Vector("--key", value, tail @ _*) =>
          loop(tail.toVector, acc.copy(key = value))
        case Vector("--maxRowsPerChunk", value, tail @ _*) =>
          Try(value.toInt).toOption.flatMap(v => loop(tail.toVector, acc.copy(maxRowsPerChunk = v)))
        case _ =>
          None
      }
    }

    loop(args, CliConfig())
  }

}
