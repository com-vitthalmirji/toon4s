package examples

import scala.util.Try

import io.toonformat.toon4s.spark.{SparkToonOps, ToonMetrics, ToonSparkOptions}
import io.toonformat.toon4s.spark.SparkToonOps._
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, NumericType, StringType, StructField, TimestampType}

object WorkloadMeasurementRunner {

  final case class PayloadStats(
      bytes: Long,
      tokens: Long,
      records: Long,
  )

  final case class CliConfig(
      parquet: Option[String] = None,
      json: Option[String] = None,
      csv: Option[String] = None,
      mode: String = "agg",
      key: String = "workload",
      maxRowsPerChunk: Int = 1000,
  )

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args.toVector).getOrElse {
      println(
        "Usage: --parquet <path> | --json <path> | --csv <path> [--mode agg|raw] [--key <name>] [--maxRowsPerChunk <n>]"
      )
      return
    }

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("toon4s-spark-workload-measurement")
      .getOrCreate()

    try {
      val sourceDf = loadSource(spark, config)
      val workloadDf = if (config.mode == "raw") sourceDf else buildWorkload(sourceDf)
      val options = ToonSparkOptions(config.key, config.maxRowsPerChunk)

      val jsonStart = System.currentTimeMillis()
      val jsonStats = summarizePayload(workloadDf.toJSON)
      val jsonEncodeMs = System.currentTimeMillis() - jsonStart

      val toonStart = System.currentTimeMillis()
      val toonDataset = workloadDf.toToonDataset(options)
      val toonStats = summarizePayload(toonDataset)
      val toonEncodeMs = System.currentTimeMillis() - toonStart

      val rowCount = jsonStats.records
      val bytesSavingsPct =
        pct(jsonStats.bytes.toDouble - toonStats.bytes.toDouble, jsonStats.bytes.toDouble)
      val tokenSavingsPct =
        pct(jsonStats.tokens.toDouble - toonStats.tokens.toDouble, jsonStats.tokens.toDouble)
      val overheadPctVsJson =
        pct(toonEncodeMs.toDouble - jsonEncodeMs.toDouble, jsonEncodeMs.toDouble)
      val avgChunkBytes =
        if (toonStats.records > 0) toonStats.bytes.toDouble / toonStats.records.toDouble else 0.0

      println("=== TOON workload measurement ===")
      println(s"rows: $rowCount")
      println(s"jsonBytes: ${jsonStats.bytes}")
      println(s"toonBytes: ${toonStats.bytes}")
      println(f"bytesSavingsPct: $bytesSavingsPct%.2f%%")
      println(s"jsonTokens: ${jsonStats.tokens}")
      println(s"toonTokens: ${toonStats.tokens}")
      println(f"tokenSavingsPct: $tokenSavingsPct%.2f%%")
      println(s"jsonEncodeMs: $jsonEncodeMs")
      println(s"toonEncodeMs: $toonEncodeMs")
      println(f"overheadPctVsJson: $overheadPctVsJson%.2f%%")
      println(s"chunkCount: ${toonStats.records}")
      println(f"avgChunkBytes: $avgChunkBytes%.2f")
    } finally {
      spark.stop()
    }
  }

  private def loadSource(spark: SparkSession, config: CliConfig): DataFrame = {
    config.parquet.orElse(config.json).orElse(config.csv) match {
    case Some(path) if config.parquet.nonEmpty =>
      spark.read.parquet(path)
    case Some(path) =>
      if (config.json.nonEmpty) spark.read.option("multiLine", "true").json(path)
      else
        spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(path)
    case None =>
      throw new IllegalArgumentException("Provide --parquet, --json, or --csv")
    }
  }

  private def buildWorkload(input: DataFrame): DataFrame = {
    val fields = input.schema.fields.toVector
    val metricColumn = fields.collectFirst { case StructField(name, _: NumericType, _, _) => name }
      .orElse(fields.headOption.map(_.name))
      .getOrElse(throw new IllegalArgumentException("Input DataFrame has no columns"))

    val dateOrTimestamp = fields.collectFirst {
      case StructField(name, DateType, _, _)      => col(name).as("group_key")
      case StructField(name, TimestampType, _, _) => to_date(col(name)).as("group_key")
    }

    val stringBucket = fields.collectFirst {
      case StructField(name, StringType, _, _) =>
        lower(substring(trim(col(name)), 1, 2)).as("group_key")
    }

    val numericBucket = fields.collectFirst {
      case StructField(name, _: NumericType, _, _) =>
        floor(col(name) / lit(100)).cast("long").as("group_key")
    }

    val groupExpr = dateOrTimestamp.orElse(
      stringBucket
    ).orElse(numericBucket).getOrElse(lit("all").as("group_key"))

    input
      .select(groupExpr, col(metricColumn).cast("double").as("metric_value"))
      .groupBy(col("group_key"))
      .agg(
        count(lit(1)).as("row_count"),
        sum(col("metric_value")).as("metric_sum"),
        avg(col("metric_value")).as("metric_avg"),
      )
      .orderBy(col("group_key"))
  }

  private def pct(delta: Double, base: Double): Double =
    if (base <= 0.0) 0.0 else (delta / base) * 100.0

  private def summarizePayload(ds: Dataset[String]): PayloadStats = {
    val partitionStats = ds.mapPartitions { values =>
      var bytes = 0L
      var tokens = 0L
      var records = 0L
      values.foreach { value =>
        bytes += value.getBytes("UTF-8").length.toLong
        tokens += ToonMetrics.estimateTokens(value).toLong
        records += 1L
      }
      Iterator.single((bytes, tokens, records))
    }(Encoders.tuple(Encoders.scalaLong, Encoders.scalaLong, Encoders.scalaLong))

    val totals = partitionStats
      .toDF("bytes", "tokens", "records")
      .agg(
        coalesce(sum(col("bytes")), lit(0L)).cast("long").as("bytes"),
        coalesce(sum(col("tokens")), lit(0L)).cast("long").as("tokens"),
        coalesce(sum(col("records")), lit(0L)).cast("long").as("records"),
      )
      .collect()
      .head

    PayloadStats(
      bytes = totals.getLong(0),
      tokens = totals.getLong(1),
      records = totals.getLong(2),
    )
  }

  private def parseArgs(args: Vector[String]): Option[CliConfig] = {
    def loop(rest: Vector[String], acc: CliConfig): Option[CliConfig] = {
      rest match {
      case Vector()                              => Some(acc)
      case Vector("--parquet", value, tail @ _*) =>
        loop(tail.toVector, acc.copy(parquet = Some(value)))
      case Vector("--json", value, tail @ _*) =>
        loop(tail.toVector, acc.copy(json = Some(value)))
      case Vector("--csv", value, tail @ _*) =>
        loop(tail.toVector, acc.copy(csv = Some(value)))
      case Vector("--key", value, tail @ _*) =>
        loop(tail.toVector, acc.copy(key = value))
      case Vector("--mode", value, tail @ _*) if value == "agg" || value == "raw" =>
        loop(tail.toVector, acc.copy(mode = value))
      case Vector("--maxRowsPerChunk", value, tail @ _*) =>
        Try(value.toInt).toOption.flatMap(v => loop(tail.toVector, acc.copy(maxRowsPerChunk = v)))
      case _ =>
        None
      }
    }
    loop(args, CliConfig())
  }

}
