package examples

import io.toonformat.toon4s.spark.{SparkToonOps, ToonSparkOptions}
import org.apache.spark.sql.SparkSession
import SparkToonOps._

/**
 * End-to-end example: query -> TOON -> LLM stub.
 */
object SparkLlmEndToEndExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("toon4s-spark-llm-e2e")
      .getOrCreate()

    try {
      import spark.implicits._

      val events = Seq(
        ("u1", "checkout", 129.5),
        ("u2", "refund", 35.0),
        ("u3", "checkout", 84.0),
      ).toDF("user_id", "event_type", "amount")

      val df = events.groupBy("event_type").sum("amount")

      df.toToon(ToonSparkOptions(key = "event_summary")) match {
      case Right(chunks) =>
        chunks.foreach { chunk =>
          val llmResult = callLLM(chunk)
          println(llmResult)
        }
      case Left(error) =>
        println(s"TOON encoding failed: ${error.message}")
      }
    } finally {
      spark.stop()
    }
  }

  private def callLLM(chunk: String): String =
    s"LLM summary for chunk: ${chunk.take(80)}..."

}
