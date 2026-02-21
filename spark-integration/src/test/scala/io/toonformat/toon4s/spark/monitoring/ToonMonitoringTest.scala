package io.toonformat.toon4s.spark.monitoring

import io.toonformat.toon4s.spark.ToonSparkOptions
import io.toonformat.toon4s.spark.testkit.SparkTestSuite

class ToonMonitoringTest extends SparkTestSuite {

  test("evaluateGuardrails strict mode blocks deep nesting") {
    val s = spark
    import s.implicits._

    val nested = Seq((1, Map("k" -> Map("x" -> Map("y" -> "z"))))).toDF("id", "payload")
    val decision = ToonMonitoring.evaluateGuardrails(
      nested,
      ToonMonitoring.GuardrailOptions(mode = ToonMonitoring.GuardrailMode.Strict),
    )

    assert(!decision.useToon)
    assert(decision.reason.contains("TOON blocked"))
  }

  test("encodeWithGuardrails falls back to json and emits warning") {
    val s = spark
    import s.implicits._

    val df = Seq((1, "a"), (2, "b")).toDF("id", "value")
    var warningText = ""

    val result = ToonMonitoring.encodeWithGuardrails(
      df = df,
      options = ToonSparkOptions(key = "users"),
      guardrailOptions = ToonMonitoring.GuardrailOptions(
        minBytesPerChunk = 1024 * 1024,
        maxRowsPerChunk = 1000,
        mode = ToonMonitoring.GuardrailMode.Strict,
      ),
      fallbackMode = ToonMonitoring.FallbackMode.Json,
      onWarning = msg => warningText = msg,
    )

    assert(result.isRight)
    result.foreach { value =>
      assertEquals(value.path, ToonMonitoring.EncodingPath.JsonFallback)
      assert(value.warning.nonEmpty)
      assertEquals(value.jsonFallbackRows, Vector.empty)
      assertEquals(value.counters.totalEncodes, 1L)
      assertEquals(value.counters.successfulEncodes, 0L)
      assertEquals(value.counters.failedEncodes, 1L)
      assert(value.counters.chunkCount > 0)
    }
    assert(warningText.nonEmpty)
  }

  test("encodeWithGuardrails returns toon chunks when allowed") {
    val s = spark
    import s.implicits._

    val df = (1 to 20).map(i => (i, s"user_$i")).toDF("id", "name")
    val result = ToonMonitoring.encodeWithGuardrails(
      df = df,
      options = ToonSparkOptions(key = "users"),
      guardrailOptions = ToonMonitoring.GuardrailOptions(
        minBytesPerChunk = 1,
        maxRowsPerChunk = 10,
        mode = ToonMonitoring.GuardrailMode.Lenient,
      ),
      fallbackMode = ToonMonitoring.FallbackMode.Skip,
    )

    assert(result.isRight)
    result.foreach { value =>
      assertEquals(value.path, ToonMonitoring.EncodingPath.Toon)
      assert(value.toonChunks.nonEmpty)
      assertEquals(value.counters.totalEncodes, 1L)
      assertEquals(value.counters.successfulEncodes, 1L)
      assertEquals(value.counters.failedEncodes, 0L)
      assert(value.counters.avgEstimatedTokensPerChunk > 0.0)
    }
  }

  test("encodeWithGuardrails enforces json fallback row safety limit") {
    val s = spark
    import s.implicits._

    val df = (1 to 5).map(i => (i, s"user_$i")).toDF("id", "name")

    val result = ToonMonitoring.encodeWithGuardrails(
      df = df,
      options = ToonSparkOptions(key = "users"),
      guardrailOptions = ToonMonitoring.GuardrailOptions(
        minBytesPerChunk = 1024 * 1024,
        maxRowsPerChunk = 1000,
        mode = ToonMonitoring.GuardrailMode.Strict,
        maxJsonFallbackRows = 2,
      ),
      fallbackMode = ToonMonitoring.FallbackMode.Json,
    )

    assert(result.isLeft)
    result.left.foreach(error =>
      assert(error.message.contains("safety limit"))
    )
  }

}
