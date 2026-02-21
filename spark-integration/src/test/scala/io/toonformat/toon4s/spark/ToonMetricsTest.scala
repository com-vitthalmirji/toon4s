package io.toonformat.toon4s.spark

import munit.FunSuite

class ToonMetricsTest extends FunSuite {

  test("estimateTokens: empty string") {
    val tokens = ToonMetrics.estimateTokens("")
    assertEquals(tokens, 0)
  }

  test("estimateTokens: short string") {
    val tokens = ToonMetrics.estimateTokens("test")
    assertEquals(tokens, 1) // 4 chars = 1 token
  }

  test("estimateTokens: longer string") {
    val tokens = ToonMetrics.estimateTokens("this is a test string")
    assertEquals(tokens, 5) // 21 chars = ~5 tokens
  }

  test("estimateTokens: exact multiple") {
    val tokens = ToonMetrics.estimateTokens("12345678") // 8 chars
    assertEquals(tokens, 2) // 8 / 4 = 2
  }

  test("estimateTokens: custom chars per token") {
    val tokens = ToonMetrics.estimateTokens("12345678", charsPerToken = 2.0)
    assertEquals(tokens, 4)
  }

  test("estimateTokens: custom estimator") {
    val estimator = ToonMetrics.CharsPerTokenEstimator(charsPerToken = 8.0)
    val tokens = ToonMetrics.estimateTokens("12345678", estimator)
    assertEquals(tokens, 1)
  }

  test("fromEncodedStrings: compute metrics") {
    val jsonEncoded = """{"id":1,"name":"Alice","age":25}"""
    val toonEncoded = """id\tname\tage\n1\tAlice\t25"""

    val metrics = ToonMetrics.fromEncodedStrings(
      jsonEncoded = jsonEncoded,
      toonEncoded = toonEncoded,
      rowCount = 1,
      columnCount = 3,
    )

    assert(metrics.jsonTokenCount > 0)
    assert(metrics.toonTokenCount > 0)
    assertEquals(metrics.rowCount, 1)
    assertEquals(metrics.columnCount, 3)
  }

  test("fromEncodedStrings: preserve raw token estimate without forced savings") {
    val jsonEncoded = "a" * 40 // 10 tokens
    val toonEncoded = "b" * 200 // 50 tokens

    val metrics = ToonMetrics.fromEncodedStrings(
      jsonEncoded = jsonEncoded,
      toonEncoded = toonEncoded,
      rowCount = 1,
      columnCount = 1,
    )

    assertEquals(metrics.jsonTokenCount, 10)
    assertEquals(metrics.toonTokenCount, 50)
    assert(metrics.savingsPercent < 0.0)
  }

  test("savingsPercent: calculate correctly") {
    val metrics = ToonMetrics(
      jsonTokenCount = 100,
      toonTokenCount = 60,
      rowCount = 10,
      columnCount = 5,
    )

    val expected = ((100 - 60).toDouble / 100) * 100 // 40%
    assertEquals(metrics.savingsPercent, expected, 0.001)
  }

  test("absoluteSavings: compute difference") {
    val metrics = ToonMetrics(
      jsonTokenCount = 100,
      toonTokenCount = 70,
      rowCount = 10,
      columnCount = 5,
    )

    assertEquals(metrics.absoluteSavings, 30)
  }

  test("compressionRatio: compute ratio") {
    val metrics = ToonMetrics(
      jsonTokenCount = 100,
      toonTokenCount = 50,
      rowCount = 10,
      columnCount = 5,
    )

    assertEquals(metrics.compressionRatio, 0.5, 0.001)
  }

  test("estimatedCostSavings: with default rate") {
    val metrics = ToonMetrics(
      jsonTokenCount = 10000,
      toonTokenCount = 6000,
      rowCount = 100,
      columnCount = 10,
    )

    val savings = metrics.estimatedCostSavings()
    assert(savings > 0.0)
    // 4000 token savings * (0.002 / 1000) = 0.008
    assertEquals(savings, 0.008, 0.0001)
  }

  test("estimatedCostSavings: with custom rate") {
    val metrics = ToonMetrics(
      jsonTokenCount = 10000,
      toonTokenCount = 6000,
      rowCount = 100,
      columnCount = 10,
    )

    val savings = metrics.estimatedCostSavings(costPer1kTokens = 0.01)
    // 4000 token savings * (0.01 / 1000) = 0.04
    assertEquals(savings, 0.04, 0.0001)
  }

  test("hasMeaningfulSavings: above threshold") {
    val metrics = ToonMetrics(
      jsonTokenCount = 100,
      toonTokenCount = 70,
      rowCount = 10,
      columnCount = 5,
    )

    assert(metrics.hasMeaningfulSavings(threshold = 10.0))
    assert(metrics.hasMeaningfulSavings(threshold = 25.0))
  }

  test("hasMeaningfulSavings: below threshold") {
    val metrics = ToonMetrics(
      jsonTokenCount = 100,
      toonTokenCount = 95,
      rowCount = 10,
      columnCount = 5,
    )

    assert(!metrics.hasMeaningfulSavings(threshold = 10.0))
    assert(metrics.hasMeaningfulSavings(threshold = 3.0))
  }

  test("summary: format correctly") {
    val metrics = ToonMetrics(
      jsonTokenCount = 1000,
      toonTokenCount = 600,
      rowCount = 50,
      columnCount = 8,
    )

    val summary = metrics.summary

    assert(summary.contains("1000"))
    assert(summary.contains("600"))
    assert(summary.contains("400"))
    assert(summary.contains("40.0%"))
    assert(summary.contains("50"))
    assert(summary.contains("8"))
  }

  test("zero tokens: handle edge case") {
    val metrics = ToonMetrics(
      jsonTokenCount = 0,
      toonTokenCount = 0,
      rowCount = 0,
      columnCount = 0,
    )

    assertEquals(metrics.absoluteSavings, 0)
    assertEquals(metrics.savingsPercent, 0.0)
    assertEquals(metrics.compressionRatio, 1.0) // We handle 0/0 as 1.0 in the def
  }

  test("no savings: TOON larger than JSON") {
    val metrics = ToonMetrics(
      jsonTokenCount = 100,
      toonTokenCount = 120,
      rowCount = 5,
      columnCount = 3,
    )

    assertEquals(metrics.absoluteSavings, -20)
    assert(!metrics.hasMeaningfulSavings())
    assert(metrics.compressionRatio > 1.0)
  }

}
