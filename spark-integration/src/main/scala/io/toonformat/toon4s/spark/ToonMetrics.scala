package io.toonformat.toon4s.spark

/**
 * Token usage metrics comparing JSON vs TOON encoding.
 *
 * ==Design==
 * Immutable case class for token accounting. All fields are eagerly computed to enable pattern
 * matching and easy serialization.
 *
 * ==Usage==
 * {{{
 * import io.toonformat.toon4s.spark.SparkToonOps._
 *
 * val metrics: Either[SparkToonError, ToonMetrics] = df.toonMetrics()
 *
 * metrics.foreach { m =>
 *   println(s"JSON tokens: \${m.jsonTokenCount}")
 *   println(s"TOON tokens: \${m.toonTokenCount}")
 *   println(s"Savings: \${m.savingsPercent}%")
 *   println(s"Rows: \${m.rowCount}, Columns: \${m.columnCount}")
 * }
 * }}}
 *
 * @param jsonTokenCount
 *   Estimated token count for JSON encoding
 * @param toonTokenCount
 *   Estimated token count for TOON encoding
 * @param rowCount
 *   Number of rows in DataFrame
 * @param columnCount
 *   Number of columns in DataFrame
 */
final case class ToonMetrics(
    jsonTokenCount: Int,
    toonTokenCount: Int,
    rowCount: Int,
    columnCount: Int,
) {

  /**
   * Percentage reduction (positive = TOON saves tokens).
   *
   * Computed from token counts to ensure consistency.
   */
  def savingsPercent: Double =
    if (jsonTokenCount == 0) 0.0
    else ((jsonTokenCount - toonTokenCount).toDouble / jsonTokenCount.toDouble) * 100.0

  /**
   * Absolute token savings (JSON - TOON).
   *
   * Positive value indicates TOON uses fewer tokens.
   */
  def absoluteSavings: Int = jsonTokenCount - toonTokenCount

  /**
   * Compression ratio (TOON / JSON).
   *
   * Values < 1.0 indicate compression. Example: 0.5 means TOON is 50% the size of JSON.
   */
  def compressionRatio: Double = {
    if (jsonTokenCount == 0) 1.0
    else toonTokenCount.toDouble / jsonTokenCount.toDouble
  }

  /**
   * Estimated cost savings in USD (assuming 0.002 USD per 1K tokens for GPT-4).
   *
   * @param costPer1kTokens
   *   Cost in USD per 1000 tokens
   * @return
   *   Estimated USD savings
   */
  def estimatedCostSavings(costPer1kTokens: Double = 0.002): Double = {
    (absoluteSavings.toDouble / 1000.0) * costPer1kTokens
  }

  /** Human-readable summary string. */
  def summary: String = {
    f"""ToonMetrics(
       |  rows=$rowCount, columns=$columnCount
       |  JSON: $jsonTokenCount tokens
       |  TOON: $toonTokenCount tokens
       |  Savings: $savingsPercent%.1f%% ($absoluteSavings tokens)
       |  Compression ratio: $compressionRatio%.2f
       |)""".stripMargin
  }

  /**
   * Check if TOON encoding provides meaningful savings.
   *
   * @param threshold
   *   Minimum savings percentage to consider meaningful (default 10%)
   * @return
   *   true if savings exceed threshold
   */
  def hasMeaningfulSavings(threshold: Double = 10.0): Boolean = {
    savingsPercent >= threshold
  }

}

object ToonMetrics {

  /** Token estimation contract. */
  trait TokenEstimator extends Serializable {

    def estimate(text: String): Int

  }

  /**
   * Approximate estimator based on average characters per token.
   *
   * @param charsPerToken
   *   Approximate number of UTF-16 characters per token (must be > 0)
   * @param minTokensWhenNonEmpty
   *   Minimum token count for non-empty input
   */
  final case class CharsPerTokenEstimator(
      charsPerToken: Double = 4.0,
      minTokensWhenNonEmpty: Int = 1,
  ) extends TokenEstimator {

    require(charsPerToken > 0.0, "charsPerToken must be > 0")

    require(minTokensWhenNonEmpty >= 0, "minTokensWhenNonEmpty must be >= 0")

    override def estimate(text: String): Int = {
      if (text.isEmpty) 0
      else {
        val rough = math.round(text.length.toDouble / charsPerToken).toInt
        math.max(rough, minTokensWhenNonEmpty)
      }
    }

  }

  val defaultTokenEstimator: TokenEstimator = CharsPerTokenEstimator()

  /**
   * Token estimation strategy.
   *
   * Uses approximate GPT-style tokenization (4 characters per token) by default. For provider-
   * specific estimates, pass a custom estimator.
   */
  def estimateTokens(text: String): Int = {
    estimateTokens(text, defaultTokenEstimator)
  }

  /** Estimate token count with a caller-provided estimator. */
  def estimateTokens(text: String, estimator: TokenEstimator): Int = {
    estimator.estimate(text)
  }

  /** Estimate token count using custom characters-per-token ratio. */
  def estimateTokens(text: String, charsPerToken: Double): Int = {
    estimateTokens(text, CharsPerTokenEstimator(charsPerToken = charsPerToken))
  }

  /**
   * Compute metrics from two encoded strings.
   *
   * @param jsonEncoded
   *   JSON-encoded string
   * @param toonEncoded
   *   TOON-encoded string
   * @param rowCount
   *   Number of rows
   * @param columnCount
   *   Number of columns
   * @return
   *   ToonMetrics comparing the two encodings
   */
  def fromEncodedStrings(
      jsonEncoded: String,
      toonEncoded: String,
      rowCount: Int,
      columnCount: Int,
      tokenEstimator: TokenEstimator = defaultTokenEstimator,
  ): ToonMetrics = {
    val jsonTokens = estimateTokens(jsonEncoded, tokenEstimator)
    val toonTokens = estimateTokens(toonEncoded, tokenEstimator)
    ToonMetrics(
      jsonTokenCount = jsonTokens,
      toonTokenCount = toonTokens,
      rowCount = rowCount,
      columnCount = columnCount,
    )
  }

  /** Zero metrics (no data). */
  val empty: ToonMetrics = ToonMetrics(
    jsonTokenCount = 0,
    toonTokenCount = 0,
    rowCount = 0,
    columnCount = 0,
  )

  /**
   * Combine multiple metrics (e.g., from chunked encoding).
   *
   * @param metrics
   *   Vector of ToonMetrics to aggregate
   * @return
   *   Aggregated ToonMetrics
   */
  def aggregate(metrics: Vector[ToonMetrics]): ToonMetrics = {
    if (metrics.isEmpty) empty
    else {
      val totalJsonTokens = metrics.map(_.jsonTokenCount).sum
      val totalToonTokens = metrics.map(_.toonTokenCount).sum
      val totalRows = metrics.map(_.rowCount).sum
      val avgColumns = metrics.map(_.columnCount).sum / metrics.length

      ToonMetrics(
        jsonTokenCount = totalJsonTokens,
        toonTokenCount = totalToonTokens,
        rowCount = totalRows,
        columnCount = avgColumns,
      )
    }
  }

}
