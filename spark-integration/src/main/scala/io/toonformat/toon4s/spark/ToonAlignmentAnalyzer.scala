package io.toonformat.toon4s.spark

import org.apache.spark.sql.types._

/**
 * Schema analyzer for TOON alignment detection.
 *
 * ==Design==
 * Based on TOON generation benchmark findings, this analyzer evaluates DataFrame schemas to predict
 * TOON generation success. The benchmark showed:
 *   - Aligned data (tabular, shallow): 90.5% accuracy, 22% token savings
 *   - Non-aligned data (deep hierarchies): 0% one-shot accuracy, massive token waste
 *
 * ==Usage==
 * {{{
 * import io.toonformat.toon4s.spark.ToonAlignmentAnalyzer._
 *
 * val df = spark.sql("SELECT * FROM users")
 * val alignment = analyzeSchema(df.schema)
 *
 * if (alignment.aligned) {
 *   df.toToon() // Safe to use TOON
 * } else {
 *   logger.warn(s"Schema not TOON-aligned: \${alignment.recommendation}")
 *   df.toJSON.take(1000) // Fall back to JSON with bounded sample
 * }
 * }}}
 *
 * ==Alignment criteria==
 * Based on benchmark results:
 *   - **users case** (90.5% acc): Flat tabular, 3 columns, no nesting
 *   - **order case** (78.6% acc): 1-2 levels nesting, uniform arrays
 *   - **invoice case** (52.4% acc): 2-3 levels, arrays with totals
 *   - **company case** (0% one-shot): Deep hierarchy (4+ levels), recursive structures
 */
object ToonAlignmentAnalyzer {

  /**
   * Alignment score and recommendations.
   *
   * @param score
   *   Alignment score from 0.0 (not aligned) to 1.0 (perfectly aligned)
   * @param aligned
   *   True if schema is TOON-aligned (score >= 0.7)
   * @param warnings
   *   List of potential issues that may reduce generation accuracy
   * @param recommendation
   *   Human-readable guidance on whether to use TOON
   * @param expectedAccuracy
   *   Estimated generation accuracy based on benchmark data
   * @param maxDepth
   *   Maximum nesting depth detected in schema
   */
  final case class AlignmentScore(
      score: Double,
      aligned: Boolean,
      warnings: List[String],
      recommendation: String,
      expectedAccuracy: String,
      maxDepth: Int,
  )

  /**
   * Analyze DataFrame schema for TOON alignment.
   *
   * Evaluates schema structure against benchmark-proven patterns. Returns detailed alignment score
   * with warnings and recommendations.
   *
   * @param schema
   *   Spark StructType to analyze
   * @return
   *   AlignmentScore with detailed analysis
   *
   * @example
   *   {{{
   * val schema = StructType(Seq(
   *   StructField("id", IntegerType),
   *   StructField("name", StringType),
   *   StructField("age", IntegerType)
   * ))
   *
   * val alignment = analyzeSchema(schema)
   * // AlignmentScore(score=1.0, aligned=true,
   * //   recommendation="✅ Schema is TOON-aligned. Expected accuracy: 85-95%")
   *   }}}
   */
  def analyzeSchema(schema: StructType): AlignmentScore = {
    val maxDepth = calculateMaxDepth(schema)
    val fieldCount = schema.fields.length
    val hasArrays = containsArrays(schema)
    val hasComplexNesting = hasDeepStructs(schema)
    val hasRecursivePotential = detectRecursivePatterns(schema)

    val score = calculateAlignmentScore(
      maxDepth,
      fieldCount,
      hasArrays,
      hasComplexNesting,
      hasRecursivePotential,
    )

    val aligned = score >= 0.7

    val warnings = buildWarnings(
      maxDepth,
      fieldCount,
      hasArrays,
      hasComplexNesting,
      hasRecursivePotential,
    )

    val recommendation = buildRecommendation(score, maxDepth)
    val expectedAccuracy = estimateAccuracy(score, maxDepth)

    AlignmentScore(
      score = score,
      aligned = aligned,
      warnings = warnings,
      recommendation = recommendation,
      expectedAccuracy = expectedAccuracy,
      maxDepth = maxDepth,
    )
  }

  /**
   * Calculate maximum nesting depth in schema.
   *
   * Depth calculation:
   *   - Flat fields (Int, String): depth 0
   *   - Struct: depth = 1 + max(child depths)
   *   - Array: depth = 1 + element depth
   *   - Map: depth = 1 + value depth
   *
   * @param dataType
   *   DataType to analyze
   * @param currentDepth
   *   Current depth (used for recursion)
   * @return
   *   Maximum nesting depth
   */
  private def calculateMaxDepth(dataType: DataType, currentDepth: Int = 0): Int = dataType match {
  case StructType(fields) =>
    if (fields.isEmpty) currentDepth
    else fields.map(f => calculateMaxDepth(f.dataType, currentDepth + 1)).max

  case ArrayType(elementType, _) =>
    calculateMaxDepth(elementType, currentDepth + 1)

  case MapType(_, valueType, _) =>
    calculateMaxDepth(valueType, currentDepth + 1)

  case _ => currentDepth
  }

  /**
   * Check if schema contains array types.
   *
   * Arrays can cause TOON generation issues:
   *   - Array count mismatches (`[N]` vs actual rows)
   *   - Non-uniform array elements
   *
   * @param schema
   *   Schema to analyze
   * @return
   *   True if arrays detected
   */
  private def containsArrays(schema: StructType): Boolean = {
    def hasArrayType(dataType: DataType): Boolean = dataType match {
    case ArrayType(_, _)    => true
    case StructType(fields) => fields.exists(f => hasArrayType(f.dataType))
    case MapType(_, v, _)   => hasArrayType(v)
    case _                  => false
    }

    schema.fields.exists(f => hasArrayType(f.dataType))
  }

  /**
   * Check if schema has deep struct nesting.
   *
   * Based on benchmark: company case (deep hierarchy) had 0% one-shot accuracy.
   *
   * @param schema
   *   Schema to analyze
   * @return
   *   True if nesting depth > 2
   */
  private def hasDeepStructs(schema: StructType): Boolean = {
    calculateMaxDepth(schema) > 2
  }

  /**
   * Detect potentially recursive patterns in schema.
   *
   * Recursive structures (e.g., org charts, file systems) are TOON's worst case. While we can't
   * detect true recursion (Spark doesn't allow it), we can detect patterns that suggest recursive
   * intent.
   *
   * @param schema
   *   Schema to analyze
   * @return
   *   True if recursive patterns detected
   */
  private def detectRecursivePatterns(schema: StructType): Boolean = {
    // Look for struct-in-array-in-struct patterns (common in hierarchies)
    def hasHierarchyPattern(dataType: DataType): Boolean = dataType match {
    case StructType(fields) =>
      fields.exists {
        case StructField(_, ArrayType(StructType(_), _), _, _) => true
        case StructField(_, dt, _, _)                          => hasHierarchyPattern(dt)
      }
    case _ => false
    }

    hasHierarchyPattern(schema)
  }

  /**
   * Calculate alignment score based on schema characteristics.
   *
   * Scoring logic based on benchmark results:
   *   - Flat tabular (depth 0-1): score = 1.0 (90.5% accuracy)
   *   - Shallow nesting (depth 2): score = 0.8 (78.6% accuracy)
   *   - Medium nesting (depth 3): score = 0.5 (52.4% accuracy)
   *   - Deep nesting (depth 4+): score = 0.0 (0% one-shot accuracy)
   *
   * @param maxDepth
   *   Maximum nesting depth
   * @param fieldCount
   *   Number of fields
   * @param hasArrays
   *   Contains arrays
   * @param hasComplexNesting
   *   Deep nesting detected
   * @param hasRecursivePotential
   *   Recursive patterns detected
   * @return
   *   Alignment score (0.0 - 1.0)
   */
  private def calculateAlignmentScore(
      maxDepth: Int,
      fieldCount: Int,
      hasArrays: Boolean,
      hasComplexNesting: Boolean,
      hasRecursivePotential: Boolean,
  ): Double = {
    var score = 1.0

    // Primary factor: nesting depth (benchmark-proven critical)
    if (maxDepth == 0) {
      score = 1.0 // Perfect (flat tabular like users case)
    } else if (maxDepth == 1) {
      score = 0.95 // Excellent (simple nesting)
    } else if (maxDepth == 2) {
      score = 0.8 // Good (like order case: 78.6% accuracy)
    } else if (maxDepth == 3) {
      score = 0.5 // Marginal (like invoice case: 52.4% accuracy)
    } else {
      score = 0.2 // Poor (like company case: 0% one-shot accuracy)
    }

    // Penalize excessive fields (can cause indentation drift in long outputs)
    if (fieldCount > 20) score -= 0.1
    if (fieldCount > 50) score -= 0.2

    // Penalize arrays (array count mismatches common)
    if (hasArrays) score -= 0.05

    // Severe penalty for complex nesting
    if (hasComplexNesting) score -= 0.2

    // Severe penalty for recursive patterns
    if (hasRecursivePotential) score -= 0.3

    math.max(0.0, math.min(1.0, score))
  }

  /**
   * Build list of warnings based on schema analysis.
   *
   * @return
   *   List of warning messages
   */
  private def buildWarnings(
      maxDepth: Int,
      fieldCount: Int,
      hasArrays: Boolean,
      hasComplexNesting: Boolean,
      hasRecursivePotential: Boolean,
  ): List[String] = {
    val warnings = List.newBuilder[String]

    if (maxDepth > 3) {
      warnings += s"Deep nesting detected ($maxDepth levels). Benchmark shows 0% one-shot accuracy for depth > 3."
    } else if (maxDepth > 2) {
      warnings += s"Medium nesting detected ($maxDepth levels). Expected accuracy: 50-70%."
    }

    if (hasComplexNesting) {
      warnings += "Complex nested structures detected. Consider flattening schema for better TOON generation."
    }

    if (hasRecursivePotential) {
      warnings += "Recursive hierarchy pattern detected. TOON is NOT production-ready for recursive structures."
    }

    if (hasArrays && maxDepth > 1) {
      warnings += "Arrays in nested structures can cause count mismatches. Validate array [N] counts carefully."
    }

    if (fieldCount > 50) {
      warnings += s"Large schema ($fieldCount fields). May cause indentation drift in long TOON outputs."
    }

    warnings.result()
  }

  /**
   * Build recommendation message based on alignment score.
   *
   * @param score
   *   Alignment score
   * @param maxDepth
   *   Maximum nesting depth
   * @return
   *   Recommendation string
   */
  private def buildRecommendation(score: Double, maxDepth: Int): String = {
    if (score >= 0.9) {
      "Schema is TOON-aligned. This matches benchmark's best-performing cases (users: 90.5% accuracy)."
    } else if (score >= 0.7) {
      "Schema is TOON-aligned. Expected good generation accuracy with minimal repairs."
    } else if (score >= 0.5) {
      "Schema is partially aligned. TOON may work but expect repair cycles. Consider flattening or use JSON."
    } else if (score >= 0.3) {
      "Schema is poorly aligned. TOON generation likely to fail. Strongly recommend JSON instead."
    } else {
      "Schema is NOT TOON-aligned. Use JSON. Deep hierarchies had 0% one-shot accuracy in benchmark."
    }
  }

  /**
   * Estimate expected generation accuracy based on alignment score.
   *
   * Based on benchmark data:
   *   - users (flat): 90.5% accuracy
   *   - order (shallow nesting): 78.6% accuracy
   *   - invoice (medium nesting): 52.4% accuracy
   *   - company (deep nesting): 0% one-shot, 48.6% final
   *
   * @param score
   *   Alignment score
   * @param maxDepth
   *   Nesting depth
   * @return
   *   Expected accuracy range
   */
  private def estimateAccuracy(score: Double, maxDepth: Int): String = {
    if (score >= 0.9) "85-95% (benchmark: users case 90.5%)"
    else if (score >= 0.7) "75-85% (benchmark: order case 78.6%)"
    else if (score >= 0.5) "50-75% (benchmark: invoice case 52.4%)"
    else if (score >= 0.3) "25-50% after repairs"
    else "0-25% (benchmark: company case 0% one-shot)"
  }

}
