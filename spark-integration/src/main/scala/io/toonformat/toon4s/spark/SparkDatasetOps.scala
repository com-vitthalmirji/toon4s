package io.toonformat.toon4s.spark

import io.toonformat.toon4s.{DecodeOptions, EncodeOptions}
import io.toonformat.toon4s.spark.error.SparkToonError
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

/**
 * Extension methods for type-safe Dataset[T] <-> TOON conversion.
 *
 * ==Design==
 * Provides compile-time type-safe TOON encoding/decoding for Spark Datasets. Leverages Spark's
 * Encoder mechanism for seamless conversion between typed Datasets and TOON format.
 *
 * ==Benefits over DataFrame==
 *   - Compile-time type safety - catches errors before runtime
 *   - Automatic schema derivation from case classes
 *   - IntelliJ/IDE autocomplete for Dataset operations
 *   - No runtime reflection for type information
 *
 * ==Usage==
 * {{{
 * import io.toonformat.toon4s.spark.SparkDatasetOps._
 *
 * case class User(id: Int, name: String, email: String)
 *
 * val ds: Dataset[User] = spark.read.json("users.json").as[User]
 *
 * // Encode typed Dataset to TOON
 * ds.toToon() match {
 *   case Right(toonChunks) =>
 *     toonChunks.foreach(chunk => sendToLLM(chunk))
 *   case Left(error) =>
 *     logger.error(s"Encoding failed: \${error.message}")
 * }
 *
 * // Get token metrics for cost estimation
 * ds.toonMetrics() match {
 *   case Right(metrics) =>
 *     println(s"Savings: \${metrics.savingsPercent}%")
 *   case Left(error) =>
 *     println(s"Error: \${error.message}")
 * }
 *
 * // Decode back to typed Dataset
 * val decoded: Either[SparkToonError, Dataset[User]] =
 *   SparkDatasetOps.fromToon[User](toonChunks)
 * }}}
 *
 * ==Key design pattern==
 * Dataset[T] provides compile-time type safety while delegating to DataFrame operations internally.
 * This ensures we reuse the battle-tested DataFrame↔TOON logic while providing a type-safe facade.
 *
 * @see
 *   [[https://spark.apache.org/docs/latest/sql-programming-guide.html#datasets-and-dataframes Spark Datasets guide]]
 * @see
 *   [[https://www.chaosgenius.io/blog/apache-spark-with-scala/ Apache Spark With Scala 101]]
 */
object SparkDatasetOps {

  /**
   * Extension methods for type-safe Dataset[T].
   *
   * Provides TOON encoding with compile-time type safety via Spark's Encoder mechanism.
   */
  implicit class DatasetToonOps[T](ds: Dataset[T])(implicit encoder: Encoder[T]) {

    /**
     * Encode Dataset[T] to TOON format.
     *
     * Delegates to DataFrame.toToon() internally but maintains type information for the Dataset.
     * Uses the case class type name as the default key.
     *
     * @param key
     *   Top-level key for TOON document (default: type name of T)
     * @param maxRowsPerChunk
     *   Maximum rows per TOON chunk (default: 1000)
     * @param options
     *   TOON encoding options
     * @return
     *   Either error or Vector of TOON-encoded strings
     *
     * @example
     *   {{{
     * case class Product(id: Int, name: String, price: Double)
     * val ds: Dataset[Product] = ...
     *
     * ds.toToon() match {  // Uses "Product" as key automatically
     *   case Right(toon) => println(toon.head)
     *   case Left(err) => logger.error(err.message)
     * }
     *   }}}
     */
    def toToon(
        key: String = inferKeyName[T],
        maxRowsPerChunk: Int = 1000,
        options: EncodeOptions = EncodeOptions(),
    ): Either[SparkToonError, Vector[String]] = {
      // Delegate to DataFrame implementation
      import SparkToonOps._
      ds.toDF().toToon(key, maxRowsPerChunk, options)
    }

    /**
     * Compute token metrics for Dataset[T].
     *
     * Compares JSON vs TOON token usage for the typed Dataset, useful for cost estimation before
     * sending to LLMs.
     *
     * @param key
     *   Top-level key for TOON document (default: type name of T)
     * @param options
     *   TOON encoding options
     * @return
     *   Either error or ToonMetrics with token counts and savings
     *
     * @example
     *   {{{
     * ds.toonMetrics() match {
     *   case Right(metrics) =>
     *     if (metrics.hasMeaningfulSavings(threshold = 20.0)) {
     *       println(s"TOON saves \${metrics.absoluteSavings} tokens!")
     *     }
     *   case Left(err) => println(s"Error: \${err.message}")
     * }
     *   }}}
     */
    def toonMetrics(
        key: String = inferKeyName[T],
        options: EncodeOptions = EncodeOptions(),
    ): Either[SparkToonError, ToonMetrics] = {
      import SparkToonOps._
      ds.toDF().toonMetrics(key, options)
    }

    /**
     * Show TOON sample for debugging (convenience method).
     *
     * Encodes a sample of the Dataset and prints the TOON output. Useful for quick inspection of
     * TOON format during development.
     *
     * @param n
     *   Number of rows to sample (default 5)
     */
    def showToonSample(n: Int = 5): Unit = {
      import SparkToonOps._
      ds.toDF().showToonSample(n)
    }

  }

  /**
   * Decode TOON documents back to type-safe Dataset[T].
   *
   * Reconstructs a typed Dataset from TOON-encoded strings. Requires the schema to be inferred from
   * the type parameter T (via implicit Encoder).
   *
   * @tparam T
   *   Case class type for the Dataset
   * @param toonDocuments
   *   Vector of TOON-encoded strings (e.g., from chunked encoding)
   * @param options
   *   TOON decoding options
   * @param spark
   *   Implicit SparkSession for Dataset creation
   * @param encoder
   *   Implicit Encoder for type T (auto-derived for case classes)
   * @return
   *   Either error or typed Dataset[T]
   *
   * @example
   *   {{{
   * case class User(id: Int, name: String, email: String)
   *
   * val toonChunks: Vector[String] = ...
   *
   * SparkDatasetOps.fromToon[User](toonChunks) match {
   *   case Right(ds: Dataset[User]) =>
   *     ds.show()
   *     ds.filter(_.id > 100).collect()
   *   case Left(error) =>
   *     logger.error(s"Decoding failed: \${error.message}")
   * }
   *   }}}
   */
  def fromToon[T: Encoder](
      toonDocuments: Vector[String],
      options: DecodeOptions = DecodeOptions(),
  )(implicit spark: SparkSession): Either[SparkToonError, Dataset[T]] = {
    // Get schema from Encoder
    val encoder = implicitly[Encoder[T]]
    val schema = encoder.schema

    // Decode to DataFrame first
    SparkToonOps.fromToon(toonDocuments, schema, options).map { df =>
      // Convert DataFrame to typed Dataset[T]
      import spark.implicits._
      df.as[T](encoder)
    }
  }

  /**
   * Infer key name from type T.
   *
   * Uses Scala reflection to extract the simple class name from the type parameter. Falls back to
   * "data" if reflection fails.
   *
   * @tparam T
   *   Type to infer name from
   * @return
   *   Simple class name or "data"
   */
  private def inferKeyName[T](implicit encoder: Encoder[T]): String = {
    // Extract type name from encoder schema
    val schema = encoder.schema
    schema.catalogString match {
    case s if s.startsWith("struct<") => "data" // Fallback for complex types
    case _                            =>
      // Try to extract from encoder's class name
      scala.util
        .Try {
          val className = encoder.getClass.getName
          className.split('.').last.split('$').head
        }
        .getOrElse("data")
    }
  }

}
