package io.toonformat.toon4s.spark

import scala.util.Try

/**
 * LLM integration helpers.
 *
 * Production path:
 *   - `DataFrame.writeToLlmPartitions(...)`
 *   - `LlmPartitionWriterFactory.fromClientFactory(...)`
 *   - llm4s client types (`org.llm4s.*`)
 *
 * Compatibility path:
 *   - The standalone `LlmClient`/`Message`/`Completion` helpers are legacy.
 *   - Use them only for migration tests. Do not use them in new production code.
 */
package object llm {

  /**
   * Result type alias matching llm4s pattern.
   *
   * Equivalent to llm4s's `type Result[+A] = Either[error.LLMError, A]`
   */
  type Result[+A] = Either[LlmError, A]

  /** Helper methods for Result type (matches llm4s Result companion object). */
  object Result {

    def success[A](value: A): Result[A] = Right(value)

    def failure[A](error: LlmError): Result[A] = Left(error)

    def sequence[A](results: Vector[Result[A]]): Result[Vector[A]] = {
      results.foldLeft[Result[Vector[A]]](Right(Vector.empty)) {
        case (Right(acc), Right(value)) => Right(acc :+ value)
        case (Left(err), _)             => Left(err)
        case (_, Left(err))             => Left(err)
      }
    }

    def safely[A](operation: => A): Result[A] = {
      Try(operation).toEither.left.map(ex => LlmError.UnknownError(ex.getMessage, Some(ex)))
    }

  }

}
