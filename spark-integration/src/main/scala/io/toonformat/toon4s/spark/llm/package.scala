package io.toonformat.toon4s.spark

import scala.util.Try

/**
 * LLM integration types and abstractions.
 *
 * Design aligned with llm4s (https://github.com/llm4s/llm4s) for forward compatibility.
 *
 * ==Migration path==
 * When llm4s is published to Maven Central, users can:
 *   1. Add llm4s dependency
 *   2. Use Llm4sAdapter to bridge between toon4s-spark and llm4s
 *   3. Gradually migrate to llm4s-native conversation model
 *
 * ==Current status==
 * This package provides a simplified LLM client abstraction that mirrors llm4s design patterns but
 * works standalone. All types use similar naming and structure to llm4s for easy future migration.
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
