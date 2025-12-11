package io.toonformat.toon4s.spark.llm

/**
 * LLM error types aligned with llm4s error hierarchy.
 *
 * ==Design Alignment with llm4s==
 * Mirrors llm4s's error system:
 *   - Sealed trait hierarchy with 23 error types
 *   - Marker traits for recoverability (`RecoverableError`, `NonRecoverableError`)
 *   - Rich metadata (provider, context, correlation ID)
 *   - Formatted error messages
 *
 * @see
 *   [[https://github.com/llm4s/llm4s llm4s error package]]
 */
sealed trait LlmError extends Product with Serializable {

  def message: String

  def code: Option[String] = None

  def context: Map[String, String] = Map.empty

  def correlationId: Option[String] = None

  def formatted: String = {
    val baseMsg = s"${getClass.getSimpleName}: $message"
    val codeStr = code.map(c => s" [code: $c]").getOrElse("")
    val contextStr = if (context.nonEmpty) {
      val ctx = context.map { case (k, v) => s"$k=$v" }.mkString(", ")
      s" (context: $ctx)"
    } else ""
    s"$baseMsg$codeStr$contextStr"
  }

}

// ========== Marker Traits (llm4s pattern) ==========

/** Errors that can potentially be recovered via retry. */
protected trait RecoverableError extends LlmError

/** Errors that cannot be recovered (require intervention). */
protected trait NonRecoverableError extends LlmError

// ========== Error Implementations ==========

object LlmError {

  /**
   * API error from LLM provider.
   *
   * Matches llm4s `APIError`.
   *
   * @param provider
   *   Provider name (e.g., "openai", "anthropic")
   * @param message
   *   Error message
   * @param statusCode
   *   Optional HTTP status code
   * @param context
   *   Additional context
   */
  final case class ApiError(
      provider: String,
      message: String,
      statusCode: Option[Int] = None,
      override val context: Map[String, String] = Map.empty,
  ) extends RecoverableError {

    override val code: Option[String] = statusCode.map(_.toString)

  }

  /**
   * Authentication/authorization error.
   *
   * Matches llm4s `AuthenticationError`.
   *
   * @param provider
   *   Provider name
   * @param message
   *   Error description
   */
  final case class AuthenticationError(
      provider: String,
      message: String,
  ) extends NonRecoverableError

  /**
   * Rate limit exceeded.
   *
   * Matches llm4s `RateLimitError`.
   *
   * @param provider
   *   Provider name
   * @param retryAfterSeconds
   *   Seconds until retry allowed
   */
  final case class RateLimitError(
      provider: String,
      retryAfterSeconds: Option[Int] = None,
  ) extends RecoverableError {

    def message: String = retryAfterSeconds match {
    case Some(secs) => s"Rate limit exceeded for $provider. Retry after $secs seconds."
    case None       => s"Rate limit exceeded for $provider."
    }

  }

  /**
   * Request timeout.
   *
   * Matches llm4s `TimeoutError`.
   *
   * @param message
   *   Timeout description
   * @param cause
   *   Optional underlying exception
   */
  final case class TimeoutError(
      message: String,
      cause: Option[Throwable] = None,
  ) extends RecoverableError

  /**
   * Network error (connection, DNS, etc.).
   *
   * Matches llm4s `NetworkError`.
   *
   * @param message
   *   Error description
   * @param cause
   *   Optional underlying exception
   * @param provider
   *   Provider name
   */
  final case class NetworkError(
      message: String,
      cause: Option[Throwable] = None,
      provider: String = "unknown",
  ) extends RecoverableError

  /**
   * Input validation error.
   *
   * Matches llm4s `ValidationError`.
   *
   * @param message
   *   Validation failure description
   * @param field
   *   Optional field name
   * @param violations
   *   List of specific violations
   */
  final case class ValidationError(
      message: String,
      field: Option[String] = None,
      violations: List[String] = List.empty,
  ) extends NonRecoverableError {

    override val context: Map[String, String] =
      field.map(f => Map("field" -> f)).getOrElse(Map.empty)

  }

  /**
   * Context window exceeded.
   *
   * Matches llm4s `ContextError`.
   *
   * @param message
   *   Error description
   * @param promptTokens
   *   Tokens in prompt
   * @param contextWindow
   *   Context window size
   */
  final case class ContextError(
      message: String,
      promptTokens: Int,
      contextWindow: Int,
  ) extends NonRecoverableError {

    override val context: Map[String, String] = Map(
      "promptTokens" -> promptTokens.toString,
      "contextWindow" -> contextWindow.toString,
    )

  }

  /**
   * Configuration error.
   *
   * Matches llm4s `ConfigurationError`.
   *
   * @param message
   *   Configuration issue description
   * @param key
   *   Optional configuration key
   */
  final case class ConfigurationError(
      message: String,
      key: Option[String] = None,
  ) extends NonRecoverableError

  /**
   * Service unavailable or degraded.
   *
   * Matches llm4s `ServiceError`.
   *
   * @param provider
   *   Provider name
   * @param message
   *   Error description
   */
  final case class ServiceError(
      provider: String,
      message: String,
  ) extends RecoverableError

  /**
   * Processing error during execution.
   *
   * Matches llm4s `ProcessingError`.
   *
   * @param message
   *   Error description
   * @param cause
   *   Optional underlying exception
   */
  final case class ProcessingError(
      message: String,
      cause: Option[Throwable] = None,
  ) extends NonRecoverableError

  /**
   * Unknown/unexpected error.
   *
   * Matches llm4s `UnknownError`.
   *
   * @param message
   *   Error description
   * @param cause
   *   Optional underlying exception
   */
  final case class UnknownError(
      message: String,
      cause: Option[Throwable] = None,
  ) extends RecoverableError

  // ========== Helper Methods ==========

  /** Check if error is recoverable via retry. */
  def isRecoverable(error: LlmError): Boolean = error.isInstanceOf[RecoverableError]

  /** Check if error is non-recoverable. */
  def isNonRecoverable(error: LlmError): Boolean = error.isInstanceOf[NonRecoverableError]

  /** Create error from exception. */
  def fromException(ex: Throwable): LlmError = ex match {
  case _: java.net.SocketTimeoutException =>
    TimeoutError(s"Request timed out: ${ex.getMessage}", Some(ex))
  case _: java.net.UnknownHostException =>
    NetworkError(s"Network error: ${ex.getMessage}", Some(ex))
  case _: java.io.IOException =>
    NetworkError(s"IO error: ${ex.getMessage}", Some(ex))
  case _ =>
    UnknownError(ex.getMessage, Some(ex))
  }

}

/**
 * Helper functions for LLM client implementations.
 *
 * Matches llm4s helper utilities.
 */
object LlmClientHelpers {

  /**
   * Retry logic with exponential backoff.
   *
   * Pure functional retry combinator. Respects error recoverability.
   *
   * @param maxAttempts
   *   Maximum retry attempts
   * @param baseDelayMillis
   *   Base delay for exponential backoff
   * @param operation
   *   Operation to retry
   * @return
   *   Either final error or success value
   */
  def retry[A](
      maxAttempts: Int,
      baseDelayMillis: Long = 1000,
  )(operation: => Either[LlmError, A]): Either[LlmError, A] = {

    @scala.annotation.tailrec
    def attempt(attemptsLeft: Int, lastError: Option[LlmError]): Either[LlmError, A] = {
      if (attemptsLeft <= 0) {
        Left(lastError.getOrElse(LlmError.UnknownError("Retry exhausted with no error")))
      } else {
        operation match {
        case Right(value) => Right(value)

        case Left(error: LlmError.RateLimitError) =>
          // Honor retry-after for rate limits
          error.retryAfterSeconds.foreach(secs => Thread.sleep(secs * 1000L))
          attempt(attemptsLeft - 1, Some(error))

        case Left(error) if LlmError.isRecoverable(error) =>
          // Retry recoverable errors with exponential backoff
          val delay = baseDelayMillis * (maxAttempts - attemptsLeft + 1)
          Thread.sleep(delay)
          attempt(attemptsLeft - 1, Some(error))

        case Left(error) =>
          // Don't retry non-recoverable errors
          Left(error)
        }
      }
    }

    attempt(maxAttempts, None)
  }

  /**
   * Validate prompt length against token limit.
   *
   * @param prompt
   *   Input prompt
   * @param maxTokens
   *   Maximum allowed tokens
   * @return
   *   Either validation error or unit
   */
  def validatePromptLength(prompt: String, maxTokens: Int): Either[LlmError, Unit] = {
    val estimatedTokens = prompt.length / 4 // Rough estimate
    if (estimatedTokens > maxTokens) {
      Left(
        LlmError.ValidationError(
          s"Prompt too long: ~$estimatedTokens tokens (max: $maxTokens)",
          field = Some("prompt"),
        )
      )
    } else {
      Right(())
    }
  }

  /**
   * Build prompt with system message and user content.
   *
   * @param systemPrompt
   *   System prompt
   * @param userContent
   *   User content (may include TOON data)
   * @return
   *   Formatted prompt
   */
  def buildPrompt(systemPrompt: String, userContent: String): String = {
    if (systemPrompt.isEmpty) userContent
    else s"$systemPrompt\n\n$userContent"
  }

}
