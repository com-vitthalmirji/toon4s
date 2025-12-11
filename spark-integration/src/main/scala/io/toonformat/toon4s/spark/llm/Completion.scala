package io.toonformat.toon4s.spark.llm

/**
 * LLM completion response with metadata.
 *
 * ==Design Alignment with llm4s==
 * Mirrors llm4s's `org.llm4s.llmconnect.model.Completion`:
 *   - Comprehensive metadata (id, timestamps, model)
 *   - Token usage tracking
 *   - AssistantMessage for response
 *
 * @param id
 *   Completion ID from provider
 * @param created
 *   Timestamp (Unix epoch seconds)
 * @param content
 *   Response content
 * @param model
 *   Model identifier used for completion
 * @param message
 *   AssistantMessage representation
 * @param usage
 *   Optional token usage statistics
 */
final case class Completion(
    id: String,
    created: Long,
    content: String,
    model: String,
    message: AssistantMessage,
    usage: Option[TokenUsage] = None,
)

object Completion {

  /** Create completion from simple response (convenience method). */
  def fromResponse(
      content: String,
      model: String = "unknown",
      usage: Option[TokenUsage] = None,
  ): Completion = {
    Completion(
      id = java.util.UUID.randomUUID().toString,
      created = System.currentTimeMillis() / 1000,
      content = content,
      model = model,
      message = AssistantMessage(content),
      usage = usage,
    )
  }

}

/**
 * Token usage statistics for completion.
 *
 * ==Design Alignment with llm4s==
 * Matches llm4s's `org.llm4s.llmconnect.model.TokenUsage`:
 *   - Prompt tokens (input)
 *   - Completion tokens (output)
 *   - Total tokens
 *
 * @param promptTokens
 *   Tokens in the input prompt
 * @param completionTokens
 *   Tokens in the completion response
 * @param totalTokens
 *   Total tokens (prompt + completion)
 */
final case class TokenUsage(
    promptTokens: Int,
    completionTokens: Int,
    totalTokens: Int,
) {

  /**
   * Estimate cost based on per-token pricing.
   *
   * @param promptCostPer1k
   *   Cost per 1000 prompt tokens
   * @param completionCostPer1k
   *   Cost per 1000 completion tokens
   * @return
   *   Estimated cost in dollars
   */
  def estimateCost(
      promptCostPer1k: Double,
      completionCostPer1k: Double,
  ): Double = {
    val promptCost = (promptTokens / 1000.0) * promptCostPer1k
    val completionCost = (completionTokens / 1000.0) * completionCostPer1k
    promptCost + completionCost
  }

}

/**
 * Streaming chunk from LLM response.
 *
 * ==Design Alignment with llm4s==
 * Mirrors llm4s's `org.llm4s.llmconnect.model.StreamedChunk`:
 *   - Incremental content deltas
 *   - Finish reason tracking
 *
 * @param id
 *   Chunk ID (usually same as completion ID)
 * @param content
 *   Content delta for this chunk
 * @param finishReason
 *   Optional finish reason (e.g., "stop", "length")
 */
final case class StreamedChunk(
    id: String,
    content: Option[String],
    finishReason: Option[String] = None,
)

/**
 * Token budget for context management.
 *
 * ==Design Alignment with llm4s==
 * Mirrors llm4s's token budget calculation:
 *   - Context window management
 *   - Reserve tokens for completion
 *   - Headroom for safety
 *
 * @param available
 *   Tokens available for prompt
 * @param total
 *   Total context window size
 * @param reserved
 *   Tokens reserved for completion
 */
final case class TokenBudget(
    available: Int,
    total: Int,
    reserved: Int,
) {

  /** Check if prompt fits within budget. */
  def fits(promptTokens: Int): Boolean = promptTokens <= available

  /** Remaining tokens after prompt. */
  def remaining(promptTokens: Int): Int = (available - promptTokens).max(0)

}

/**
 * Headroom percentage for token budget safety margin.
 *
 * ==Design Alignment with llm4s==
 * Matches llm4s's `HeadroomPercent` enum.
 */
sealed trait HeadroomPercent {

  def value: Double

}

object HeadroomPercent {

  case object None extends HeadroomPercent { val value = 0.0 }

  case object Low extends HeadroomPercent { val value = 0.05 }

  case object Standard extends HeadroomPercent { val value = 0.10 }

  case object High extends HeadroomPercent { val value = 0.20 }

}
