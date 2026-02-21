package io.toonformat.toon4s.spark.llm

/**
 * Compatibility LLM client abstraction.
 *
 * This is kept for local tests and migration-only flows. For production Spark pipelines, use
 * `writeToLlmPartitions` with llm4s clients through `LlmPartitionWriterFactory`.
 */
trait LlmClient {

  // ========== Core llm4s-Compatible Methods ==========

  /**
   * Complete conversation with LLM.
   *
   * Primary method matching llm4s interface:
   * `def complete(conversation: Conversation, options: CompletionOptions): Result[Completion]`
   *
   * @param conversation
   *   Message sequence
   * @param options
   *   Completion configuration
   * @return
   *   Either LLM error or completion
   */
  def complete(
      conversation: Conversation,
      options: CompletionOptions = CompletionOptions(),
  ): Result[Completion]

  /**
   * Stream completion with callback.
   *
   * Matches llm4s streaming interface: `def streamComplete(conversation: Conversation, options:
   * CompletionOptions, onChunk: StreamedChunk => Unit): Result[Completion]`
   *
   * @param conversation
   *   Message sequence
   * @param options
   *   Completion configuration
   * @param onChunk
   *   Callback for each chunk
   * @return
   *   Either error or final completion
   */
  def streamComplete(
      conversation: Conversation,
      options: CompletionOptions = CompletionOptions(),
  )(onChunk: StreamedChunk => Unit): Result[Completion] = {
    // Default implementation: call complete and emit single chunk
    complete(conversation, options).map { completion =>
      onChunk(StreamedChunk(
        id = completion.id,
        content = Some(completion.content),
        finishReason = Some("stop"),
      ))
      completion
    }
  }

  // ========== Context Window Management (llm4s pattern) ==========

  /**
   * Get total context window size in tokens.
   *
   * Matches llm4s: `def getContextWindow(): Int`
   */
  def getContextWindow: Int

  /**
   * Get tokens reserved for completion.
   *
   * Matches llm4s: `def getReserveCompletion(): Int`
   */
  def getReserveCompletion: Int

  /**
   * Calculate available token budget for prompt.
   *
   * Matches llm4s: `def getContextBudget(headroom: HeadroomPercent): TokenBudget`
   *
   * Formula: (contextWindow - reserve) * (1 - headroom)
   */
  def getContextBudget(headroom: HeadroomPercent = HeadroomPercent.Standard): TokenBudget = {
    val total = getContextWindow
    val reserved = getReserveCompletion
    val available = ((total - reserved) * (1 - headroom.value)).toInt
    TokenBudget(available, total, reserved)
  }

  // ========== Lifecycle (llm4s pattern) ==========

  /**
   * Validate client configuration.
   *
   * Matches llm4s: `def validate(): Result[Unit]`
   */
  def validate(): Result[Unit] = Right(())

  /**
   * Close client and release resources.
   *
   * Matches llm4s: `def close(): Unit`
   */
  def close(): Unit = ()

  // ========== Backward-Compatible String-Based Methods ==========

  /**
   * Complete simple string prompt (convenience method).
   *
   * @param prompt
   *   User prompt
   * @return
   *   Either error or response string
   */
  final def completeSimple(prompt: String): Either[LlmError, String] = {
    Conversation.userOnly(prompt)
      .left.map(err => LlmError.ValidationError(err))
      .flatMap(conv => complete(conv).map(_.content))
  }

  /**
   * Complete with system and user prompts (convenience method).
   *
   * @param systemPrompt
   *   System instructions
   * @param userPrompt
   *   User input
   * @return
   *   Either error or response string
   */
  final def completeWithSystem(
      systemPrompt: String,
      userPrompt: String,
  ): Either[LlmError, String] = {
    Conversation.fromPrompts(systemPrompt, userPrompt)
      .left.map(err => LlmError.ValidationError(err))
      .flatMap(conv => complete(conv).map(_.content))
  }

  /** Get client configuration. */
  def config: LlmConfig

}

/**
 * LLM configuration aligned with llm4s CompletionOptions.
 *
 * ==Design alignment==
 * Extends original LlmConfig with fields matching llm4s's CompletionOptions:
 *   - temperature, maxTokens (already present)
 *   - topP, presencePenalty, frequencyPenalty (new, matching llm4s)
 *   - contextWindow, reserveCompletion (new, for token management)
 *
 * @param model
 *   Model identifier
 * @param maxTokens
 *   Maximum completion tokens
 * @param temperature
 *   Sampling temperature (0.0-1.0)
 * @param topP
 *   Nucleus sampling parameter
 * @param presencePenalty
 *   Presence penalty (-2.0 to 2.0)
 * @param frequencyPenalty
 *   Frequency penalty (-2.0 to 2.0)
 * @param contextWindow
 *   Total context window size
 * @param reserveCompletion
 *   Tokens to reserve for completion
 * @param systemPrompt
 *   Default system prompt
 * @param timeoutMillis
 *   Request timeout
 * @param retryAttempts
 *   Retry attempts on failure
 */
final case class LlmConfig(
    model: String,
    maxTokens: Int = 4096,
    temperature: Double = 0.7,
    topP: Double = 1.0,
    presencePenalty: Double = 0.0,
    frequencyPenalty: Double = 0.0,
    contextWindow: Int = 128000,
    reserveCompletion: Int = 4096,
    systemPrompt: String = "",
    timeoutMillis: Long = 30000,
    retryAttempts: Int = 3,
)

object LlmConfig {

  /** Default configuration for GPT-4o (128k context). */
  val default: LlmConfig = LlmConfig(
    model = "gpt-4o",
    maxTokens = 4096,
    temperature = 0.7,
    contextWindow = 128000,
    reserveCompletion = 4096,
  )

  /** Configuration for deterministic outputs (zero temperature). */
  val deterministic: LlmConfig = default.copy(temperature = 0.0)

  /** Configuration for creative outputs (higher temperature). */
  val creative: LlmConfig = default.copy(temperature = 0.9)

  /** Configuration for code generation. */
  val codeGeneration: LlmConfig = LlmConfig(
    model = "gpt-4o",
    maxTokens = 8192,
    temperature = 0.2,
    contextWindow = 128000,
    reserveCompletion = 8192,
    systemPrompt = "You are an expert programmer. Generate clean, idiomatic code.",
  )

  /** Configuration for data analysis (structured outputs). */
  val dataAnalysis: LlmConfig = LlmConfig(
    model = "gpt-4o",
    maxTokens = 4096,
    temperature = 0.3,
    contextWindow = 128000,
    reserveCompletion = 4096,
    systemPrompt = "You are a data analyst. Provide structured, factual analysis.",
  )

  /** Configuration for Claude 3.5 Sonnet (200k context). */
  val claude35Sonnet: LlmConfig = LlmConfig(
    model = "claude-3-5-sonnet-20241022",
    maxTokens = 8192,
    temperature = 0.7,
    contextWindow = 200000,
    reserveCompletion = 8192,
  )

}

/**
 * Completion options for individual requests.
 *
 * ==Design alignment with llm4s==
 * Mirrors llm4s's `CompletionOptions` case class with fluent builders.
 *
 * @param temperature
 *   Sampling temperature
 * @param topP
 *   Nucleus sampling
 * @param maxTokens
 *   Maximum completion tokens
 * @param presencePenalty
 *   Presence penalty
 * @param frequencyPenalty
 *   Frequency penalty
 */
final case class CompletionOptions(
    temperature: Double = 0.7,
    topP: Double = 1.0,
    maxTokens: Option[Int] = None,
    presencePenalty: Double = 0.0,
    frequencyPenalty: Double = 0.0,
) {

  /** Create options from LlmConfig. */
  def withConfig(config: LlmConfig): CompletionOptions = copy(
    temperature = config.temperature,
    topP = config.topP,
    maxTokens = Some(config.maxTokens),
    presencePenalty = config.presencePenalty,
    frequencyPenalty = config.frequencyPenalty,
  )

}

object CompletionOptions {

  /** Default options. */
  val default: CompletionOptions = CompletionOptions()

  /** Deterministic completion (temperature = 0). */
  val deterministic: CompletionOptions = default.copy(temperature = 0.0)

  /** Creative completion (temperature = 0.9). */
  val creative: CompletionOptions = default.copy(temperature = 0.9)

  /** Create from LlmConfig. */
  def fromConfig(config: LlmConfig): CompletionOptions = CompletionOptions(
    temperature = config.temperature,
    topP = config.topP,
    maxTokens = Some(config.maxTokens),
    presencePenalty = config.presencePenalty,
    frequencyPenalty = config.frequencyPenalty,
  )

}
