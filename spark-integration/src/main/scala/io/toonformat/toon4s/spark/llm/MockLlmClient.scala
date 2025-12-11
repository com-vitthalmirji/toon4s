package io.toonformat.toon4s.spark.llm

/**
 * Mock LLM client for testing.
 *
 * Returns predefined responses for testing without making actual API calls. Aligned with llm4s
 * testing patterns.
 *
 * @param config
 *   Client configuration
 * @param responses
 *   Map of conversation keys to response strings
 */
class MockLlmClient(
    val config: LlmConfig,
    responses: Map[String, String] = Map.empty,
) extends LlmClient {

  /** Complete conversation with mock response. */
  def complete(
      conversation: Conversation,
      options: CompletionOptions = CompletionOptions(),
  ): Result[Completion] = {
    // Create key from last user message
    val key = conversation.messages.lastOption match {
    case Some(UserMessage(content)) => content
    case Some(msg)                  => msg.content
    case None                       => ""
    }

    val responseContent = responses.getOrElse(
      key,
      s"Mock response for: ${key.take(50)}...",
    )

    Right(Completion.fromResponse(
      content = responseContent,
      model = config.model,
      usage = Some(TokenUsage(
        promptTokens = key.length / 4,
        completionTokens = responseContent.length / 4,
        totalTokens = (key.length + responseContent.length) / 4,
      )),
    ))
  }

  /** Stream mock completion. */
  override def streamComplete(
      conversation: Conversation,
      options: CompletionOptions = CompletionOptions(),
  )(onChunk: StreamedChunk => Unit): Result[Completion] = {
    complete(conversation, options).map { completion =>
      // Emit chunks for each word
      val words = completion.content.split("\\s+")
      words.foreach { word =>
        onChunk(StreamedChunk(
          id = completion.id,
          content = Some(word + " "),
          finishReason = None,
        ))
      }
      // Emit final chunk
      onChunk(StreamedChunk(
        id = completion.id,
        content = None,
        finishReason = Some("stop"),
      ))
      completion
    }
  }

  def getContextWindow(): Int = config.contextWindow

  def getReserveCompletion(): Int = config.reserveCompletion

}

object MockLlmClient {

  /** Create mock client with default config. */
  def apply(responses: Map[String, String] = Map.empty): MockLlmClient = {
    new MockLlmClient(LlmConfig.default, responses)
  }

  /** Create mock client that always succeeds. */
  val alwaysSucceeds: MockLlmClient = new MockLlmClient(
    LlmConfig.default,
    Map.empty,
  )

  /** Create mock client that always fails. */
  val alwaysFails: LlmClient = new LlmClient {
    val config: LlmConfig = LlmConfig.default

    def complete(
        conversation: Conversation,
        options: CompletionOptions = CompletionOptions(),
    ): Result[Completion] = {
      Left(LlmError.ApiError("mock", "Mock failure", Some(500)))
    }

    def getContextWindow(): Int = config.contextWindow
    def getReserveCompletion(): Int = config.reserveCompletion
  }

  /** Create mock client with specific config. */
  def withConfig(config: LlmConfig, responses: Map[String, String] = Map.empty): MockLlmClient = {
    new MockLlmClient(config, responses)
  }

}
