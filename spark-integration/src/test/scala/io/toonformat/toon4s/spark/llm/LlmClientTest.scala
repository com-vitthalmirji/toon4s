package io.toonformat.toon4s.spark.llm

import munit.FunSuite

class LlmClientTest extends FunSuite {

  test("Message: create system message") {
    val result = Message.system("You are a helpful assistant")
    assert(result.isRight)
    result.foreach { msg =>
      assertEquals(msg.role, MessageRole.System)
      assertEquals(msg.content, "You are a helpful assistant")
    }
  }

  test("Message: reject empty system message") {
    val result = Message.system("")
    assert(result.isLeft)
  }

  test("Conversation: create from prompts") {
    val result = Conversation.fromPrompts(
      "You are a data analyst",
      "Analyze this data",
    )
    assert(result.isRight)
    result.foreach { conv =>
      assertEquals(conv.messages.size, 2)
      assertEquals(conv.messages(0).role, MessageRole.System)
      assertEquals(conv.messages(1).role, MessageRole.User)
    }
  }

  test("Conversation: user only") {
    val result = Conversation.userOnly("Hello")
    assert(result.isRight)
    result.foreach { conv =>
      assertEquals(conv.messages.size, 1)
      assertEquals(conv.messages.head.role, MessageRole.User)
    }
  }

  test("MockLlmClient: complete simple conversation") {
    val client = MockLlmClient(Map("Hello" -> "Hi there!"))

    val result = for {
      conv <- Conversation.userOnly("Hello")
      completion <- client.complete(conv)
    } yield completion

    assert(result.isRight)
    result.foreach { completion =>
      assertEquals(completion.content, "Hi there!")
      assert(completion.usage.isDefined)
    }
  }

  test("MockLlmClient: complete with system prompt") {
    val client = MockLlmClient.alwaysSucceeds

    val result = for {
      conv <- Conversation.fromPrompts("You are helpful", "Help me")
      completion <- client.complete(conv)
    } yield completion

    assert(result.isRight)
    result.foreach(completion => assert(completion.content.contains("Mock response")))
  }

  test("MockLlmClient: stream completion") {
    val client = MockLlmClient(Map("test" -> "Hello world"))
    var chunks: List[StreamedChunk] = List.empty

    val result = for {
      conv <- Conversation.userOnly("test")
      completion <- client.streamComplete(conv) { chunk => chunks = chunks :+ chunk }
    } yield completion

    assert(result.isRight)
    assert(chunks.nonEmpty)
    assert(chunks.last.finishReason.contains("stop"))
  }

  test("MockLlmClient: context window management") {
    val client = MockLlmClient.alwaysSucceeds

    assertEquals(client.getContextWindow, 128000)
    assertEquals(client.getReserveCompletion, 4096)

    val budget = client.getContextBudget()
    assert(budget.available > 0)
    assert(budget.available < client.getContextWindow)
  }

  test("MockLlmClient: backward compatible string API") {
    val client = MockLlmClient(Map("test" -> "response"))

    val result = client.completeSimple("test")
    assert(result.isRight)
    assertEquals(result.getOrElse(""), "response")
  }

  test("MockLlmClient: alwaysFails") {
    val client = MockLlmClient.alwaysFails

    val result = for {
      conv <- Conversation.userOnly("test")
      completion <- client.complete(conv)
    } yield completion

    assert(result.isLeft)
  }

  test("LlmConfig: presets") {
    assertEquals(LlmConfig.default.model, "gpt-4o")
    assertEquals(LlmConfig.deterministic.temperature, 0.0)
    assertEquals(LlmConfig.creative.temperature, 0.9)
    assertEquals(LlmConfig.claude35Sonnet.contextWindow, 200000)
  }

  test("CompletionOptions: from config") {
    val config = LlmConfig.default
    val options = CompletionOptions.fromConfig(config)

    assertEquals(options.temperature, config.temperature)
    assertEquals(options.maxTokens, Some(config.maxTokens))
  }

  test("TokenUsage: estimate cost") {
    val usage = TokenUsage(
      promptTokens = 1000,
      completionTokens = 500,
      totalTokens = 1500,
    )

    val cost = usage.estimateCost(
      promptCostPer1k = 0.01,
      completionCostPer1k = 0.03,
    )

    // 1000/1000 * 0.01 + 500/1000 * 0.03 = 0.01 + 0.015 = 0.025
    assertEquals(cost, 0.025, 0.0001)
  }

  test("LlmError: recoverability") {
    val recoverable = LlmError.RateLimitError("openai", Some(60))
    assert(LlmError.isRecoverable(recoverable))

    val nonRecoverable = LlmError.AuthenticationError("openai", "Invalid API key")
    assert(LlmError.isNonRecoverable(nonRecoverable))
  }

  test("LlmClientHelpers: retry with recoverable error") {
    var attempts = 0
    def operation: Either[LlmError, String] = {
      attempts += 1
      if (attempts < 3) Left(LlmError.TimeoutError("timeout"))
      else Right("success")
    }

    val result = LlmClientHelpers.retry(maxAttempts = 3, baseDelayMillis = 1) {
      operation
    }

    assert(result.isRight)
    assertEquals(attempts, 3)
  }

  test("LlmClientHelpers: don't retry non-recoverable error") {
    var attempts = 0
    def operation: Either[LlmError, String] = {
      attempts += 1
      Left(LlmError.AuthenticationError("provider", "auth failed"))
    }

    val result = LlmClientHelpers.retry(maxAttempts = 3, baseDelayMillis = 1) {
      operation
    }

    assert(result.isLeft)
    assertEquals(attempts, 1)
  }

  test("LlmClientHelpers: does not sleep on last attempt") {
    var attempts = 0
    var sleepCalls = 0

    val result = LlmClientHelpers.retry(
      maxAttempts = 1,
      baseDelayMillis = 1,
      sleepMillis = _ => sleepCalls += 1,
    ) {
      attempts += 1
      Left(LlmError.TimeoutError("timeout"))
    }

    assert(result.isLeft)
    assertEquals(attempts, 1)
    assertEquals(sleepCalls, 0)
  }

  test("TokenBudget: fits check") {
    val budget = TokenBudget(available = 1000, total = 2000, reserved = 500)

    assert(budget.fits(500))
    assert(budget.fits(1000))
    assert(!budget.fits(1001))

    assertEquals(budget.remaining(300), 700)
  }

  test("HeadroomPercent: values") {
    assertEquals(HeadroomPercent.NoHeadroom.value, 0.0)
    assertEquals(HeadroomPercent.Low.value, 0.05)
    assertEquals(HeadroomPercent.Standard.value, 0.10)
    assertEquals(HeadroomPercent.High.value, 0.20)
  }

}
