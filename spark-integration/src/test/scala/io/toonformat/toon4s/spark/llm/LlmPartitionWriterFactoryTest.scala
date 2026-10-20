package io.toonformat.toon4s.spark.llm

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._

import munit.FunSuite
import org.llm4s.config.Llm4sConfig
import org.llm4s.error.{LLMError, NetworkError}
import org.llm4s.llmconnect.{LLMClient, LLMConnect}
import org.llm4s.llmconnect.model.{
  AssistantMessage,
  Completion,
  CompletionOptions,
  Conversation,
  SystemMessage,
  UserMessage,
}
import org.llm4s.llmconnect.spi.ProviderRegistry

/**
 * Drives `LlmPartitionWriterFactory.fromClientFactory` with an llm4s `LLMClient`, from Scala 2.13
 * against the Scala 3 llm4s API.
 */
class LlmPartitionWriterFactoryTest extends FunSuite {

  /**
   * A fake llm4s client. It is a JDK dynamic proxy rather than a subclass: Scala 2.13 reads the
   * Scala 3 `LLMClient` through the TASTy reader, which can call its methods but cannot type-check
   * a subclass of it (`completeStructured` takes an upickle `ReadWriter` it cannot resolve).
   */
  final private class RecordingClient(reply: Conversation => Either[LLMError, Completion]) {

    val conversations = new ConcurrentLinkedQueue[Conversation]()

    val options = new ConcurrentLinkedQueue[CompletionOptions]()

    @volatile var closed = false

    val client: LLMClient = Proxy
      .newProxyInstance(
        classOf[LLMClient].getClassLoader,
        Array[Class[_]](classOf[LLMClient]),
        new InvocationHandler {
          def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef =
            method.getName match {
            case "complete" =>
              val conversation = args(0).asInstanceOf[Conversation]
              conversations.add(conversation)
              options.add(args(1).asInstanceOf[CompletionOptions])
              reply(conversation)
            case "close" =>
              closed = true
              null
            case other =>
              throw new UnsupportedOperationException(s"not used by the sink: $other")
            }
        },
      )
      .asInstanceOf[LLMClient]

  }

  private val ok: Conversation => Either[LLMError, Completion] =
    _ =>
      Right(
        Completion(
          id = "completion-1",
          created = 0L,
          content = "ok",
          model = "fake-model",
          message = AssistantMessage("ok"),
        )
      )

  private val request =
    LlmChunkRequest("key-1", partitionId = 0, chunkIndex = 0, toonChunk = "a: 1")

  test("sends the system prompt and the TOON chunk with its idempotency key") {
    val client = new RecordingClient(ok)
    val store = new IdempotencyStore.InMemory
    val writer = LlmPartitionWriterFactory
      .fromClientFactory(() => client.client, store, systemPrompt = "Summarise.")
      .create()

    assertEquals(writer.send(request), Right(LlmSendStatus.Sent))
    assertEquals(
      client.conversations.asScala.toList.map(_.messages.toList),
      List(List(SystemMessage("Summarise."), UserMessage("idempotency_key=key-1\na: 1"))),
    )
    assertEquals(store.isSent("key-1"), Right(true))
  }

  test("passes the completion options to the client") {
    val client = new RecordingClient(ok)
    val options = CompletionOptions().withMaxTokens(256)
    val writer = LlmPartitionWriterFactory
      .fromClientFactory(() => client.client, completionOptions = options)
      .create()

    assertEquals(writer.send(request), Right(LlmSendStatus.Sent))
    assertEquals(client.options.asScala.toList, List(options))
  }

  test("a key already sent is reported as a duplicate without calling the client") {
    val client = new RecordingClient(ok)
    val store = new IdempotencyStore.InMemory
    assertEquals(store.markSent("key-1"), Right(()))
    val writer = LlmPartitionWriterFactory.fromClientFactory(() => client.client, store).create()

    assertEquals(writer.send(request), Right(LlmSendStatus.Duplicate))
    assert(client.conversations.isEmpty)
  }

  test("a client error is returned and the key is not marked as sent") {
    val failure = NetworkError("connection refused", None, "fake-endpoint")
    val client = new RecordingClient(_ => Left(failure))
    val store = new IdempotencyStore.InMemory
    val writer = LlmPartitionWriterFactory.fromClientFactory(() => client.client, store).create()

    assertEquals(writer.send(request), Left(failure))
    assertEquals(store.isSent("key-1"), Right(false))
  }

  test("a markSent failure after a successful send is reported, not retried as a resend") {
    val client = new RecordingClient(ok)
    val storeFailure = NetworkError("store unavailable", None, "idempotency-store")
    val store = new IdempotencyStore {
      def isSent(key: String): Either[LLMError, Boolean] = Right(false)
      def markSent(key: String): Either[LLMError, Unit] = Left(storeFailure)
    }
    val writer = LlmPartitionWriterFactory.fromClientFactory(() => client.client, store).create()

    assertEquals(
      writer.send(request),
      Right(LlmSendStatus.SentButUnrecorded(storeFailure.formatted)),
    )
    assertEquals(client.conversations.size, 1)
  }

  test("closing the writer closes the client") {
    val client = new RecordingClient(ok)
    val writer = LlmPartitionWriterFactory.fromClientFactory(() => client.client).create()

    writer.close()
    assert(client.closed)
  }

  // The client setup the spark-integration README shows, run from Scala 2.13. No provider module
  // or provider section is on the test classpath, so it must end in an llm4s error - a Left -
  // rather than a linkage error thrown from the Scala 3 configuration code.
  test(
    "the README's client setup runs from Scala 2.13 and reports a missing provider as an error"
  ) {
    val registry = ProviderRegistry.default
    val result = for {
      models <- Llm4sConfig.modelRegistryService()
      config <- Llm4sConfig.defaultProvider()(registry)
      client <- LLMConnect.getClient(config)(models, registry)
    } yield client

    assert(
      result.left.exists(_.message.toLowerCase.contains("provider")),
      s"expected a missing-provider error, got $result",
    )
  }

}
