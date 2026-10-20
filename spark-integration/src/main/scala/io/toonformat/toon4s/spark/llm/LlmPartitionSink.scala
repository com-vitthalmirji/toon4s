package io.toonformat.toon4s.spark.llm

import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap

import scala.annotation.nowarn
import scala.util.Try

import org.llm4s.error.{LLMError, UnknownError}
import org.llm4s.llmconnect.LLMClient
import org.llm4s.llmconnect.model.{
  CompletionOptions => Llm4sCompletionOptions,
  Conversation => Llm4sConversation,
  SystemMessage => Llm4sSystemMessage,
  UserMessage => Llm4sUserMessage,
}

final case class LlmChunkRequest(
    idempotencyKey: String,
    partitionId: Int,
    chunkIndex: Int,
    toonChunk: String,
)

sealed trait LlmSendStatus

object LlmSendStatus {

  case object Sent extends LlmSendStatus

  case object Duplicate extends LlmSendStatus

  // The chunk reached the LLM, but the idempotency store failed to record it. Retrying the
  // send would re-deliver the chunk to the LLM, so this is reported rather than retried.
  final case class SentButUnrecorded(reason: String) extends LlmSendStatus

}

trait IdempotencyStore extends Serializable {

  def isSent(key: String): Either[LLMError, Boolean]

  def markSent(key: String): Either[LLMError, Unit]

}

object IdempotencyStore {

  object Noop extends IdempotencyStore {

    def isSent(key: String): Either[LLMError, Boolean] = Right(false)

    def markSent(key: String): Either[LLMError, Unit] = Right(())

  }

  final class InMemory extends IdempotencyStore {

    private val sentKeys = new ConcurrentHashMap[String, java.lang.Boolean]()

    def isSent(key: String): Either[LLMError, Boolean] =
      Right(sentKeys.containsKey(key))

    def markSent(key: String): Either[LLMError, Unit] = {
      sentKeys.put(key, java.lang.Boolean.TRUE)
      Right(())
    }

  }

}

trait LlmPartitionWriter extends Serializable {

  def send(request: LlmChunkRequest): Either[LLMError, LlmSendStatus]

  def close(): Unit = ()

}

trait LlmPartitionWriterFactory extends Serializable {

  def create(): LlmPartitionWriter

}

object LlmPartitionWriterFactory {

  def fromClientFactory(
      clientFactory: () => LLMClient,
      idempotencyStore: IdempotencyStore = IdempotencyStore.Noop,
      systemPrompt: String = "Analyze this TOON payload.",
      completionOptions: Llm4sCompletionOptions = Llm4sCompletionOptions(),
  ): LlmPartitionWriterFactory =
    new LlmPartitionWriterFactory {
      def create(): LlmPartitionWriter = {
        val client = clientFactory()
        new LlmPartitionWriter {
          def send(request: LlmChunkRequest): Either[LLMError, LlmSendStatus] = {
            idempotencyStore.isSent(request.idempotencyKey).flatMap {
              case true  => Right(LlmSendStatus.Duplicate)
              case false =>
                Llm4sConversation
                  .create(
                    List(
                      Llm4sSystemMessage(systemPrompt),
                      Llm4sUserMessage(
                        s"idempotency_key=${request.idempotencyKey}\n${request.toonChunk}"
                      ),
                    )
                  )
                  .flatMap(conv => client.complete(conv, completionOptions))
                  .map(_ => markSentOrReport(idempotencyStore, request.idempotencyKey))
            }
          }
          override def close(): Unit = client.close()
        }
      }
    }

  // Called only after the chunk has already reached the LLM. A failure to record it must not
  // be surfaced as a Left: the caller's retry would re-send the chunk, since the idempotency
  // key would still look unsent. Reporting it as a status instead keeps that resend from
  // happening while still telling the caller the dedup guarantee is compromised for this key.
  private def markSentOrReport(store: IdempotencyStore, key: String): LlmSendStatus =
    store.markSent(key) match {
    case Right(_)  => LlmSendStatus.Sent
    case Left(err) => LlmSendStatus.SentButUnrecorded(err.formatted)
    }

  @deprecated(
    "Legacy client path. Use fromClientFactory with an llm4s client. Scheduled for removal.",
    "0.9.0",
  )
  @nowarn("cat=deprecation")
  def fromLegacyClientFactory(
      clientFactory: () => LlmClient,
      idempotencyStore: IdempotencyStore = IdempotencyStore.Noop,
      systemPrompt: String = "Analyze this TOON payload.",
      completionOptions: CompletionOptions = CompletionOptions(),
  ): LlmPartitionWriterFactory =
    new LlmPartitionWriterFactory {
      def create(): LlmPartitionWriter = {
        val client = clientFactory()
        new LlmPartitionWriter {
          def send(request: LlmChunkRequest): Either[LLMError, LlmSendStatus] = {
            val prompt = s"idempotency_key=${request.idempotencyKey}\n${request.toonChunk}"
            idempotencyStore.isSent(request.idempotencyKey).flatMap {
              case true  => Right(LlmSendStatus.Duplicate)
              case false =>
                client
                  .complete(
                    Conversation(Vector(
                      SystemMessage(systemPrompt),
                      UserMessage(prompt),
                    )),
                    completionOptions,
                  )
                  .left
                  .map(toLlm4sError)
                  .map(_ => markSentOrReport(idempotencyStore, request.idempotencyKey))
            }
          }
          override def close(): Unit = client.close()
        }
      }
    }

  private def toLlm4sError(error: LlmError): LLMError =
    UnknownError(
      error.formatted,
      error match {
      case LlmError.UnknownError(_, Some(cause)) => cause
      case _                                     => new RuntimeException(error.formatted)
      },
    )

}

final case class LlmPartitionWriteOptions(
    maxRetries: Int = 2,
    retryBackoffMs: Long = 25L,
    failOnError: Boolean = true,
    idempotencyPrefix: String = "toon4s",
)

final case class LlmPartitionWriteMetrics(
    partitionCount: Long,
    attemptedChunks: Long,
    sentChunks: Long,
    duplicateChunks: Long,
    failedChunks: Long,
    totalBytes: Long,
    totalEstimatedTokens: Long,
    firstError: Option[String],
)

object LlmPartitionWriteMetrics {

  val empty: LlmPartitionWriteMetrics =
    LlmPartitionWriteMetrics(0L, 0L, 0L, 0L, 0L, 0L, 0L, None)

}

private[spark] object LlmPartitionIdempotency {

  def build(
      prefix: String,
      key: String,
      partitionId: Int,
      chunkIndex: Int,
      chunk: String,
  ): String = {
    val chunkHash = sha256(chunk)
    s"$prefix:$key:p$partitionId:c$chunkIndex:h$chunkHash"
  }

  private def sha256(value: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    digest.digest(value.getBytes("UTF-8")).map("%02x".format(_)).mkString
  }

}

private[spark] object LlmRetry {

  def withRetries[A](
      maxRetries: Int,
      backoffMs: Long,
  )(operation: => Either[LLMError, A]): Either[LLMError, A] = {
    @annotation.tailrec
    def loop(attempt: Int): Either[LLMError, A] = {
      operation match {
      case right @ Right(_)                  => right
      case Left(err) if attempt < maxRetries =>
        val sleepMillis = math.max(0L, backoffMs) * (attempt.toLong + 1L)
        Try(Thread.sleep(sleepMillis))
        loop(attempt + 1)
      case left @ Left(_) => left
      }
    }
    loop(0)
  }

}
