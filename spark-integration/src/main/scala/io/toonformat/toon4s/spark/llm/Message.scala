package io.toonformat.toon4s.spark.llm

/**
 * Message types for LLM conversations.
 *
 * ==Design Alignment with llm4s==
 * These types mirror llm4s's message model:
 *   - `org.llm4s.llmconnect.model.Message` (sealed trait)
 *   - `SystemMessage`, `UserMessage`, `AssistantMessage`
 *
 * ==Future Compatibility==
 * When llm4s is available, these types can be converted to/from llm4s types using a compatibility
 * layer.
 *
 * @see
 *   [[https://github.com/llm4s/llm4s llm4s project]]
 */
sealed trait Message {

  def role: MessageRole

  def content: String

  /** Validate message content (llm4s pattern). */
  def validate(): Either[String, Unit] = {
    if (content.trim.isEmpty) Left(s"${role.name} message cannot be empty")
    else Right(())
  }

}

/** Message role types (matches llm4s MessageRole). */
sealed trait MessageRole {

  def name: String

}

object MessageRole {

  case object System extends MessageRole { val name = "system" }

  case object User extends MessageRole { val name = "user" }

  case object Assistant extends MessageRole { val name = "assistant" }

}

/**
 * System message for providing instructions to the LLM.
 *
 * Equivalent to llm4s `SystemMessage`.
 */
final case class SystemMessage(content: String) extends Message {

  val role: MessageRole = MessageRole.System

}

/**
 * User message representing user input.
 *
 * Equivalent to llm4s `UserMessage`.
 */
final case class UserMessage(content: String) extends Message {

  val role: MessageRole = MessageRole.User

}

/**
 * Assistant message representing LLM response.
 *
 * Equivalent to llm4s `AssistantMessage` (simplified - no tool calls yet).
 */
final case class AssistantMessage(content: String) extends Message {

  val role: MessageRole = MessageRole.Assistant

}

object Message {

  /** Smart constructor for system messages (llm4s pattern). */
  def system(content: String): Either[String, SystemMessage] = {
    if (content.trim.isEmpty) Left("System message cannot be empty")
    else Right(SystemMessage(content))
  }

  /** Smart constructor for user messages (llm4s pattern). */
  def user(content: String): Either[String, UserMessage] = {
    if (content.trim.isEmpty) Left("User message cannot be empty")
    else Right(UserMessage(content))
  }

  /** Smart constructor for assistant messages (llm4s pattern). */
  def assistant(content: String): Either[String, AssistantMessage] = {
    if (content.trim.isEmpty) Left("Assistant message cannot be empty")
    else Right(AssistantMessage(content))
  }

}

/**
 * Conversation containing a sequence of messages.
 *
 * ==Design Alignment with llm4s==
 * Mirrors llm4s's `org.llm4s.llmconnect.model.Conversation`:
 *   - Immutable message container
 *   - Smart constructors with validation
 *   - Fluent API for adding messages
 *
 * @param messages
 *   Sequence of messages in chronological order
 */
final case class Conversation(messages: Vector[Message]) {

  /** Add a message to the conversation. */
  def addMessage(message: Message): Conversation =
    copy(messages = messages :+ message)

  /** Add multiple messages to the conversation. */
  def addMessages(newMessages: Vector[Message]): Conversation =
    copy(messages = messages ++ newMessages)

  /** Get the last message in the conversation. */
  def lastMessage: Option[Message] = messages.lastOption

  /** Filter messages by role. */
  def filterByRole(role: MessageRole): Vector[Message] =
    messages.filter(_.role == role)

  /** Validate conversation structure (llm4s pattern). */
  def validate(): Either[String, Unit] = {
    if (messages.isEmpty) {
      Left("Conversation cannot be empty")
    } else {
      // Validate each message
      val validationResults = messages.map(_.validate())
      validationResults.collectFirst { case Left(err) => err } match {
      case Some(error) => Left(error)
      case None        => Right(())
      }
    }
  }

}

object Conversation {

  /** Create conversation with validation (llm4s pattern). */
  def create(messages: Message*): Either[String, Conversation] = {
    val conv = Conversation(messages.toVector)
    conv.validate().map(_ => conv)
  }

  /** Create conversation from system and user prompts (llm4s pattern). */
  def fromPrompts(systemPrompt: String, userPrompt: String): Either[String, Conversation] = {
    for {
      sys <- Message.system(systemPrompt)
      user <- Message.user(userPrompt)
      conv <- create(sys, user)
    } yield conv
  }

  /** Create conversation with only user prompt (llm4s pattern). */
  def userOnly(prompt: String): Either[String, Conversation] = {
    for {
      user <- Message.user(prompt)
      conv <- create(user)
    } yield conv
  }

  /** Create conversation with only system prompt (llm4s pattern). */
  def systemOnly(prompt: String): Either[String, Conversation] = {
    for {
      sys <- Message.system(prompt)
      conv <- create(sys)
    } yield conv
  }

  /** Empty conversation. */
  val empty: Conversation = Conversation(Vector.empty)

}
