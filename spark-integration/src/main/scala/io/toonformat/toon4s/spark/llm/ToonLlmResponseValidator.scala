package io.toonformat.toon4s.spark.llm

/**
 * Lightweight response checks for TOON prompt pipelines.
 *
 * This detects common signs that a model did not understand the TOON payload format and returned a
 * clarification request or raw JSON echo instead of the requested task output.
 */
object ToonLlmResponseValidator {

  sealed trait ValidationIssue extends Product with Serializable {

    def message: String

  }

  object ValidationIssue {

    final case class EmptyResponse(message: String = "LLM response is empty")
        extends ValidationIssue

    final case class FormatConfusion(message: String) extends ValidationIssue

    final case class RawJsonEcho(message: String) extends ValidationIssue

  }

  final case class ValidationResult(
      valid: Boolean,
      issues: Vector[ValidationIssue],
  ) {

    def hasConfusion: Boolean = issues.exists {
      case _: ValidationIssue.FormatConfusion => true
      case _                                  => false
    }

    def hasRawJsonEcho: Boolean = issues.exists {
      case _: ValidationIssue.RawJsonEcho => true
      case _                              => false
    }

  }

  private val confusionPatterns = Vector(
    "what is toon",
    "i do not understand toon",
    "please provide json",
    "please provide the data in json",
    "unknown format",
    "cannot parse toon",
    "unsupported format",
  )

  def validate(response: String): ValidationResult = {
    val trimmed = Option(response).map(_.trim).getOrElse("")
    if (trimmed.isEmpty) {
      ValidationResult(valid = false, issues = Vector(ValidationIssue.EmptyResponse()))
    } else {
      val lowered = trimmed.toLowerCase(java.util.Locale.ROOT)
      val issues = Vector.newBuilder[ValidationIssue]

      if (confusionPatterns.exists(lowered.contains)) {
        issues += ValidationIssue.FormatConfusion(
          "LLM response suggests format confusion. Add explicit TOON instructions in the system prompt."
        )
      }

      val looksLikeRawJsonObject = lowered.startsWith("{") && lowered.endsWith("}")
      val looksLikeRawJsonArray = lowered.startsWith("[") && lowered.endsWith("]")
      if (looksLikeRawJsonObject || looksLikeRawJsonArray) {
        issues += ValidationIssue.RawJsonEcho(
          "LLM returned raw JSON. Verify prompt intent and require task output format explicitly."
        )
      }

      val builtIssues = issues.result()
      ValidationResult(valid = builtIssues.isEmpty, issues = builtIssues)
    }
  }

}
