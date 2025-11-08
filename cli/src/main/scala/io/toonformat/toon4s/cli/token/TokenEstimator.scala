package io.toonformat.toon4s.cli.token

import com.knuddels.jtokkit.Encodings
import com.knuddels.jtokkit.api.{Encoding, EncodingType}

object TokenEstimator {

  private def registry = Encodings.newDefaultEncodingRegistry()

  private def encodingTypeFor(name: String): EncodingType =
    name.toLowerCase match {
    case "o200k" | "o200k_base" => EncodingType.O200K_BASE
    case "p50k" | "p50k_base"   => EncodingType.P50K_BASE
    case "r50k" | "r50k_base"   => EncodingType.R50K_BASE
    case _                      => EncodingType.CL100K_BASE
    }

  def canonicalName(name: String): String =
    name.toLowerCase match {
    case "o200k" | "o200k_base" => "O200K_BASE"
    case "p50k" | "p50k_base"   => "P50K_BASE"
    case "r50k" | "r50k_base"   => "R50K_BASE"
    case _                      => "CL100K_BASE"
    }

  def resolveEncoding(name: String): Encoding =
    registry.getEncoding(encodingTypeFor(name))

  def estimateTokens(text: String): Int =
    if (text.isEmpty) 0 else resolveEncoding("cl100k").countTokens(text)

  def estimateTokens(text: String, tokenizer: String): Int = {
    val enc = resolveEncoding(tokenizer)
    if (text.isEmpty) 0 else enc.countTokens(text)
  }

}
