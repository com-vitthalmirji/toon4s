package io.toonformat.toon4s.cli.token

import com.knuddels.jtokkit.Encodings
import com.knuddels.jtokkit.api.{Encoding, EncodingType}

object TokenEstimator {
  private val encoding: Encoding =
    Encodings.newDefaultEncodingRegistry().getEncoding(EncodingType.CL100K_BASE)

  def estimateTokens(text: String): Int =
    if (text.isEmpty) 0 else encoding.countTokens(text)
}
