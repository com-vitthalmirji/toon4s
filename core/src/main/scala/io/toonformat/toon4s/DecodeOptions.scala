package io.toonformat.toon4s

sealed trait Strictness
object Strictness {
  case object Strict  extends Strictness
  case object Lenient extends Strictness
  case object Audit   extends Strictness
}

final case class DecodeOptions(
    indent: Int = 2,
    strict: Boolean = true,                    // [DEPRECATED] prefer `strictness` (kept for compatibility)
    strictness: Strictness = Strictness.Strict // preferred
)
