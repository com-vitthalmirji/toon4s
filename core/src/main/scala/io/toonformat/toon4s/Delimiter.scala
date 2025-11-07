package io.toonformat.toon4s

sealed abstract class Delimiter(val char: Char)

object Delimiter {
  case object Comma extends Delimiter(',')
  case object Tab   extends Delimiter('\t')
  case object Pipe  extends Delimiter('|')

  val values: List[Delimiter] = List(Comma, Tab, Pipe)
}
