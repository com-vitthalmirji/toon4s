package io.toonformat.toon4s.decode

import io.toonformat.toon4s._
import io.toonformat.toon4s.{Constants => C}
import scala.annotation.tailrec

/** Experimental streaming helpers for tabular arrays, implemented with total, tail-recursive
  * functions. Side-effects are limited to the user-provided callbacks.
  */
object Streaming {

  /** Streams tabular array rows to the provided consumer. Invokes `onRow(keyOpt, fields, values)`
    * for each row encountered under any tabular header. Only processes rows directly following a
    * tabular header; nested structures within a row are not streamed.
    */
  def foreachTabular(
      in: java.io.Reader,
      options: DecodeOptions = DecodeOptions()
  )(
      onRow: (Option[String], List[String], Vector[String]) => Unit
  ): Either[error.DecodeError, Unit] = {
    val scan  = Scanner.toParsedLines(in, options.indent, options.strict)
    val lines = scan.lines

    @tailrec
    def streamRows(
        idx: Int,
        baseDepth: Int,
        header: ArrayHeaderInfo,
        seen: Int
    ): Either[error.DecodeError, Int] = {
      val rowDepth = baseDepth + 1
      if (seen >= header.length || idx >= lines.length) Right(idx)
      else
        lines.lift(idx) match {
          case Some(pl) if pl.depth == rowDepth =>
            val values = Parser.parseDelimitedValues(pl.content, header.delimiter)
            onRow(header.key, header.fields, values)
            streamRows(idx + 1, baseDepth, header, seen + 1)
          case Some(pl) if pl.depth < rowDepth  => Right(idx)
          case Some(_)                          => Right(idx)
          case None                             => Right(idx)
        }
    }

    @tailrec
    def loop(idx: Int): Either[error.DecodeError, Unit] = {
      if (idx >= lines.length) Right(())
      else {
        val pl = lines(idx)
        Parser.parseArrayHeaderLine(pl.content, Delimiter.Comma) match {
          case Some((h, inline)) if h.fields.nonEmpty && inline.isEmpty =>
            streamRows(idx + 1, pl.depth, h, 0) match {
              case Right(next) => loop(next)
              case Left(e)     => Left(e)
            }
          case _                                                        => loop(idx + 1)
        }
      }
    }

    loop(0)
  }

  /** Streams all array headers and their rows with a simple immutable path stack of keys. Calls
    * `onHeader(path, header)` when a header is encountered and `onRow(path, header, values)` for
    * each row. Path contains enclosing object keys leading to the header (outermost first).
    */
  def foreachArrays(
      in: java.io.Reader,
      options: DecodeOptions = DecodeOptions()
  )(
      onHeader: (Vector[String], ArrayHeaderInfo) => Unit,
      onRow: (Vector[String], ArrayHeaderInfo, Vector[String]) => Unit
  ): Either[error.DecodeError, Unit] = {
    val scan  = Scanner.toParsedLines(in, options.indent, options.strict)
    val lines = scan.lines

    @tailrec
    def streamRows(
        idx: Int,
        baseDepth: Int,
        header: ArrayHeaderInfo,
        path: Vector[String],
        seen: Int
    ): Either[error.DecodeError, Int] = {
      val rowDepth = baseDepth + 1
      if (seen >= header.length || idx >= lines.length) Right(idx)
      else
        lines.lift(idx) match {
          case Some(pl) if pl.depth == rowDepth =>
            val values = Parser.parseDelimitedValues(pl.content, header.delimiter)
            onRow(path, header, values)
            streamRows(idx + 1, baseDepth, header, path, seen + 1)
          case Some(pl) if pl.depth < rowDepth  => Right(idx)
          case Some(_)                          => Right(idx)
          case None                             => Right(idx)
        }
    }

    @tailrec
    def loop(idx: Int): Either[error.DecodeError, Unit] = {
      if (idx >= lines.length) Right(())
      else {
        lines.lift(idx) match {
          case Some(pl)
              if pl.content.startsWith(C.ListItemPrefix) || pl.content == C.ListItemMarker =>
            val after =
              if (pl.content.startsWith(C.ListItemPrefix)) pl.content.drop(C.ListItemPrefix.length)
              else ""
            scala.util.Try(Parser.parseKeyToken(after, 0)).toEither match {
              case Right((key, restIdx)) =>
                val rest = after.substring(restIdx).trim
                Parser.parseArrayHeaderLine(rest, Delimiter.Comma) match {
                  case Some((h, inline)) if inline.isEmpty && h.fields.nonEmpty =>
                    val header = h.copy(key = Some(key))
                    onHeader(Vector.empty, header)
                    streamRows(idx + 1, pl.depth, header, Vector.empty, 0) match {
                      case Right(next) => loop(next)
                      case Left(err)   => Left(err)
                    }
                  case _                                                        => loop(idx + 1)
                }
              case _                     =>
                // also support bare header after hyphen
                if (Parser.isArrayHeaderAfterHyphen(after))
                  Parser.parseArrayHeaderLine(after, Delimiter.Comma) match {
                    case Some((header, inline)) if inline.isEmpty =>
                      onHeader(Vector.empty, header)
                      streamRows(idx + 1, pl.depth, header, Vector.empty, 0) match {
                        case Right(next) => loop(next)
                        case Left(err)   => Left(err)
                      }
                    case _                                        => loop(idx + 1)
                  }
                else loop(idx + 1)
            }
          case Some(pl) =>
            Parser.parseArrayHeaderLine(pl.content, Delimiter.Comma) match {
              case Some((h, inline)) if inline.isEmpty && h.fields.nonEmpty =>
                val header = h
                onHeader(Vector.empty, header)
                streamRows(idx + 1, pl.depth, header, Vector.empty, 0) match {
                  case Right(next) => loop(next)
                  case Left(err)   => Left(err)
                }
              case _                                                        => loop(idx + 1)
            }
          case None     => Right(())
        }
      }
    }

    loop(0)
  }
}
