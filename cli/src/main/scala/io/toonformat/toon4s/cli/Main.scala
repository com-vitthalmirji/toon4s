package io.toonformat.toon4s.cli

import io.toonformat.toon4s._
import io.toonformat.toon4s.json.SimpleJson
import scopt.OParser

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

object Main {
  sealed trait Mode
  private case object EncodeMode extends Mode
  private case object DecodeMode extends Mode

  private final case class Config(
      mode: Option[Mode] = None,
      input: Path = Paths.get(""),
      output: Option[Path] = None,
      indent: Int = 2,
      strict: Boolean = true,
      delimiter: Delimiter = Delimiter.Comma,
      lengthMarker: Boolean = false
  )

  def main(args: Array[String]): Unit = {
    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        run(config) match {
          case Right(_)  =>
          case Left(err) =>
            System.err.println(err)
            sys.exit(1)
        }
      case None         =>
        sys.exit(1)
    }
  }

  private def run(config: Config): Either[String, Unit] = config.mode match {
    case Some(EncodeMode) => runEncode(config)
    case Some(DecodeMode) => runDecode(config)
    case None             => Left("Please specify --encode or --decode")
  }

  private def runEncode(config: Config): Either[String, Unit] = {
    val jsonInput  = Files.readString(config.input, StandardCharsets.UTF_8)
    val scalaValue = SimpleJson.toScala(SimpleJson.parse(jsonInput))
    val options    = EncodeOptions(
      indent = config.indent,
      delimiter = config.delimiter,
      lengthMarker = config.lengthMarker
    )
    Toon.encode(scalaValue, options).left.map(_.message).flatMap {
      encoded => writeOutput(encoded, config.output)
    }
  }

  private def runDecode(config: Config): Either[String, Unit] = {
    val toonInput = Files.readString(config.input, StandardCharsets.UTF_8)
    val options   = DecodeOptions(indent = config.indent, strict = config.strict)
    Toon
      .decode(toonInput, options)
      .left
      .map(_.message)
      .flatMap {
        json =>
          val rendered = SimpleJson.stringify(json)
          writeOutput(rendered, config.output)
      }
  }

  private def writeOutput(content: String, output: Option[Path]): Either[String, Unit] =
    try {
      output match {
        case Some(path) =>
          Option(path.getParent).foreach(
            p => Files.createDirectories(p)
          )
          Files.write(path, content.getBytes(StandardCharsets.UTF_8))
        case None       =>
          println(content)
      }
      Right(())
    } catch {
      case ex: Throwable =>
        Left(s"Failed to write output: ${ex.getMessage}")
    }

  private val parser = {
    import scopt.OParser
    val builder = OParser.builder[Config]
    import builder._
    OParser.sequence(
      programName("toon4s-cli"),
      help("help").text("Show help and exit."),
      opt[Unit]("encode")
        .action(
          (_, c) => c.copy(mode = Some(EncodeMode))
        )
        .text("Encode JSON input to TOON output."),
      opt[Unit]("decode")
        .action(
          (_, c) => c.copy(mode = Some(DecodeMode))
        )
        .text("Decode TOON input to JSON output."),
      opt[String]('o', "output")
        .valueName("<file>")
        .action(
          (path, c) => c.copy(output = Some(Paths.get(path)))
        )
        .text("Optional output path; defaults to stdout."),
      opt[Int]("indent")
        .valueName("<n>")
        .validate(
          n => if (n >= 0) success else failure("indent must be non-negative")
        )
        .action(
          (indent, c) => c.copy(indent = indent)
        )
        .text("Indentation used for encoding (default: 2)."),
      opt[Boolean]("strict")
        .action(
          (flag, c) => c.copy(strict = flag)
        )
        .text("Strict decoding (default: true)."),
      opt[String]("delimiter")
        .valueName("comma|tab|pipe")
        .validate(
          value =>
            parseDelimiter(value)
              .map(
                _ => success
              )
              .getOrElse(failure("delimiter must be comma, tab, or pipe"))
        )
        .action(
          (value, c) =>
            parseDelimiter(value)
              .map(
                delim => c.copy(delimiter = delim)
              )
              .getOrElse(c)
        )
        .text("Delimiter for encoding tabular data (default: comma)."),
      opt[Unit]("length-marker")
        .action(
          (_, c) => c.copy(lengthMarker = true)
        )
        .text("Emit #length markers for encoded arrays."),
      arg[String]("<input>")
        .required()
        .action(
          (path, c) => c.copy(input = Paths.get(path))
        )
        .text("Input file path.")
    )
  }

  private def parseDelimiter(value: String): Option[Delimiter] =
    value.toLowerCase match {
      case "comma" | "," => Some(Delimiter.Comma)
      case "tab" | "\\t" => Some(Delimiter.Tab)
      case "pipe" | "|"  => Some(Delimiter.Pipe)
      case _             => None
    }
}
