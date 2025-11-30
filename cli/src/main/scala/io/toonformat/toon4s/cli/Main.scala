package io.toonformat.toon4s.cli

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import io.toonformat.toon4s._
import io.toonformat.toon4s.json.SimpleJson
import io.toonformat.toon4s.util.EitherUtils._
import scopt.OParser

object Main {

  sealed trait Mode

  private case object EncodeMode extends Mode

  private case object DecodeMode extends Mode

  final private case class Config(
      mode: Option[Mode] = None,
      input: Path = Paths.get(""),
      output: Option[Path] = None,
      indent: Int = 2,
      strictness: String = "strict", // CLI string, converted to Strictness
      delimiter: Delimiter = Delimiter.Comma,
      keyFolding: String = "off",
      flattenDepth: Int = Int.MaxValue,
      expandPaths: String = "off",
      stats: Boolean = false,
      optimize: Boolean = false,
      tokenizer: String = "cl100k",
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
    case None =>
      sys.exit(1)
    }
  }

  private def readUtf8(path: Path): Either[String, String] =
    scala.util
      .Try(Files.readString(path, StandardCharsets.UTF_8))
      .toEither
      .left
      .map(t => s"Failed to read input: ${t.getMessage}")

  private def emitWithStats(
      inputText: String,
      outputText: String,
      output: Option[Path],
      stats: Boolean,
      tokenizer: String,
  ): Either[String, Unit] = {
    val write = () => writeOutput(outputText, output)
    if (!stats) write()
    else {
      val name = token.TokenEstimator.canonicalName(tokenizer)
      val in = token.TokenEstimator.estimateTokens(inputText, tokenizer)
      val out = token.TokenEstimator.estimateTokens(outputText, tokenizer)
      val pct = if (in > 0) Math.round((1.0 - out.toDouble / in) * 100).toInt else 0
      System.err.println(
        s"[stats] tokenizer=$name input=$in output=$out delta=${out - in} savings=$pct%"
      )
      write()
    }
  }

  private def run(config: Config): Either[String, Unit] = config.mode match {
  case Some(EncodeMode) => runEncode(config)
  case Some(DecodeMode) => runDecode(config)
  case None             => Left("Please specify --encode or --decode")
  }

  private def runEncode(config: Config): Either[String, Unit] = {
    for {
      jsonInput <- readUtf8(config.input)
      scalaValue <- scala.util
        .Try(SimpleJson.toScala(SimpleJson.parse(jsonInput)))
        .toEitherMap(t => s"Invalid JSON input: ${t.getMessage}")
      base = EncodeOptions(
        indent = config.indent,
        delimiter = config.delimiter,
        keyFolding = asKeyFolding(config.keyFolding),
        flattenDepth = config.flattenDepth,
      )
      opt <- if (config.optimize) optimize(scalaValue, base, config.tokenizer) else Right(base)
      encoded <- Toon.encode(scalaValue, opt).left.map(_.message)
      _ <- emitWithStats(jsonInput, encoded, config.output, config.stats, config.tokenizer)
    } yield ()
  }

  private def runDecode(config: Config): Either[String, Unit] = {
    val options = DecodeOptions(
      indent = config.indent,
      strictness = asStrictness(config.strictness),
      expandPaths = asExpandPaths(config.expandPaths),
    )
    for {
      toonInput <- readUtf8(config.input)
      json <- Toon.decode(toonInput, options).left.map(_.message)
      rendered = SimpleJson.stringify(json)
      _ <- emitWithStats(toonInput, rendered, config.output, config.stats, config.tokenizer)
    } yield ()
  }

  private def writeOutput(content: String, output: Option[Path]): Either[String, Unit] =
    scala.util
      .Try {
        output match {
        case Some(path) =>
          Option(path.getParent).foreach(p => Files.createDirectories(p))
          Files.write(path, content.getBytes(StandardCharsets.UTF_8))
        case None =>
          println(content)
        }
      }
      .toEither
      .left
      .map(ex => s"Failed to write output: ${ex.getMessage}")
      .map(_ => ())

  private val parser = {
    import scopt.OParser
    val builder = OParser.builder[Config]
    import builder._
    OParser.sequence(
      programName("toon4s-cli"),
      help("help").text("Show help and exit."),
      opt[Unit]("encode")
        .action((_, c) => c.copy(mode = Some(EncodeMode)))
        .text("Encode JSON input to TOON output."),
      opt[Unit]("decode")
        .action((_, c) => c.copy(mode = Some(DecodeMode)))
        .text("Decode TOON input to JSON output."),
      opt[String]('o', "output")
        .valueName("<file>")
        .action((path, c) => c.copy(output = Some(Paths.get(path))))
        .text("Optional output path; defaults to stdout."),
      opt[Int]("indent")
        .valueName("<n>")
        .validate(n => if (n > 0) success else failure("indent must be positive"))
        .action((indent, c) => c.copy(indent = indent))
        .text("Indentation used for encoding (default: 2)."),
      opt[String]("strictness")
        .valueName("strict|lenient")
        .validate(v =>
          if (Set("strict", "lenient").contains(v.toLowerCase)) success
          else failure("strictness must be 'strict' or 'lenient'")
        )
        .action((v, c) => c.copy(strictness = v.toLowerCase))
        .text(
          "Strictness mode (default: strict). " +
            "'strict' enforces TOON v2.1 §14: count mismatches, indentation errors, etc. " +
            "'lenient' accepts malformed input when possible."
        ),
      opt[String]("delimiter")
        .valueName("comma|tab|pipe")
        .validate(value =>
          parseDelimiter(value)
            .map(_ => success)
            .getOrElse(failure("delimiter must be comma, tab, or pipe"))
        )
        .action((value, c) =>
          parseDelimiter(value)
            .map(delim => c.copy(delimiter = delim))
            .getOrElse(c)
        )
        .text("Delimiter for encoding tabular data (default: comma)."),
      opt[String]("key-folding")
        .valueName("off|safe")
        .validate(v =>
          if (Set("off", "safe").contains(v.toLowerCase)) success
          else failure("key-folding must be 'off' or 'safe'")
        )
        .action((v, c) => c.copy(keyFolding = v.toLowerCase))
        .text("Fold single-key object chains into dotted paths (default: off)."),
      opt[Int]("flatten-depth")
        .valueName("<n>")
        .validate(n => if (n >= 0) success else failure("flatten-depth must be non-negative"))
        .action((n, c) => c.copy(flattenDepth = n))
        .text("Maximum segments to fold when key-folding is safe (default: unlimited)."),
      opt[String]("expand-paths")
        .valueName("off|safe")
        .validate(v =>
          if (Set("off", "safe").contains(v.toLowerCase)) success
          else failure("expand-paths must be 'off' or 'safe'")
        )
        .action((v, c) => c.copy(expandPaths = v.toLowerCase))
        .text("Decode dotted keys into nested objects (default: off)."),
      opt[Unit]("optimize")
        .action((_, c) => c.copy(optimize = true, stats = true))
        .text("Optimize delimiter and key-folding for token savings (enables --stats)."),
      opt[Unit]("stats")
        .action((_, c) => c.copy(stats = true))
        .text("Print GPT token counts for input/output to stderr."),
      opt[String]("tokenizer")
        .valueName("cl100k|o200k|p50k|r50k")
        .action((name, c) => c.copy(tokenizer = name))
        .text("Tokenizer to use with --stats (default: cl100k)."),
      arg[String]("<input>")
        .required()
        .action((path, c) => c.copy(input = Paths.get(path)))
        .text("Input file path."),
    )
  }

  private def parseDelimiter(value: String): Option[Delimiter] =
    value.toLowerCase match {
    case "comma" | "," => Some(Delimiter.Comma)
    case "tab" | "\\t" => Some(Delimiter.Tab)
    case "pipe" | "|"  => Some(Delimiter.Pipe)
    case _             => None
    }

  private def asStrictness(s: String): Strictness = s.toLowerCase match {
  case "strict"  => Strictness.Strict
  case "lenient" => Strictness.Lenient
  case _         => Strictness.Strict // Default to strict for safety
  }

  private def asKeyFolding(s: String): KeyFolding = s.toLowerCase match {
  case "safe" => KeyFolding.Safe
  case _      => KeyFolding.Off
  }

  private def asExpandPaths(s: String): PathExpansion = s.toLowerCase match {
  case "safe" => PathExpansion.Safe
  case _      => PathExpansion.Off
  }

  private def optimize(
      scalaValue: Any,
      base: EncodeOptions,
      tokenizer: String,
  ): Either[String, EncodeOptions] = {
    val delimiters = List(Delimiter.Comma, Delimiter.Tab, Delimiter.Pipe)
    val foldingModes = List(KeyFolding.Off, KeyFolding.Safe)
    val candidates = for {
      d <- delimiters
      f <- foldingModes
    } yield base.copy(delimiter = d, keyFolding = f)
    val normalized = io.toonformat.toon4s.internal.Normalize.toJson(scalaValue)
    val scored = candidates.map {
      opt =>
        val str = io.toonformat.toon4s.encode.Encoders.encode(normalized, opt)
        val toks = token.TokenEstimator.estimateTokens(str, tokenizer)
        (opt, toks)
    }
    val best = scored.minBy(_._2)
    Right(best._1)
  }

}
