package io.toonformat.toon4s.cli

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import io.toonformat.toon4s.Delimiter
import io.toonformat.toon4s.json.SimpleJson
import munit.FunSuite
import scopt.OParser

// Option parsing, a JSON -> TOON -> JSON round trip in each mode, and the failure paths of run.
class MainSpec extends FunSuite {

  private def tempFile(suffix: String, content: String): Path = {
    val path = Files.createTempFile("toon4s-cli", suffix)
    path.toFile.deleteOnExit()
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
  }

  private def read(path: Path): String =
    new String(Files.readAllBytes(path), StandardCharsets.UTF_8)

  // scopt reports a rejected argument list on stderr; keep the test output clean
  private def parse(args: String*): Option[Main.Config] = {
    val sink = new PrintStream(new ByteArrayOutputStream())
    Console.withErr(sink)(OParser.parse(Main.parser, args, Main.Config()))
  }

  private val json =
    """{"users":[{"id":1,"name":"Ada","active":true},{"id":2,"name":"Bob","active":false}],""" +
      """"meta":{"source":{"system":"crm"}},"tags":["a","b"],"note":null}"""

  // --- parsing ---------------------------------------------------------------

  test("encode options are parsed into the config") {
    val config = parse(
      "--encode",
      "-o",
      "out.toon",
      "--indent",
      "4",
      "--delimiter",
      "pipe",
      "--key-folding",
      "safe",
      "--flatten-depth",
      "2",
      "in.json",
    )
    assertEquals(
      config,
      Some(Main.Config(
        mode = Some(Main.EncodeMode),
        input = Paths.get("in.json"),
        output = Some(Paths.get("out.toon")),
        indent = 4,
        delimiter = Delimiter.Pipe,
        keyFolding = "safe",
        flattenDepth = 2,
      )),
    )
  }

  test("decode options are parsed into the config, case-insensitively") {
    val config = parse("--decode", "--strictness", "LENIENT", "--expand-paths", "Safe", "in.toon")
    assertEquals(config.map(_.mode), Some(Some(Main.DecodeMode)))
    assertEquals(config.map(c => (c.strictness, c.expandPaths)), Some(("lenient", "safe")))
  }

  test("--optimize also turns on --stats") {
    assertEquals(parse("--optimize", "in.json").map(c => (c.optimize, c.stats)), Some((true, true)))
  }

  test("invalid argument lists are rejected") {
    List(
      Seq("--encode"), // no input
      Seq("--encode", "--indent", "0", "in.json"),
      Seq("--encode", "--indent", "two", "in.json"),
      Seq("--encode", "--delimiter", "semicolon", "in.json"),
      Seq("--decode", "--strictness", "loose", "in.toon"),
      Seq("--encode", "--key-folding", "deep", "in.json"),
      Seq("--encode", "--flatten-depth", "-1", "in.json"),
      Seq("--decode", "--expand-paths", "all", "in.toon"),
      Seq("--encode", "--no-such-option", "in.json"),
      Seq("--encode", "a.json", "b.json"), // one input only
    ).foreach(args => assertEquals(parse(args: _*), None, args.mkString(" ")))
  }

  // --- round trips -----------------------------------------------------------

  // returns the TOON written in between
  private def roundTrip(encodeArgs: Seq[String], decodeArgs: Seq[String]): String = {
    val input = tempFile(".json", json)
    val toon = tempFile(".toon", "")
    val back = tempFile(".json", "")
    val encode = parse(encodeArgs ++ Seq("-o", toon.toString, input.toString): _*).get
    assertEquals(Main.run(encode), Right(()))
    val decode = parse(decodeArgs ++ Seq("-o", back.toString, toon.toString): _*).get
    assertEquals(Main.run(decode), Right(()))
    assert(!read(toon).trim.startsWith("{"), s"expected TOON, got:\n${read(toon)}")
    assertEquals(SimpleJson.parse(read(back)), SimpleJson.parse(json))
    read(toon)
  }

  test("encode then decode gives back the same JSON") {
    roundTrip(Seq("--encode"), Seq("--decode"))
    ()
  }

  test("the round trip holds with a pipe delimiter and a wider indent") {
    val toon = roundTrip(
      Seq("--encode", "--delimiter", "pipe", "--indent", "4"),
      Seq("--decode", "--indent", "4"),
    )
    assert(toon.contains("|"), toon)
  }

  test("folded keys come back as nested objects when paths are expanded") {
    val toon =
      roundTrip(Seq("--encode", "--key-folding", "safe"), Seq("--decode", "--expand-paths", "safe"))
    assert(toon.contains("meta.source.system"), toon)
  }

  test("the modes are inferred from the file extensions") {
    roundTrip(Nil, Nil)
    ()
  }

  // --- failure paths ---------------------------------------------------------

  test("invalid JSON fails the encode with a message") {
    val result =
      Main.run(Main.Config(mode = Some(Main.EncodeMode), input = tempFile(".json", "{not json")))
    assert(result.left.exists(_.startsWith("Invalid JSON input:")), result)
  }

  test("a missing input file fails with a message") {
    val result =
      Main.run(Main.Config(mode = Some(Main.EncodeMode), input = Paths.get("does-not-exist.json")))
    assert(result.left.exists(_.startsWith("Failed to read input:")), result)
  }

  test("an output path that cannot be written fails with a message") {
    val blocker = tempFile(".file", "")
    val result = Main.run(Main.Config(
      mode = Some(Main.EncodeMode),
      input = tempFile(".json", json),
      output = Some(blocker.resolve("out.toon")), // a file's child cannot be created
    ))
    assert(result.left.exists(_.startsWith("Failed to write output:")), result)
  }

}
