package io.toonformat.toon4s.cli

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import munit.FunSuite
import scopt.OParser

class StdinInputSpec extends FunSuite {

  private def stream(text: String) = new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8))

  private def tempFile(suffix: String, content: String): Path = {
    val path = Files.createTempFile("toon4s-cli", suffix)
    path.toFile.deleteOnExit()
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
  }

  private def read(path: Path): String =
    new String(Files.readAllBytes(path), StandardCharsets.UTF_8)

  test("- reads standard input as UTF-8") {
    assertEquals(
      Main.readUtf8(Paths.get("-"), stream("""{"name":"Zoë"}""")),
      Right("""{"name":"Zoë"}"""),
    )
  }

  test("a file path is still read from the file, not standard input") {
    val path = tempFile(".json", """{"a":1}""")
    assertEquals(Main.readUtf8(path, stream("ignored")), Right("""{"a":1}"""))
  }

  test("- is accepted as the input argument") {
    assertEquals(
      OParser.parse(Main.parser, Seq("--encode", "-"), Main.Config()).map(_.input),
      Some(Paths.get("-")),
    )
  }

  test("encoding from standard input writes the TOON encoding") {
    val output = tempFile(".toon", "")
    val original = System.in
    System.setIn(stream("""{"tags":["jazz","chill","lofi"]}"""))
    val result =
      try Main.run(Main.Config(
          mode = Some(Main.EncodeMode),
          input = Paths.get("-"),
          output = Some(output),
        ))
      finally System.setIn(original)
    assertEquals(result, Right(()))
    assertEquals(read(output), "tags[3]: jazz,chill,lofi")
  }

  test("decoding from standard input writes the JSON") {
    val output = tempFile(".json", "")
    val original = System.in
    System.setIn(stream("name: Ada\nage: 36"))
    val result =
      try Main.run(Main.Config(
          mode = Some(Main.DecodeMode),
          input = Paths.get("-"),
          output = Some(output),
        ))
      finally System.setIn(original)
    assertEquals(result, Right(()))
    assertEquals(read(output), """{"name":"Ada","age":36}""")
  }

}
