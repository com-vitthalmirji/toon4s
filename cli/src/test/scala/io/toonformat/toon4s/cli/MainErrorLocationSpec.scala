package io.toonformat.toon4s.cli

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import munit.FunSuite

class MainErrorLocationSpec extends FunSuite {

  private def tempFile(suffix: String, content: String): Path = {
    val path = Files.createTempFile("toon4s-cli", suffix)
    path.toFile.deleteOnExit()
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
  }

  private def decode(input: Path, output: Option[Path] = None): Either[String, Unit] =
    Main.run(Main.Config(mode = Some(Main.DecodeMode), input = input, output = output))

  test("a decode error reports the line and column it was found at") {
    val result = decode(tempFile(".toon", "key: value\n\tnested: wrong"))
    result match {
    case Left(msg) =>
      assert(msg.matches("(?s)^2:\\d+: .*"), s"expected a line:column prefix, got: $msg")
      assert(msg.contains("Tabs are not allowed"), msg)
    case Right(_) => fail("expected the decode to fail")
    }
  }

  test("a decode error includes the offending line as a snippet") {
    val result = decode(tempFile(".toon", "key: value\n\tnested: wrong"))
    assert(result.left.exists(_.contains("nested: wrong")), result.toString)
  }

  test("a valid document still decodes to JSON") {
    val output = tempFile(".json", "")
    assertEquals(decode(tempFile(".toon", "name: Ada\nage: 36"), Some(output)), Right(()))
    assertEquals(
      new String(Files.readAllBytes(output), StandardCharsets.UTF_8),
      """{"name":"Ada","age":36}""",
    )
  }

}
