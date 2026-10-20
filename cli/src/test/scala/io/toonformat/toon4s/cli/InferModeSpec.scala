package io.toonformat.toon4s.cli

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import munit.FunSuite

class InferModeSpec extends FunSuite {

  private def tempFile(suffix: String, content: String): Path = {
    val path = Files.createTempFile("toon4s-cli", suffix)
    path.toFile.deleteOnExit()
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
  }

  private def read(path: Path): String = new String(Files.readAllBytes(path), StandardCharsets.UTF_8)

  private def run(input: Path, mode: Option[Main.Mode] = None): (Either[String, Unit], String) = {
    val output = tempFile(".out", "")
    (Main.run(Main.Config(mode = mode, input = input, output = Some(output))), read(output))
  }

  test("a .json input is encoded") {
    assertEquals(Main.inferMode(Paths.get("data.json")), Some(Main.EncodeMode))
  }

  test("a .toon input is decoded") {
    assertEquals(Main.inferMode(Paths.get("data.toon")), Some(Main.DecodeMode))
  }

  test("extensions are matched case-insensitively") {
    assertEquals(Main.inferMode(Paths.get("DATA.JSON")), Some(Main.EncodeMode))
    assertEquals(Main.inferMode(Paths.get("dir/Data.Toon")), Some(Main.DecodeMode))
  }

  test("other names, and standard input, are not inferred") {
    List("data.txt", "data", "json", "data.json.bak", "-").foreach {
      name => assertEquals(Main.inferMode(Paths.get(name)), None, name)
    }
  }

  test("running without a flag encodes a .json file") {
    assertEquals(run(tempFile(".json", """{"tags":["jazz","chill"]}""")), (Right(()), "tags[2]: jazz,chill"))
  }

  test("running without a flag decodes a .toon file") {
    assertEquals(run(tempFile(".toon", "name: Ada")), (Right(()), """{"name":"Ada"}"""))
  }

  test("an explicit flag wins over the extension") {
    // TOON content in a .json file, decoded because --decode says so
    assertEquals(run(tempFile(".json", "name: Ada"), Some(Main.DecodeMode)), (Right(()), """{"name":"Ada"}"""))
  }

  test("an unknown extension without a flag asks for --encode or --decode") {
    val (result, _) = run(tempFile(".txt", "name: Ada"))
    assertEquals(result, Left("Please specify --encode or --decode (or use a .json or .toon input file)"))
  }

}
