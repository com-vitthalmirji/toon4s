package io.toonformat.toon4s.cli

import io.toonformat.toon4s.cli.token.TokenEstimator
import munit.FunSuite
import scopt.OParser

class TokenizerOptionSpec extends FunSuite {

  private def parse(args: String*): Option[Main.Config] =
    OParser.parse(Main.parser, args, Main.Config())

  test("the tokenizer defaults to cl100k") {
    assertEquals(parse("--encode", "in.json").map(_.tokenizer), Some("cl100k"))
  }

  TokenEstimator.supportedNames.foreach {
    name =>
      test(s"--tokenizer $name is accepted") {
        assertEquals(parse("--encode", "--tokenizer", name, "in.json").map(_.tokenizer), Some(name))
      }
  }

  test("tokenizer names are matched case-insensitively") {
    assertEquals(parse("--encode", "--tokenizer", "O200K_BASE", "in.json").map(_.tokenizer), Some("O200K_BASE"))
  }

  List("o200", "gpt-4", "cl100k-base", "").foreach {
    name =>
      test(s"--tokenizer '$name' is rejected") {
        assertEquals(parse("--encode", "--tokenizer", name, "in.json"), None)
      }
  }

  test("every accepted name resolves to its own encoding") {
    val expected = Map(
      "cl100k" -> "CL100K_BASE",
      "o200k" -> "O200K_BASE",
      "p50k" -> "P50K_BASE",
      "r50k" -> "R50K_BASE",
    )
    TokenEstimator.supportedNames.foreach {
      name =>
        assertEquals(TokenEstimator.canonicalName(name), expected(name.stripSuffix("_base")), name)
    }
  }

}
