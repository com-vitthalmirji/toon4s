package io.toonformat.toon4s.cli

import munit.FunSuite
import scopt.OParser

class HelpTextSpec extends FunSuite {

  private val usage = OParser.usage(Main.parser)

  test("--strictness help names the spec version the decoder implements") {
    assert(usage.contains("TOON v3.3 §14"), usage)
  }

  test("the help text no longer refers to TOON v2.1") {
    assert(!usage.contains("v2.1"), usage)
  }

}
