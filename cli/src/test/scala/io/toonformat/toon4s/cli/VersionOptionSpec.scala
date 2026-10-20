package io.toonformat.toon4s.cli

import munit.FunSuite
import scopt.{OEffect, OParser}

class VersionOptionSpec extends FunSuite {

  private def effects(args: String*): List[OEffect] =
    OParser.runParser(Main.parser, args, Main.Config())._2

  test("the build version is a release-style version") {
    assert(BuildVersion.value.matches("""\d+\.\d+\.\d+.*"""), BuildVersion.value)
  }

  test("--version prints the program name and build version, then exits successfully") {
    // scopt runs effects in order and exits at Terminate, so the missing-<input> error it also
    // collects is never shown; only the effects up to and including Terminate matter.
    val shown = effects("--version").span(e => !e.isInstanceOf[OEffect.Terminate])
    assertEquals(
      shown._1 :+ shown._2.head,
      List(OEffect.DisplayToOut(s"toon4s-cli ${BuildVersion.value}"), OEffect.Terminate(Right(()))),
    )
  }

  test("--help starts with the program name and version") {
    val shown = effects("--help").collect { case OEffect.DisplayToOut(text) => text }
    assert(shown.exists(_.startsWith(s"toon4s-cli ${BuildVersion.value}")), shown.mkString("\n"))
  }

  test("--help lists --version") {
    assert(OParser.usage(Main.parser).contains("--version"), OParser.usage(Main.parser))
  }

}
