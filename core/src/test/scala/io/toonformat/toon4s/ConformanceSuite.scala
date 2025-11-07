package io.toonformat.toon4s

import io.toonformat.toon4s.JsonValue
import io.toonformat.toon4s.JsonValue._
import io.toonformat.toon4s.json.SimpleJson
import io.toonformat.toon4s.{DecodeOptions, Delimiter, EncodeOptions, Toon}
import java.nio.file.{Files, Path, Paths}
import munit.FunSuite
import scala.jdk.CollectionConverters._
import scala.util.Using

class ConformanceSuite extends FunSuite {
  private val decodeFixtures = loadDecodeFixtures(resourcePath("conformance/decode"))
  private val encodeFixtures = loadEncodeFixtures(resourcePath("conformance/encode"))

  decodeFixtures.foreach {
    case (fileBase, cases) =>
      cases.foreach {
        testCase =>
          val displayName = s"decode/$fileBase/${testCase.name}"
          test(displayName) {
            val result = Toon.decode(testCase.input, testCase.options)
            if (testCase.shouldError) assert(result.isLeft, s"Expected failure for $displayName")
            else {
              val expected =
                testCase.expected.getOrElse(fail(s"Missing expected value for $displayName"))
              result match {
                case Right(actual) => assertEquals(actual, expected)
                case Left(err)     => fail(s"Unexpected decode failure: ${err.message}")
              }
            }
          }
      }
  }

  encodeFixtures.foreach {
    case (fileBase, cases) =>
      cases.foreach {
        testCase =>
          val displayName = s"encode/$fileBase/${testCase.name}"
          test(displayName) {
            val scalaValue = SimpleJson.toScala(testCase.input)
            Toon
              .encode(scalaValue, testCase.options)
              .fold(
                err => fail(s"Unexpected encode failure: ${err.message}"),
                actual => assertEquals(actual, testCase.expected)
              )
          }
      }
  }

  private case class DecodeCase(
      name: String,
      input: String,
      expected: Option[JsonValue],
      shouldError: Boolean,
      options: DecodeOptions
  )

  private case class EncodeCase(
      name: String,
      input: JsonValue,
      expected: String,
      options: EncodeOptions
  )

  private def loadDecodeFixtures(dir: Path): Vector[(String, Vector[DecodeCase])] = {
    listJsonFiles(dir).map {
      path =>
        val fixture = parseFixture(path)
        val tests   = extractTests(fixture)
        val cases   = tests.map {
          testValue =>
            val obj         = asObject(testValue, s"test entry in ${path.getFileName}")
            val name        = asString(obj.get("name"), "name")
            val input       = asString(obj.get("input"), "input")
            val expectedOpt = obj.get("expected").map(identity)
            val shouldError = obj.get("shouldError").exists(asBoolean(_, "shouldError"))
            val options     = obj.get("options").map(parseDecodeOptions).getOrElse(DecodeOptions())
            DecodeCase(name, input, expectedOpt, shouldError, options)
        }
        path.getFileName.toString.stripSuffix(".json") -> cases
    }
  }

  private def loadEncodeFixtures(dir: Path): Vector[(String, Vector[EncodeCase])] = {
    listJsonFiles(dir).map {
      path =>
        val fixture = parseFixture(path)
        val tests   = extractTests(fixture)
        val cases   = tests.map {
          testValue =>
            val obj      = asObject(testValue, s"test entry in ${path.getFileName}")
            val name     = asString(obj.get("name"), "name")
            val inputAst = obj.get("input").getOrElse(fail(s"Missing input in $name"))
            val expected = asString(obj.get("expected"), "expected")
            val options  = obj.get("options").map(parseEncodeOptions).getOrElse(EncodeOptions())
            EncodeCase(name, inputAst, expected, options)
        }
        path.getFileName.toString.stripSuffix(".json") -> cases
    }
  }

  private def parseFixture(path: Path): JsonValue = {
    val content = Files.readString(path)
    SimpleJson.parse(content)
  }

  private def extractTests(fixture: JsonValue): Vector[JsonValue] = {
    val obj        = asObject(fixture, "fixture root")
    val testsValue = obj.getOrElse("tests", fail("Fixture missing tests array"))
    asArray(testsValue, "tests array")
  }

  private def parseDecodeOptions(value: JsonValue): DecodeOptions = {
    val obj    = asObject(value, "decode options")
    val indent = obj.get("indent").map(asInt(_, "indent"))
    val strict = obj.get("strict").map(asBoolean(_, "strict"))
    DecodeOptions(indent = indent.getOrElse(2), strict = strict.getOrElse(true))
  }

  private def parseEncodeOptions(value: JsonValue): EncodeOptions = {
    val obj        = asObject(value, "encode options")
    val indentOpt  = obj.get("indent").map(asInt(_, "indent"))
    val delimiter  = obj
      .get("delimiter")
      .map(
        v => asString(Some(v), "delimiter")
      )
      .map(parseDelimiter)
      .getOrElse(Delimiter.Comma)
    val lengthFlag = obj.get("lengthMarker").exists {
      case JString(s) => s.contains('#')
      case _          => false
    }
    EncodeOptions(
      indent = indentOpt.getOrElse(2),
      delimiter = delimiter,
      lengthMarker = lengthFlag
    )
  }

  private def parseDelimiter(value: String): Delimiter = value match {
    case "\t"  => Delimiter.Tab
    case "|"   => Delimiter.Pipe
    case ","   => Delimiter.Comma
    case other => throw new IllegalArgumentException(s"Unsupported delimiter: '$other'")
  }

  private def asObject(value: JsonValue, context: String): Map[String, JsonValue] = value match {
    case JObj(fields) => fields
    case other        => fail(s"Expected object for $context but found $other")
  }

  private def asArray(value: JsonValue, context: String): Vector[JsonValue] = value match {
    case JArray(values) => values
    case other          => fail(s"Expected array for $context but found $other")
  }

  private def asString(value: Option[JsonValue], context: String): String = value match {
    case Some(JString(v)) => v
    case Some(other)      => fail(s"Expected string for $context but found $other")
    case None             => fail(s"Missing $context")
  }

  private def asBoolean(value: JsonValue, context: String): Boolean = value match {
    case JBool(b) => b
    case other    => fail(s"Expected boolean for $context but found $other")
  }

  private def asInt(value: JsonValue, context: String): Int = value match {
    case JNumber(n) => n.toInt
    case other      => fail(s"Expected number for $context but found $other")
  }

  private def resourcePath(name: String): Path = {
    val url = Option(getClass.getClassLoader.getResource(name))
      .getOrElse(fail(s"Missing resource directory: $name"))
    Paths.get(url.toURI)
  }

  private def listJsonFiles(dir: Path): Vector[Path] =
    Using.resource(Files.list(dir)) {
      _.iterator()
        .asScala
        .filter(_.getFileName.toString.endsWith(".json"))
        .toVector
        .sortBy(_.getFileName.toString)
    }
}
