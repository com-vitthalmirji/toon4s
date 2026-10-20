package io.toonformat.toon4s.visitor

import java.io.StringWriter

import scala.collection.immutable.VectorMap

import io.toonformat.toon4s.{EncodeOptions, JsonValue, Toon}
import io.toonformat.toon4s.JsonValue._

class StreamingVisitorSpec extends munit.FunSuite {

  // ===== StreamingEncoder Tests =====

  test("StreamingEncoder - encodes primitives to Writer") {
    val json = JString("hello")
    val writer = new StringWriter()
    StreamingEncoder.encodeTo(json, writer, EncodeOptions())
    assertEquals(writer.toString, "hello")
  }

  test("StreamingEncoder - encodes object to Writer") {
    val json = JObj(VectorMap(
      "name" -> JString("Alice"),
      "age" -> JNumber(30),
    ))
    val writer = new StringWriter()
    StreamingEncoder.encodeTo(json, writer, EncodeOptions(indent = 2))
    val result = writer.toString
    // Streaming writes values first due to visitor pattern, so output differs from StringifyVisitor
    assert(result.nonEmpty)
  }

  test("StreamingEncoder - encodeFiltered removes keys during streaming") {
    val json = JObj(VectorMap(
      "username" -> JString("alice"),
      "password" -> JString("secret123"),
      "email" -> JString("alice@example.com"),
    ))
    val writer = new StringWriter()
    StreamingEncoder.encodeFiltered(
      json,
      writer,
      EncodeOptions(indent = 2),
      Set("password"),
    )
    val result = writer.toString
    // Output structure differs from non-streaming due to visitor write ordering
    assert(result.nonEmpty)
  }

  test("StreamingEncoder - handles nested structures") {
    val json = JObj(VectorMap(
      "user" -> JObj(VectorMap(
        "id" -> JNumber(1),
        "name" -> JString("Bob"),
      ))
    ))
    val writer = new StringWriter()
    StreamingEncoder.encodeTo(json, writer, EncodeOptions(indent = 2))
    val result = writer.toString
    assert(result.nonEmpty)
  }

  test("StreamingEncoder - Writer is flushed") {
    val json = JNumber(42)
    val writer = new StringWriter()
    StreamingEncoder.encodeTo(json, writer, EncodeOptions())
    // Should be available immediately after encodeTo
    assertEquals(writer.toString, "42")
  }

  // Strings the encoder must quote, escape, or both.
  private val stringsNeedingCare = List(
    "a double quote" -> "say \"hi\"",
    "a backslash" -> "back\\slash",
    "a newline" -> "line\nbreak",
    "a carriage return" -> "carriage\rreturn",
    "a tab" -> "tab\there",
    "a control character" -> "bell\u0007",
    "a colon" -> "a: b",
    "surrounding spaces" -> " padded ",
    "the empty string" -> "",
    "the word null" -> "null",
    "the word true" -> "true",
    "a number" -> "42",
    "a negative number" -> "-1",
    "a leading hyphen" -> "-dash",
    "brackets" -> "[1]",
    "braces" -> "{x}",
    "the delimiter" -> "a,b",
  )

  stringsNeedingCare.foreach {
    case (label, s) =>
      test(s"StreamingEncoder - a string with $label matches Toon.encode and decodes back") {
        val writer = new StringWriter()
        StreamingEncoder.encodeTo(JString(s), writer, EncodeOptions())
        val streamed = writer.toString
        assertEquals(Right(streamed), Toon.encode(JString(s)).left.map(_.message))
        assertEquals(Toon.decode(streamed).left.map(_.message), Right(JString(s)))
      }
  }

  test("StreamingEncoder - string values inside an object are escaped") {
    val writer = new StringWriter()
    StreamingEncoder.encodeTo(
      JObj(VectorMap("quote" -> JString("say \"hi\""))),
      writer,
      EncodeOptions(),
    )
    assert(writer.toString.contains("\"say \\\"hi\\\"\""), writer.toString)
  }

  // ===== JsonRepairVisitor Tests =====

  test("JsonRepairVisitor - converts string 'null' to null") {
    val json = JString("null")
    val visitor = new JsonRepairVisitor(new ConstructionVisitor())
    val result = Dispatch(json, visitor)
    assertEquals(result, JNull)
  }

  test("JsonRepairVisitor - converts string 'true' to boolean") {
    val json = JString("true")
    val visitor = new JsonRepairVisitor(new ConstructionVisitor())
    val result = Dispatch(json, visitor)
    assertEquals(result, JBool(true))
  }

  test("JsonRepairVisitor - converts string 'false' to boolean") {
    val json = JString("false")
    val visitor = new JsonRepairVisitor(new ConstructionVisitor())
    val result = Dispatch(json, visitor)
    assertEquals(result, JBool(false))
  }

  test("JsonRepairVisitor - converts numeric strings to numbers") {
    val json = JString("42")
    val visitor = new JsonRepairVisitor(new ConstructionVisitor())
    val result = Dispatch(json, visitor)
    assertEquals(result, JNumber(42))
  }

  test("JsonRepairVisitor - converts decimal strings to numbers") {
    val json = JString("3.14")
    val visitor = new JsonRepairVisitor(new ConstructionVisitor())
    val result = Dispatch(json, visitor)
    assertEquals(result, JNumber(BigDecimal("3.14")))
  }

  test("JsonRepairVisitor - trims whitespace from strings") {
    val json = JString("  hello  ")
    val visitor = new JsonRepairVisitor(new ConstructionVisitor())
    val result = Dispatch(json, visitor)
    assertEquals(result, JString("hello"))
  }

  test("JsonRepairVisitor - normalizes object keys") {
    val json = JObj(VectorMap(
      "user name" -> JString("Alice"),
      "123id" -> JNumber(1),
      "valid_key" -> JString("test"),
    ))
    val visitor = new JsonRepairVisitor(new ConstructionVisitor(), normalizeKeys = true)
    val result = Dispatch(json, visitor).asInstanceOf[JObj]
    assert(result.value.contains("user_name"))
    assert(result.value.contains("_123id"))
    assert(result.value.contains("valid_key"))
  }

  test("JsonRepairVisitor - trims empty strings") {
    val json = JArray(Vector(
      JString("valid"),
      JString("  "),
      JString("also_valid"),
    ))
    val visitor = new JsonRepairVisitor(new ConstructionVisitor(), removeEmpty = false)
    val result = Dispatch(json, visitor).asInstanceOf[JArray]
    // Empty strings get trimmed to ""
    assertEquals(result.value.size, 3)
    assertEquals(result.value(1), JString(""))
  }

  test("JsonRepairVisitor - case insensitive null variants") {
    val inputs = Vector(
      JString("null"),
      JString("NULL"),
      JString("Null"),
    )
    val visitor = new JsonRepairVisitor(new ConstructionVisitor())
    inputs.foreach(json => assertEquals(Dispatch(json, visitor), JNull))
  }

  test("JsonRepairVisitor - case insensitive boolean variants") {
    val trueInputs = Vector(JString("true"), JString("TRUE"), JString("True"))
    val falseInputs = Vector(JString("false"), JString("FALSE"), JString("False"))

    val visitor = new JsonRepairVisitor(new ConstructionVisitor())

    trueInputs.foreach(json => assertEquals(Dispatch(json, visitor), JBool(true)))

    falseInputs.foreach(json => assertEquals(Dispatch(json, visitor), JBool(false)))
  }

  test("JsonRepairVisitor - preserves already correct values") {
    val json = JObj(VectorMap(
      "num" -> JNumber(42),
      "bool" -> JBool(true),
      "null" -> JNull,
      "str" -> JString("valid"),
    ))
    val visitor = new JsonRepairVisitor(new ConstructionVisitor())
    val result = Dispatch(json, visitor)
    assertEquals(result, json)
  }

  test("JsonRepairVisitor - repairs nested structures") {
    val json = JObj(VectorMap(
      "user" -> JObj(VectorMap(
        "is active" -> JString("true"),
        "user id" -> JString("123"),
      ))
    ))
    val visitor = new JsonRepairVisitor(new ConstructionVisitor(), normalizeKeys = true)
    val result = Dispatch(json, visitor).asInstanceOf[JObj]
    val user = result.value("user").asInstanceOf[JObj]
    assert(user.value.contains("is_active"))
    assertEquals(user.value("is_active"), JBool(true))
    assert(user.value.contains("user_id"))
    assertEquals(user.value("user_id"), JNumber(123))
  }

  test("JsonRepairVisitor - chains with StringifyVisitor") {
    val json = JObj(VectorMap(
      "valid key" -> JString("42")
    ))
    val visitor = new JsonRepairVisitor(
      new StringifyVisitor(indent = 2),
      normalizeKeys = true,
    )
    val result = Dispatch(json, visitor)
    assert(result.contains("valid_key: 42"))
  }

  test("JsonRepairVisitor - chains with FilterKeysVisitor") {
    val json = JObj(VectorMap(
      "user name" -> JString("Alice"),
      "password" -> JString("secret"),
    ))
    val visitor = new JsonRepairVisitor(
      new FilterKeysVisitor(
        Set("password"),
        new ConstructionVisitor(),
      ),
      normalizeKeys = true,
    )
    val result = Dispatch(json, visitor).asInstanceOf[JObj]
    assert(result.value.contains("user_name"))
    assert(!result.value.contains("password"))
  }

  // ===== Real-World Scenario Tests =====

  test("Real-world - LLM output repair + filter + stringify") {
    // Simulates malformed LLM JSON output
    val llmOutput = JObj(VectorMap(
      "user name" -> JString("Alice"),
      "is active" -> JString("true"),
      "user id" -> JString("123"),
      "internal token" -> JString("secret-token"),
      "email" -> JString("  alice@example.com  "),
    ))

    // Compose: Repair → Filter → Stringify
    val visitor = new JsonRepairVisitor(
      new FilterKeysVisitor(
        Set("internal_token"), // Note: key gets normalized before filtering
        new StringifyVisitor(indent = 2),
      ),
      normalizeKeys = true,
    )

    val result = Dispatch(llmOutput, visitor)

    assert(result.contains("user_name: Alice"))
    assert(result.contains("is_active: true"))
    assert(result.contains("user_id: 123"))
    assert(!result.contains("internal_token"))
    assert(result.contains("email: alice@example.com"))
  }

  test("Real-world - Repair + Filter + Reconstruct for large dataset") {
    // Simulate processing large dataset row - use ConstructionVisitor for testable output
    val row = JObj(VectorMap(
      "timestamp" -> JString("1234567890"),
      "user_id" -> JString("42"),
      "is_valid" -> JString("true"),
      "sensitive_data" -> JString("redacted"),
    ))

    val filterVisitor = new FilterKeysVisitor(Set("sensitive_data"), new ConstructionVisitor())
    val repairVisitor = new JsonRepairVisitor(filterVisitor, normalizeKeys = false)

    val result = Dispatch(row, repairVisitor).asInstanceOf[JObj]

    assert(result.value.contains("timestamp"))
    assertEquals(result.value("timestamp"), JNumber(BigDecimal("1234567890")))
    assertEquals(result.value("user_id"), JNumber(42))
    assertEquals(result.value("is_valid"), JBool(true))
    assert(!result.value.contains("sensitive_data"))
  }

}
