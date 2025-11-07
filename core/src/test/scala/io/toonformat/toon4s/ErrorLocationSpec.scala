package io.toonformat.toon4s

import io.toonformat.toon4s.error.DecodeError
import munit.FunSuite

/** Tests for error location reporting (P2.5).
  *
  * Verifies that errors include line/column information and contextual snippets.
  */
class ErrorLocationSpec extends FunSuite {

  test("tab in indentation error shows line and column") {
    val toon = """
key: value
	nested: wrong
""".trim

    val result = Toon.decode(toon)
    assert(result.isLeft, "Should fail on tab in indentation")
    result.left.foreach {
      err =>
        val msg = err.getMessage
        assert(msg.contains("2:"), s"Error should include line number: $msg")
        assert(msg.contains("Tabs are not allowed"), s"Error message: $msg")
        assert(msg.contains("nested"), s"Error should include snippet: $msg")
    }
  }

  test("invalid indentation shows line and column") {
    val toon = """
key: value
   nested: wrong
""".trim

    val result = Toon.decode(toon, DecodeOptions(indent = 2))
    assert(result.isLeft, "Should fail on invalid indentation")
    result.left.foreach {
      err =>
        val msg = err.getMessage
        assert(msg.contains("2:"), s"Error should include line number: $msg")
        assert(msg.contains("Indentation must be exact multiple"), s"Error message: $msg")
    }
  }

  test("Scanner errors include location information") {
    // This test verifies that errors caught during scanning (like tab/indentation errors)
    // include location info since Scanner has access to line numbers and raw content
    val toon1 = "\tkey: value" // Tab at start

    val result1 = Toon.decode(toon1)
    assert(result1.isLeft, "Should fail on tab")
    result1.left.foreach {
      err =>
        val msg = err.getMessage
        assert(msg.contains("1:"), s"Error should include line number: $msg")
    }
  }

  test("error without location still works") {
    val toon = "arr[5]: 1,2,3"

    val result = Toon.decode(toon)
    assert(result.isLeft, "Should fail on array length mismatch")
    result.left.foreach {
      err =>
        assert(err.isInstanceOf[DecodeError.Range], s"Should be Range error: $err")
        // This error may not have location info since it's a validation error
        assert(err.getMessage.contains("Expected"), s"Error message: ${err.getMessage}")
    }
  }

  test("error location toString format") {
    val loc = error.ErrorLocation(5, 12, "key value")
    assertEquals(loc.toString, "5:12")
  }

  test("error getMessage includes location") {
    val loc = error.ErrorLocation(5, 12, "key: value")
    val err = error.DecodeError.Syntax("Missing colon after key", Some(loc))

    val msg = err.getMessage
    assert(msg.contains("5:12:"), s"Message should include location: $msg")
    assert(msg.contains("Missing colon after key"), s"Message should include error text: $msg")
    assert(msg.contains("key: value"), s"Message should include snippet: $msg")
  }

  test("error getMessage without location") {
    val err = error.DecodeError.Syntax("Missing colon after key", None)

    val msg = err.getMessage
    assertEquals(msg, "Missing colon after key")
    assert(!msg.contains(":"), "Message should not contain location markers")
  }
}
