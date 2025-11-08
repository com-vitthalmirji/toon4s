package io.toonformat.toon4s

import io.toonformat.toon4s.error.DecodeError
import munit.FunSuite

/** Tests for P0.4: Input size limits to prevent DoS attacks. */
class InputSizeLimitsSpec extends FunSuite {

  test("rejects deeply nested structures exceeding maxDepth") {
    val options = DecodeOptions(maxDepth = Some(3))
    // Create nested structure with depth > 3
    val toon = """
a:
  b:
    c:
      d:
        e: value
""".trim

    val result = Toon.decode(toon, options)
    assert(result.isLeft, s"Should reject deep nesting, got: $result")
    result.left.foreach {
      err =>
        assert(err.message.contains("nesting depth"), s"Error should mention depth: ${err.message}")
    }
  }

  test("accepts nested structures within maxDepth limit") {
    val options = DecodeOptions(maxDepth = Some(5))
    // Create nested structure: obj -> obj -> obj -> obj -> obj (5 levels)
    val toon = """
a:
  b:
    c:
      d:
        e: value
""".trim

    val result = Toon.decode(toon, options)
    assert(result.isRight, s"Should accept nesting within limit: ${result.left.map(_.message)}")
  }

  test("rejects arrays exceeding maxArrayLength") {
    val options = DecodeOptions(maxArrayLength = Some(3))
    val toon = "arr[5]: 1,2,3,4,5"

    val result = Toon.decode(toon, options)
    assert(result.isLeft, "Should reject large array")
    result.left.foreach {
      err =>
        assert(
          err.message.contains("array length"),
          s"Error should mention array length: ${err.message}",
        )
    }
  }

  test("accepts arrays within maxArrayLength limit") {
    val options = DecodeOptions(maxArrayLength = Some(5))
    val toon = "arr[5]: 1,2,3,4,5"

    val result = Toon.decode(toon, options)
    assert(result.isRight, s"Should accept array within limit: ${result.left.map(_.message)}")
  }

  test("rejects strings exceeding maxStringLength") {
    val options = DecodeOptions(maxStringLength = Some(10))
    val longString = "a" * 100
    val toon = s"""text: "$longString""""

    val result = Toon.decode(toon, options)
    assert(result.isLeft, "Should reject long string")
    result.left.foreach {
      err =>
        assert(
          err.message.contains("string length"),
          s"Error should mention string length: ${err.message}",
        )
    }
  }

  test("accepts strings within maxStringLength limit") {
    val options = DecodeOptions(maxStringLength = Some(100))
    val shortString = "a" * 50
    val toon = s"""text: "$shortString""""

    val result = Toon.decode(toon, options)
    assert(result.isRight, s"Should accept string within limit: ${result.left.map(_.message)}")
  }

  test("allows unlimited depth when maxDepth = None") {
    val options = DecodeOptions(maxDepth = None)
    // Create very deep nesting
    val toon = """
a:
  b:
    c:
      d:
        e:
          f:
            g:
              h:
                i:
                  j: value
""".trim

    val result = Toon.decode(toon, options)
    assert(result.isRight, "Should accept unlimited depth")
  }

  test("allows unlimited array length when maxArrayLength = None") {
    val options = DecodeOptions(maxArrayLength = None)
    val toon = "arr[10000]: " + (1 to 10000).mkString(",")

    val result = Toon.decode(toon, options)
    assert(result.isRight, "Should accept unlimited array length")
  }

  test("allows unlimited string length when maxStringLength = None") {
    val options = DecodeOptions(maxStringLength = None)
    val longString = "a" * 100000
    val toon = s"""text: "$longString""""

    val result = Toon.decode(toon, options)
    assert(result.isRight, "Should accept unlimited string length")
  }

  test("tabular arrays respect maxArrayLength") {
    val options = DecodeOptions(maxArrayLength = Some(2))
    val toon = """
users[3]{id,name}:
  1,Alice
  2,Bob
  3,Charlie
""".trim

    val result = Toon.decode(toon, options)
    assert(result.isLeft, "Should reject tabular array exceeding limit")
  }

  test("list arrays respect maxArrayLength") {
    val options = DecodeOptions(maxArrayLength = Some(2))
    val toon = """
items[3]:
  - apple
  - banana
  - cherry
""".trim

    val result = Toon.decode(toon, options)
    assert(result.isLeft, "Should reject list array exceeding limit")
  }

}
