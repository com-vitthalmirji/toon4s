package io.toonformat.toon4s.visitor

import io.toonformat.toon4s.encode.Primitives

/**
 * Terminal visitor that converts JsonValue to TOON format string representation.
 *
 * This is a '''terminal visitor''' - it produces final String output rather than forwarding to
 * another visitor. StringifyVisitor demonstrates zero-overhead encoding by building the output
 * string directly during traversal without creating intermediate structures.
 *
 * ==Key features==
 *   - '''Zero intermediate trees''': Encodes directly to String during traversal
 *   - '''Configurable indentation''': Supports custom indent sizes
 *   - '''Type-safe''': Generic type T = String enforced throughout
 *   - '''Composable''': Can be wrapped by intermediate visitors for transformations
 *
 * ==Usage==
 * {{{
 * import io.toonformat.toon4s.visitor._
 * import io.toonformat.toon4s.JsonValue._
 * import scala.collection.immutable.VectorMap
 *
 * val json = JObj(VectorMap(
 *   "name" -> JString("Alice"),
 *   "age" -> JNumber(30)
 * ))
 *
 * val visitor = new StringifyVisitor(indent = 2)
 * val toon: String = Dispatch(json, visitor)
 * // Produces:
 * // name: Alice
 * // age: 30
 * }}}
 *
 * ==Performance characteristics==
 *   - '''Time Complexity''': O(n) where n is tree size
 *   - '''Space Complexity''': O(d) for call stack where d is depth
 *   - '''Allocations''': One StringBuilder per object/array (reused via pooling in future
 *     optimization)
 *
 * @param indent
 *   Number of spaces per indentation level (default: 2)
 *
 * @see
 *   [[Visitor]] for the visitor pattern interface
 * @see
 *   [[ObjectVisitor]] for object processing
 * @see
 *   [[Dispatch]] for the dispatcher
 */
final class StringifyVisitor(indent: Int = 2) extends Visitor[String] {

  /**
   * Visit a string value.
   *
   * Quotes the string if necessary (contains special characters or whitespace).
   *
   * @param value
   *   The string content
   * @return
   *   The string in TOON format (quoted if necessary)
   */
  override def visitString(value: String): String = {
    // Use Primitives.quoteAndEscape for single-pass quoting and escaping
    // Avoids intermediate string allocations from concatenation
    if (needsQuoting(value)) {
      Primitives.quoteAndEscape(value)
    } else {
      value
    }
  }

  /**
   * Visit a numeric value.
   *
   * @param value
   *   The numeric value as BigDecimal
   * @return
   *   The string representation of the number
   */
  override def visitNumber(value: BigDecimal): String = value.toString

  /**
   * Visit a boolean value.
   *
   * @param value
   *   The boolean value
   * @return
   *   "true" or "false"
   */
  override def visitBool(value: Boolean): String = value.toString

  /**
   * Visit a null value.
   *
   * @return
   *   "null"
   */
  override def visitNull(): String = "null"

  /**
   * Visit an array value.
   *
   * Creates either an inline array (single line) or list array (multiline) depending on content
   * complexity.
   *
   * @param elements
   *   The processed array elements as strings
   * @return
   *   The array in TOON format
   */
  override def visitArray(elements: Vector[String]): String = {
    if (elements.isEmpty) {
      s"arr[0]:"
    } else if (elements.forall(isSimplePrimitive)) {
      // Inline array format: arr[N]: val1,val2,val3
      s"arr[${elements.size}]: ${elements.mkString(",")}"
    } else {
      // List array format:
      // arr[N]:
      //   - val1
      //   - val2
      val listItems = elements.map(elem => s"${" " * indent}- $elem").mkString("\n")
      s"arr[${elements.size}]:\n$listItems"
    }
  }

  /**
   * Begin visiting an object.
   *
   * @return
   *   An ObjectVisitor that accumulates key-value pairs
   */
  override def visitObject(): ObjectVisitor[String] = new StringifyObjectVisitor(indent)

  // Helper methods

  /**
   * Check if a string needs quoting in TOON format.
   *
   * Strings need quotes if they contain:
   *   - Leading/trailing whitespace
   *   - Internal whitespace (spaces between words)
   *   - Special characters (quotes, colons, newlines, etc.)
   *   - Are reserved keywords (null, true, false)
   *
   * @param s
   *   The string to check
   * @return
   *   true if quoting is required
   */
  private def needsQuoting(s: String): Boolean = {
    s.isEmpty ||
    s != s.trim ||
    s.contains(' ') ||
    s.exists(c => c == '"' || c == ':' || c == '\n' || c == '\r' || c == '\t') ||
    s == "null" || s == "true" || s == "false"
  }

  /**
   * Check if a value is a simple primitive (can be used in inline array).
   *
   * Simple primitives are single-line values without nested structures.
   *
   * @param value
   *   The string representation of a value
   * @return
   *   true if the value is a simple primitive
   */
  private def isSimplePrimitive(value: String): Boolean = {
    !value.contains('\n') && !value.startsWith("arr[") && !value.contains(": ")
  }

}

/**
 * ObjectVisitor implementation for stringifying objects.
 *
 * Accumulates key-value pairs as strings and produces the final object representation.
 *
 * @param indent
 *   Number of spaces for indentation
 */
final class StringifyObjectVisitor(indent: Int) extends ObjectVisitor[String] {

  private val buffer = new StringBuilder()

  private var lastKey: String = ""

  private var first = true

  /**
   * Visit a key in the object.
   *
   * Stores the key for use when the value is provided.
   *
   * @param key
   *   The field name
   */
  override def visitKey(key: String): Unit = {
    lastKey = key
  }

  /**
   * Get a visitor for processing the next value.
   *
   * Returns the parent StringifyVisitor for recursive traversal.
   *
   * @return
   *   A StringifyVisitor for the value
   */
  override def visitValue(): Visitor[String] = new StringifyVisitor(indent)

  /**
   * Provide the processed value result.
   *
   * Appends the key-value pair to the buffer.
   *
   * @param value
   *   The processed value string
   */
  override def visitValue(value: String): Unit = {
    if (!first) buffer.append("\n")
    first = false

    // Format as "key: value" or multiline if value contains newlines
    if (value.contains('\n')) {
      buffer.append(s"$lastKey:\n")
      val indented = value.linesIterator.map(line => " " * indent + line).mkString("\n")
      buffer.append(indented)
    } else {
      buffer.append(s"$lastKey: $value")
    }
  }

  /**
   * Finalize the object and produce the result.
   *
   * @return
   *   The complete object as a string
   */
  override def done(): String = buffer.toString

}
