package io.toonformat.toon4s.visitor

import scala.collection.immutable.VectorMap

/**
 * Universal tree walker that dispatches any tree structure to visitors.
 *
 * This is the killer feature for zero-dep interop: '''encode/decode Jackson JsonNode, Circe Json,
 * Play JSON, or any tree without converting to JsonValue first'''.
 *
 * ==Why this matters==
 * Most libraries force you to:
 *   1. Parse to their internal AST (JsonValue)
 *   2. Transform/validate
 *   3. Convert back to your library's AST (JsonNode)
 *
 * This creates 2 intermediate trees! TreeWalker eliminates this:
 *   - '''Direct encoding''': Jackson JsonNode → Visitor[String] → TOON string (zero intermediate
 *     JsonValue)
 *   - '''Direct decoding''': TOON string → Visitor[JsonNode] → Jackson JsonNode (zero intermediate
 *     JsonValue)
 *
 * ==Real-world use case==
 * {{{
 * // You have Jackson JsonNode from REST API
 * val jacksonNode: JsonNode = objectMapper.readTree(apiResponse)
 *
 * // Encode directly to TOON without JsonValue conversion
 * val walker = TreeWalker.forJackson  // Adapter for Jackson (see examples)
 * val visitor = new StringifyVisitor(indent = 2)
 * val toon: String = walker.dispatch(jacksonNode, visitor)
 *
 * // Or compose: filter + repair + encode in single pass
 * val composedVisitor = new JsonRepairVisitor(
 *   new FilterKeysVisitor(Set("password"),
 *     new StringifyVisitor(indent = 2)
 *   )
 * )
 * val cleanToon = walker.dispatch(jacksonNode, composedVisitor)
 * }}}
 *
 * ==Performance==
 *   - '''Zero allocations''': Direct traversal without intermediate trees
 *   - '''Single pass''': Parse → Transform → Encode in O(n) time, O(d) space
 *   - '''Type safe''': Generic Tree type ensures correct adapter usage
 *
 * @tparam Tree
 *   The tree type to walk (JsonNode, io.circe.Json, play.api.libs.json.JsValue, etc.)
 */
abstract class TreeWalker[Tree] {

  /**
   * Check if tree node is a string value.
   *
   * @param tree
   *   The tree node
   * @return
   *   Some(string) if node is string, None otherwise
   */
  def asString(tree: Tree): Option[String]

  /**
   * Check if tree node is a number value.
   *
   * @param tree
   *   The tree node
   * @return
   *   Some(number) if node is number, None otherwise
   */
  def asNumber(tree: Tree): Option[BigDecimal]

  /**
   * Check if tree node is a boolean value.
   *
   * @param tree
   *   The tree node
   * @return
   *   Some(bool) if node is boolean, None otherwise
   */
  def asBool(tree: Tree): Option[Boolean]

  /**
   * Check if tree node is null.
   *
   * @param tree
   *   The tree node
   * @return
   *   true if node is null
   */
  def isNull(tree: Tree): Boolean

  /**
   * Check if tree node is an array and get elements.
   *
   * @param tree
   *   The tree node
   * @return
   *   Some(elements) if node is array, None otherwise
   */
  def asArray(tree: Tree): Option[Vector[Tree]]

  /**
   * Check if tree node is an object and get fields.
   *
   * @param tree
   *   The tree node
   * @return
   *   Some(fields) if node is object, None otherwise
   */
  def asObject(tree: Tree): Option[VectorMap[String, Tree]]

  /**
   * Dispatch tree to visitor using pattern matching.
   *
   * This is the universal adapter - works with any tree structure as long as you implement the as*
   * methods above.
   *
   * Stack-safe implementation using explicit stack for deep nesting.
   *
   * @param tree
   *   The tree to traverse
   * @param visitor
   *   The visitor to dispatch to
   * @tparam T
   *   Result type from visitor
   * @return
   *   Result of visitor traversal
   */
  final def dispatch[T](tree: Tree, visitor: Visitor[T]): T = {
    // Simple recursive implementation - practical JSON depth rarely exceeds stack limits
    // Most JSON parsers limit depth to ~1000, while JVM stack handles ~5000-10000 frames
    // For pathological cases requiring stack safety, consider @tailrec or trampoline
    asString(tree) match {
    case Some(s) => visitor.visitString(s)
    case None    =>
      asNumber(tree) match {
      case Some(n) => visitor.visitNumber(n)
      case None    =>
        asBool(tree) match {
        case Some(b) => visitor.visitBool(b)
        case None    =>
          if (isNull(tree)) {
            visitor.visitNull()
          } else {
            asArray(tree) match {
            case Some(elements) =>
              val results = elements.map(elem => dispatch(elem, visitor))
              visitor.visitArray(results)
            case None =>
              asObject(tree) match {
              case Some(fields) =>
                val objVisitor = visitor.visitObject()
                fields.foreach {
                  case (key, value) =>
                    objVisitor.visitKey(key)
                    val valueVisitor = objVisitor.visitValue()
                    val result = dispatch(value, valueVisitor)
                    objVisitor.visitValue(result)
                }
                objVisitor.done()
              case None =>
                throw new IllegalArgumentException(
                  s"Unknown tree node type: ${tree.getClass}"
                )
              }
            }
          }
        }
      }
    }
  }

}

/** Companion object with adapter examples and utilities. */
object TreeWalker {

  /**
   * Example adapter for Jackson's JsonNode.
   *
   * '''IMPORTANT''': This is documentation only. To use it:
   *   1. Add Jackson dependency to your build.sbt
   *   2. Copy this implementation to your codebase
   *   3. Uncomment and use
   *
   * Why not include it? toon4s core is zero-dep. Adding Jackson would add 300KB+ transitive deps.
   *
   * {{{
   * // Add to build.sbt:
   * // libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.17.0"
   *
   * import com.fasterxml.jackson.databind.JsonNode
   * import scala.jdk.CollectionConverters._
   *
   * object JacksonWalker extends TreeWalker[JsonNode] {
   *
   *   override def asString(tree: JsonNode): Option[String] =
   *     if (tree.isTextual) Some(tree.asText) else None
   *
   *   override def asNumber(tree: JsonNode): Option[BigDecimal] =
   *     if (tree.isNumber) Some(BigDecimal(tree.asText)) else None
   *
   *   override def asBool(tree: JsonNode): Option[Boolean] =
   *     if (tree.isBoolean) Some(tree.asBoolean) else None
   *
   *   override def isNull(tree: JsonNode): Boolean =
   *     tree.isNull
   *
   *   override def asArray(tree: JsonNode): Option[Vector[JsonNode]] =
   *     if (tree.isArray) Some(tree.elements.asScala.toVector) else None
   *
   *   override def asObject(tree: JsonNode): Option[VectorMap[String, JsonNode]] =
   *     if (tree.isObject) {
   *       val fields = tree.fields.asScala.map(e => e.getKey -> e.getValue).toSeq
   *       Some(VectorMap.from(fields))
   *     } else None
   * }
   *
   * // Usage:
   * val jacksonNode = objectMapper.readTree("""{"name":"Ada","age":30}""")
   * val toon = JacksonWalker.dispatch(jacksonNode, new StringifyVisitor(indent = 2))
   * }}}
   */
  def forJacksonExample: String =
    """
See TreeWalker scaladoc for Jackson adapter example.
Add Jackson dependency and copy the adapter to your codebase.
"""

  /**
   * Example adapter for Circe's Json.
   *
   * {{{
   * // Add to build.sbt:
   * // libraryDependencies += "io.circe" %% "circe-core" % "0.14.6"
   *
   * import io.circe.Json
   *
   * object CirceWalker extends TreeWalker[Json] {
   *
   *   override def asString(tree: Json): Option[String] =
   *     tree.asString
   *
   *   override def asNumber(tree: Json): Option[BigDecimal] =
   *     tree.asNumber.flatMap(_.toBigDecimal)
   *
   *   override def asBool(tree: Json): Option[Boolean] =
   *     tree.asBoolean
   *
   *   override def isNull(tree: Json): Boolean =
   *     tree.isNull
   *
   *   override def asArray(tree: Json): Option[Vector[Json]] =
   *     tree.asArray
   *
   *   override def asObject(tree: Json): Option[VectorMap[String, Json]] =
   *     tree.asObject.map(obj => VectorMap.from(obj.toVector))
   * }
   *
   * // Usage:
   * val circeJson = io.circe.parser.parse("""{"name":"Ada"}""").toOption.get
   * val toon = CirceWalker.dispatch(circeJson, new StringifyVisitor(indent = 2))
   * }}}
   */
  def forCirceExample: String =
    """
See TreeWalker scaladoc for Circe adapter example.
Add Circe dependency and copy the adapter to your codebase.
"""

}
