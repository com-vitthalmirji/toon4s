package io.toonformat.toon4s.types

/**
 * Domain types using Scala 3 opaque types for zero-cost type safety.
 *
 * Opaque types provide compile-time guarantees without runtime overhead. Inside this object, the
 * opaque type is equivalent to its underlying type. Outside, they are distinct types that cannot be
 * mixed.
 *
 * ==Design Pattern: Newtype Pattern with Opaque Types==
 *
 * Benefits:
 *   - Prevents mixing up Int values (indent vs depth vs line number)
 *   - Zero runtime cost (no wrapper objects)
 *   - Compiler-enforced domain boundaries
 *   - Self-documenting code
 *
 * @example
 *   {{{
 * import io.toonformat.toon4s.types.DomainTypes.*
 *
 * val indent: Indent = Indent(2)
 * val depth: Depth = Depth(3)
 *
 * // This won't compile - type safety!
 * // val x: Indent = depth
 *   }}}
 */
object DomainTypes {

  /**
   * Number of spaces per indentation level.
   *
   * Valid values: positive integers (typically 2 or 4)
   */
  opaque type Indent = Int

  object Indent {

    def apply(value: Int): Indent = {
      require(value > 0, s"Indent must be positive, got: $value")
      value
    }

    def unsafe(value: Int): Indent = value

    extension (indent: Indent) {

      def value: Int = indent

      def *(depth: Depth): Int = indent * depth.value

    }

  }

  /**
   * Nesting depth level (0-based).
   *
   * Represents how many levels deep a structure is nested. Root level = 0, first nesting = 1, etc.
   */
  opaque type Depth = Int

  object Depth {

    def apply(value: Int): Depth = {
      require(value >= 0, s"Depth cannot be negative, got: $value")
      value
    }

    def unsafe(value: Int): Depth = value

    val Zero: Depth = 0

    extension (depth: Depth) {

      def value: Int = depth

      def +(n: Int): Depth = depth + n

      def -(n: Int): Depth = depth - n

      def <(other: Depth): Boolean = depth < other

      def <=(other: Depth): Boolean = depth <= other

      def >(other: Depth): Boolean = depth > other

      def >=(other: Depth): Boolean = depth >= other

      def ==(other: Depth): Boolean = depth == other

      def indent: Int = depth

    }

  }

  /**
   * Line number in source text (1-based).
   *
   * Line numbers start at 1 for the first line.
   */
  opaque type LineNumber = Int

  object LineNumber {

    def apply(value: Int): LineNumber = {
      require(value > 0, s"Line number must be positive, got: $value")
      value
    }

    def unsafe(value: Int): LineNumber = value

    extension (line: LineNumber) {

      def value: Int = line

      def +(n: Int): LineNumber = line + n

    }

  }

  /**
   * Column number in source text (1-based).
   *
   * Column numbers start at 1 for the first character.
   */
  opaque type ColumnNumber = Int

  object ColumnNumber {

    def apply(value: Int): ColumnNumber = {
      require(value > 0, s"Column number must be positive, got: $value")
      value
    }

    def unsafe(value: Int): ColumnNumber = value

    extension (col: ColumnNumber) {

      def value: Int = col

      def +(n: Int): ColumnNumber = col + n

    }

  }

  /**
   * Array length (number of elements).
   *
   * Represents declared array length in TOON headers like `arr[5]`.
   */
  opaque type ArrayLength = Int

  object ArrayLength {

    def apply(value: Int): ArrayLength = {
      require(value >= 0, s"Array length cannot be negative, got: $value")
      value
    }

    def unsafe(value: Int): ArrayLength = value

    val Zero: ArrayLength = 0

    extension (length: ArrayLength) {

      def value: Int = length

      def +(n: Int): ArrayLength = length + n

      def <(n: Int): Boolean = length < n

      def <=(n: Int): Boolean = length <= n

      def >(n: Int): Boolean = length > n

      def >=(n: Int): Boolean = length >= n

      def ==(n: Int): Boolean = length == n

    }

  }

  /**
   * String length in characters.
   *
   * Used for validation limits to prevent memory exhaustion.
   */
  opaque type StringLength = Int

  object StringLength {

    def apply(value: Int): StringLength = {
      require(value >= 0, s"String length cannot be negative, got: $value")
      value
    }

    def unsafe(value: Int): StringLength = value

    extension (length: StringLength) {

      def value: Int = length

      def >(limit: Int): Boolean = length > limit

    }

  }

}
