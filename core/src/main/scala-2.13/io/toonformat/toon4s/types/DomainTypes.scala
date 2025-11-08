package io.toonformat.toon4s.types

/**
 * Domain types using AnyVal wrappers for Scala 2.13 type safety.
 *
 * AnyVal wrappers provide compile-time type safety with minimal runtime overhead. The Scala
 * compiler can often eliminate the wrapper allocation through escape analysis.
 *
 * ==Design Pattern: Newtype Pattern with Value Classes==
 *
 * Benefits:
 *   - Prevents mixing up Int values (indent vs depth vs line number)
 *   - Minimal runtime cost (stack-allocated, often unboxed)
 *   - Compiler-enforced domain boundaries
 *   - Self-documenting code
 *
 * @example
 *   {{{
 * import io.toonformat.toon4s.types.DomainTypes._
 *
 * val indent = Indent(2)
 * val depth = Depth(3)
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
  final case class Indent(value: Int) extends AnyVal {

    def *(depth: Depth): Int = value * depth.value

  }

  object Indent {

    def apply(value: Int): Indent = {
      require(value > 0, s"Indent must be positive, got: $value")
      new Indent(value)
    }

    def unsafe(value: Int): Indent = new Indent(value)

  }

  /**
   * Nesting depth level (0-based).
   *
   * Represents how many levels deep a structure is nested. Root level = 0, first nesting = 1, etc.
   */
  final case class Depth(value: Int) extends AnyVal {

    def +(n: Int): Depth = Depth.unsafe(value + n)

    def -(n: Int): Depth = Depth.unsafe(value - n)

    def <(other: Depth): Boolean = value < other.value

    def <=(other: Depth): Boolean = value <= other.value

    def >(other: Depth): Boolean = value > other.value

    def >=(other: Depth): Boolean = value >= other.value

    def indent: Int = value

  }

  object Depth {

    def apply(value: Int): Depth = {
      require(value >= 0, s"Depth cannot be negative, got: $value")
      new Depth(value)
    }

    def unsafe(value: Int): Depth = new Depth(value)

    val Zero: Depth = new Depth(0)

  }

  /**
   * Line number in source text (1-based).
   *
   * Line numbers start at 1 for the first line.
   */
  final case class LineNumber(value: Int) extends AnyVal {

    def +(n: Int): LineNumber = LineNumber.unsafe(value + n)

  }

  object LineNumber {

    def apply(value: Int): LineNumber = {
      require(value > 0, s"Line number must be positive, got: $value")
      new LineNumber(value)
    }

    def unsafe(value: Int): LineNumber = new LineNumber(value)

  }

  /**
   * Column number in source text (1-based).
   *
   * Column numbers start at 1 for the first character.
   */
  final case class ColumnNumber(value: Int) extends AnyVal {

    def +(n: Int): ColumnNumber = ColumnNumber.unsafe(value + n)

  }

  object ColumnNumber {

    def apply(value: Int): ColumnNumber = {
      require(value > 0, s"Column number must be positive, got: $value")
      new ColumnNumber(value)
    }

    def unsafe(value: Int): ColumnNumber = new ColumnNumber(value)

  }

  /**
   * Array length (number of elements).
   *
   * Represents declared array length in TOON headers like `arr[5]`.
   */
  final case class ArrayLength(value: Int) extends AnyVal {

    def +(n: Int): ArrayLength = ArrayLength.unsafe(value + n)

    def <(n: Int): Boolean = value < n

    def <=(n: Int): Boolean = value <= n

    def >(n: Int): Boolean = value > n

    def >=(n: Int): Boolean = value >= n

  }

  object ArrayLength {

    def apply(value: Int): ArrayLength = {
      require(value >= 0, s"Array length cannot be negative, got: $value")
      new ArrayLength(value)
    }

    def unsafe(value: Int): ArrayLength = new ArrayLength(value)

    val Zero: ArrayLength = new ArrayLength(0)

  }

  /**
   * String length in characters.
   *
   * Used for validation limits to prevent memory exhaustion.
   */
  final case class StringLength(value: Int) extends AnyVal {

    def >(limit: Int): Boolean = value > limit

  }

  object StringLength {

    def apply(value: Int): StringLength = {
      require(value >= 0, s"String length cannot be negative, got: $value")
      new StringLength(value)
    }

    def unsafe(value: Int): StringLength = new StringLength(value)

  }

}
