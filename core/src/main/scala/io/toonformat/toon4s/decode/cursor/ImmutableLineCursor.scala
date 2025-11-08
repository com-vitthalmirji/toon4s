package io.toonformat.toon4s
package decode
package cursor

/**
 * Immutable line cursor for functional parsing.
 *
 * ==Design: Immutable state + Pure functions==
 *
 * This cursor provides a purely functional interface for sequential line parsing. Unlike the
 * mutable [[decode.LineCursor]], all operations return new cursor instances, enabling:
 *   - Referential transparency (Point #10)
 *   - Easier reasoning about code
 *   - Safe concurrent access
 *   - Backtracking capabilities
 *
 * ==State Monad==
 * This implements a simplified State monad pattern using case classes and method chaining.
 *
 * @param lines
 *   Vector of parsed lines
 * @param idx
 *   Current position in the vector
 * @param blanks
 *   Vector of blank line positions
 *
 * @example
 *   {{{
 * val cursor = ImmutableLineCursor(parsedLines, 0, blankLines)
 *
 * // Functional style - returns new cursor
 * val (line, newCursor) = cursor.next
 *
 * // Chaining operations
 * val cursor2 = cursor.advance.advance.advance
 *
 * // Original cursor unchanged
 * assert(cursor.position == 0)
 * assert(cursor2.position == 3)
 *   }}}
 */
final case class ImmutableLineCursor private (
    lines: Vector[ParsedLine],
    idx: Int,
    blanks: Vector[BlankLine],
) {

  /**
   * Current position in the line vector.
   *
   * ==Pure Function==
   * Returns current index without side effects.
   */
  def position: Int = idx

  /**
   * Peek at the current line without advancing.
   *
   * ==Pure function==
   * Does not modify cursor state.
   *
   * @return
   *   Option of current line, None if at end
   *
   * @example
   *   {{{
   * cursor.peek match {
   *   case Some(line) => println(s"Current line: \${line.content}")
   *   case None => println("End of input")
   * }
   *   }}}
   */
  def peek: Option[ParsedLine] = lines.lift(idx)

  /**
   * Get current line and advance to next position.
   *
   * ==Pure function==
   * Returns tuple of (current line, new cursor).
   *
   * @return
   *   Tuple of (Option[ParsedLine], new cursor)
   *
   * @example
   *   {{{
   * val (lineOpt, nextCursor) = cursor.next
   * lineOpt match {
   *   case Some(line) => processLine(line, nextCursor)
   *   case None => Done
   * }
   *   }}}
   */
  def next: (Option[ParsedLine], ImmutableLineCursor) = {
    val cur = peek
    (cur, copy(idx = idx + 1))
  }

  /**
   * Get the previously consumed line.
   *
   * ==Pure function==
   * Returns the line that was current before the last advance.
   *
   * @return
   *   Option of previous line, None if at beginning
   *
   * @example
   *   {{{
   * val cursor2 = cursor.advance
   * cursor2.current  // Returns the line that cursor was pointing to
   *   }}}
   */
  def current: Option[ParsedLine] =
    if (idx > 0) lines.lift(idx - 1) else None

  /**
   * Check if cursor is at the end of input.
   *
   * ==Pure predicate function==
   *
   * @return
   *   true if no more lines to consume
   *
   * @example
   *   {{{
   * if (cursor.atEnd) {
   *   println("No more lines")
   * }
   *   }}}
   */
  def atEnd: Boolean = idx >= lines.length

  /**
   * Total number of lines.
   *
   * ==Pure function==
   *
   * @return
   *   Total line count
   */
  def length: Int = lines.length

  /**
   * Peek at current line if it has the target depth.
   *
   * ==Pure function with guard==
   * Combines peek with depth filtering.
   *
   * @param target
   *   The depth level to match
   * @return
   *   Option of line if depth matches, None otherwise
   *
   * @example
   *   {{{
   * cursor.peekAtDepth(2) match {
   *   case Some(line) => println(s"Found line at depth 2")
   *   case None => println("Current line is not at depth 2")
   * }
   *   }}}
   */
  def peekAtDepth(target: Int): Option[ParsedLine] =
    peek.filter(_.depth == target)

  /**
   * Check if there are more lines at the target depth.
   *
   * ==Pure predicate function==
   *
   * @param target
   *   The depth level to check
   * @return
   *   true if current line exists and has target depth
   *
   * @example
   *   {{{
   * if (cursor.hasMoreAtDepth(1)) {
   *   // Process lines at depth 1
   * }
   *   }}}
   */
  def hasMoreAtDepth(target: Int): Boolean =
    peekAtDepth(target).isDefined

  /**
   * Get all blank line information.
   *
   * ==Pure function==
   * Returns immutable vector, no side effects.
   *
   * @return
   *   Vector of blank line positions
   */
  def getBlankLines: Vector[BlankLine] = blanks

  /**
   * Advance cursor to next position.
   *
   * ==Pure function==
   * Returns new cursor with incremented index.
   *
   * @return
   *   New cursor at next position
   *
   * @example
   *   {{{
   * val next = cursor.advance
   * val next2 = next.advance
   * // cursor is unchanged
   *   }}}
   */
  def advance: ImmutableLineCursor = copy(idx = idx + 1)

  /**
   * Advance cursor by N positions.
   *
   * ==Pure Function==
   * Returns new cursor advanced by N steps.
   *
   * @param n
   *   Number of positions to advance
   * @return
   *   New cursor at idx + n
   *
   * @example
   *   {{{
   * val cursor3 = cursor.advanceBy(3)
   * assert(cursor3.position == cursor.position + 3)
   *   }}}
   */
  def advanceBy(n: Int): ImmutableLineCursor = copy(idx = idx + n)

  /**
   * Reset cursor to beginning.
   *
   * ==Pure function==
   * Returns new cursor at position 0.
   *
   * @return
   *   New cursor at start
   *
   * @example
   *   {{{
   * val start = cursor.reset
   * assert(start.position == 0)
   *   }}}
   */
  def reset: ImmutableLineCursor = copy(idx = 0)

  /**
   * Jump cursor to specific position.
   *
   * ==Pure function==
   * Returns new cursor at the specified index.
   *
   * @param position
   *   Target index
   * @return
   *   New cursor at position
   *
   * @example
   *   {{{
   * val cursor5 = cursor.jumpTo(5)
   * assert(cursor5.position == 5)
   *   }}}
   */
  def jumpTo(position: Int): ImmutableLineCursor = copy(idx = position)

  /**
   * Map over remaining lines.
   *
   * ==Higher-order function==
   * Applies function to all remaining lines.
   *
   * @param f
   *   Function to apply to each line
   * @return
   *   Vector of results
   *
   * @example
   *   {{{
   * val contents = cursor.map(_.content)
   * val depths = cursor.map(_.depth)
   *   }}}
   */
  def map[A](f: ParsedLine => A): Vector[A] =
    lines.drop(idx).map(f)

  /**
   * Filter remaining lines.
   *
   * ==Higher-order function==
   * Selects lines matching predicate.
   *
   * @param p
   *   Predicate to test each line
   * @return
   *   Vector of matching lines
   *
   * @example
   *   {{{
   * val depth2Lines = cursor.filter(_.depth == 2)
   * val nonEmpty = cursor.filter(_.content.nonEmpty)
   *   }}}
   */
  def filter(p: ParsedLine => Boolean): Vector[ParsedLine] =
    lines.drop(idx).filter(p)

  /**
   * Fold over remaining lines with initial value.
   *
   * ==Higher-order function==
   * Reduces lines to a single value.
   *
   * @param z
   *   Initial accumulator value
   * @param f
   *   Folding function
   * @return
   *   Final accumulated value
   *
   * @example
   *   {{{
   * val totalDepth = cursor.foldLeft(0)((acc, line) => acc + line.depth)
   * val allContent = cursor.foldLeft("")((acc, line) => acc + line.content)
   *   }}}
   */
  def foldLeft[A](z: A)(f: (A, ParsedLine) => A): A =
    lines.drop(idx).foldLeft(z)(f)

}

object ImmutableLineCursor {

  /**
   * Create a new cursor from scan result.
   *
   * ==Factory method==
   *
   * @param result
   *   Scan result containing lines and blanks
   * @return
   *   New cursor at position 0
   *
   * @example
   *   {{{
   * val scanResult = Scanner.toParsedLines(input, 2, true)
   * val cursor = ImmutableLineCursor.fromScanResult(scanResult)
   *   }}}
   */
  def fromScanResult(result: ScanResult): ImmutableLineCursor =
    ImmutableLineCursor(result.lines, 0, result.blanks)

  /**
   * Create a new cursor from lines and blanks.
   *
   * ==Factory method==
   *
   * @param lines
   *   Vector of parsed lines
   * @param blanks
   *   Vector of blank lines
   * @return
   *   New cursor at position 0
   *
   * @example
   *   {{{
   * val cursor = ImmutableLineCursor.apply(parsedLines, blankLines)
   *   }}}
   */
  def apply(lines: Vector[ParsedLine], blanks: Vector[BlankLine]): ImmutableLineCursor =
    ImmutableLineCursor(lines, 0, blanks)

}
