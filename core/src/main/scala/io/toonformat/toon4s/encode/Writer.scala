package io.toonformat.toon4s
package encode

import java.io.Writer

/**
 * Base trait for TOON line writing operations.
 *
 * ==Design: Interface segregation principle (ISP)==
 *
 * This trait hierarchy separates concerns:
 *   - [[EncodeLineWriter]] - Basic line writing operations
 *   - [[StreamingEncodeLineWriter]] - Additional streaming optimizations
 *
 * Clients depend only on methods they need, following SOLID principle I.
 *
 * ==Strategy design==
 * Implementations provide different output strategies:
 *   - [[LineWriter]] - In-memory StringBuilder accumulation
 *   - [[StreamLineWriter]] - Direct streaming to Writer
 */
sealed trait EncodeLineWriter {

  /**
   * Push a line at the specified depth.
   *
   * @param depth
   *   Nesting depth for indentation
   * @param line
   *   Line content to write
   */
  def push(depth: Int, line: String): Unit

  /**
   * Push a list item line (with hyphen prefix).
   *
   * @param depth
   *   Nesting depth for indentation
   * @param line
   *   Line content after the hyphen
   */
  def pushListItem(depth: Int, line: String): Unit

}

/**
 * Extended interface for streaming-specific optimizations.
 *
 * ==Design: Interface segregation principle==
 *
 * This trait adds streaming-specific methods that:
 *   - Avoid building large intermediate strings
 *   - Write primitives directly to output
 *   - Optimize for memory efficiency
 *
 * Only [[StreamLineWriter]] implements this interface. [[LineWriter]] does not need these methods
 * as it accumulates in memory anyway.
 */
sealed trait StreamingEncodeLineWriter extends EncodeLineWriter {

  /**
   * Push delimited primitives in a single operation.
   *
   * Avoids building intermediate string for inline arrays.
   *
   * @param depth
   *   Nesting depth
   * @param header
   *   Array header string
   * @param values
   *   Primitive values to write
   * @param delim
   *   Delimiter between values
   */
  def pushDelimitedPrimitives(
      depth: Int,
      header: String,
      values: Vector[JsonValue],
      delim: Delimiter,
  ): Unit

  /**
   * Push tabular row primitives.
   *
   * Writes delimited values without header.
   *
   * @param depth
   *   Nesting depth
   * @param values
   *   Primitive values for row
   * @param delim
   *   Delimiter between values
   */
  def pushRowPrimitives(
      depth: Int,
      values: Vector[JsonValue],
      delim: Delimiter,
  ): Unit

  /**
   * Push list item with inline delimited primitives.
   *
   * Combines list item marker with inline array values.
   *
   * @param depth
   *   Nesting depth
   * @param header
   *   Array header string
   * @param values
   *   Primitive values
   * @param delim
   *   Delimiter between values
   */
  def pushListItemDelimitedPrimitives(
      depth: Int,
      header: String,
      values: Vector[JsonValue],
      delim: Delimiter,
  ): Unit

  /**
   * Push key without value (for nested objects).
   *
   * @param depth
   *   Nesting depth
   * @param key
   *   Key name
   */
  def pushKeyOnly(depth: Int, key: String): Unit

  /**
   * Push key-value pair where value is a primitive.
   *
   * @param depth
   *   Nesting depth
   * @param key
   *   Key name
   * @param value
   *   Primitive value
   * @param delim
   *   Delimiter (for string quoting decision)
   */
  def pushKeyValuePrimitive(
      depth: Int,
      key: String,
      value: JsonValue,
      delim: Delimiter,
  ): Unit

}

/**
 * In-memory line writer using StringBuilder.
 *
 * ==Design: Builder + Strategy pattern==
 *
 * Accumulates output in memory, suitable for:
 *   - Small to medium outputs
 *   - When full string is needed immediately
 *   - Testing and debugging
 *
 * @param indentSize
 *   Number of spaces per indentation level
 *
 * @example
 *   {{{
 * val writer = new LineWriter(2)
 * writer.push(0, "root: value")
 * writer.push(1, "nested: value")
 * val output = writer.toString
 *   }}}
 */
final class LineWriter(indentSize: Int) extends EncodeLineWriter {

  private val builder = new StringBuilder

  private var first = true

  /**
   * Add indentation padding.
   *
   * ==Pure side effect==
   * Mutates builder but encapsulated within class.
   */
  private def pad(depth: Int): Unit = {
    var i = 0
    while (i < depth * indentSize) {
      builder.append(' ')
      i += 1
    }
  }

  def push(depth: Int, line: String): Unit = {
    if (!first) builder.append('\n') else first = false
    pad(depth)
    builder.append(line)
  }

  def pushListItem(depth: Int, line: String): Unit = {
    if (!first) builder.append('\n') else first = false
    pad(depth)
    if (line.isEmpty) builder.append('-')
    else {
      builder.append("- ")
      builder.append(line)
    }
  }

  /**
   * Get accumulated output.
   *
   * @return
   *   Complete TOON string
   */
  override def toString: String = builder.result()

}

/**
 * Streaming line writer for direct output to Writer.
 *
 * ==Design: Strategy + Decorator pattern==
 *
 * Writes directly to output stream/file, suitable for:
 *   - Large outputs
 *   - Streaming to network/file
 *   - Memory-constrained environments
 *
 * Implements both [[EncodeLineWriter]] and [[StreamingEncodeLineWriter]] for maximum optimization
 * capabilities.
 *
 * @param indentSize
 *   Number of spaces per indentation level
 * @param out
 *   Output writer to stream to
 *
 * @example
 *   {{{
 * val writer = new java.io.FileWriter("output.toon")
 * val streamWriter = new StreamLineWriter(2, writer)
 * streamWriter.push(0, "root: value")
 * writer.close()
 *   }}}
 */
final class StreamLineWriter(indentSize: Int, out: Writer) extends StreamingEncodeLineWriter {

  private var first = true

  /**
   * Add indentation padding directly to output.
   *
   * ==Pure side effect==
   * Writes to external Writer.
   */
  private def pad(depth: Int): Unit = {
    var i = 0
    val spaces = depth * indentSize
    while (i < spaces) {
      out.write(' ')
      i += 1
    }
  }

  def push(depth: Int, line: String): Unit = {
    if (!first) out.write('\n') else first = false
    pad(depth)
    out.write(line)
  }

  def pushListItem(depth: Int, line: String): Unit = {
    if (!first) out.write('\n') else first = false
    pad(depth)
    if (line.isEmpty) out.write('-')
    else {
      out.write("- ")
      out.write(line)
    }
  }

  // ========================================================================
  // Streaming Optimizations (StreamingEncodeLineWriter interface)
  // ========================================================================

  def pushDelimitedPrimitives(
      depth: Int,
      header: String,
      values: Vector[JsonValue],
      delim: Delimiter,
  ): Unit = {
    if (!first) out.write('\n') else first = false
    pad(depth)
    out.write(header)
    out.write(' ')
    var i = 0
    while (i < values.length) {
      if (i > 0) out.write(delim.char)
      Primitives.writePrimitive(values(i), delim, out)
      i += 1
    }
  }

  def pushRowPrimitives(
      depth: Int,
      values: Vector[JsonValue],
      delim: Delimiter,
  ): Unit = {
    if (!first) out.write('\n') else first = false
    pad(depth)
    var i = 0
    while (i < values.length) {
      if (i > 0) out.write(delim.char)
      Primitives.writePrimitive(values(i), delim, out)
      i += 1
    }
  }

  def pushListItemDelimitedPrimitives(
      depth: Int,
      header: String,
      values: Vector[JsonValue],
      delim: Delimiter,
  ): Unit = {
    if (!first) out.write('\n') else first = false
    pad(depth)
    out.write("- ")
    out.write(header)
    if (values.nonEmpty) out.write(' ')
    var i = 0
    while (i < values.length) {
      if (i > 0) out.write(delim.char)
      Primitives.writePrimitive(values(i), delim, out)
      i += 1
    }
  }

  def pushKeyOnly(depth: Int, key: String): Unit = {
    if (!first) out.write('\n') else first = false
    pad(depth)
    out.write(Primitives.encodeKey(key))
    out.write(':')
  }

  def pushKeyValuePrimitive(
      depth: Int,
      key: String,
      value: JsonValue,
      delim: Delimiter,
  ): Unit = {
    if (!first) out.write('\n') else first = false
    pad(depth)
    out.write(Primitives.encodeKey(key))
    out.write(':')
    out.write(' ')
    Primitives.writePrimitive(value, delim, out)
  }

}
