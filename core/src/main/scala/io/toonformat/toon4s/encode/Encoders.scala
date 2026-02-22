package io.toonformat.toon4s
package encode

import scala.collection.immutable.VectorMap

import Primitives._
import io.toonformat.toon4s.{Delimiter, EncodeOptions, KeyFolding}
import io.toonformat.toon4s.JsonValue._

object Encoders {

  // Cache for common empty array headers (length 0-10)
  private val emptyHeadersComma = (0 to 10).map(n => s"[$n]:").toArray

  private val emptyHeadersTab = (0 to 10).map(n => s"[$n\t]:").toArray

  private val emptyHeadersPipe = (0 to 10).map(n => s"[$n|]:").toArray

  final private case class FieldEntry(
      key: String,
      value: JsonValue,
      disableChildFolding: Boolean,
  )

  final private case class FoldResult(
      key: String,
      value: JsonValue,
      disableChildFolding: Boolean,
  )

  def encodeTo(value: JsonValue, out: java.io.Writer, options: EncodeOptions): Unit = {
    val folded = applyKeyFolding(value, options)
    folded match {
    case JNull | JBool(_) | JNumber(_) | JString(_) =>
      out.write(encodePrimitive(folded, options.delimiter))
    case JArray(values) =>
      val writer = new StreamLineWriter(options.indent, out)
      encodeArray(None, values, writer, 0, options)
    case JObj(fields) =>
      val writer = new StreamLineWriter(options.indent, out)
      encodeObject(fields, writer, 0, options)
    }
  }

  def encode(value: JsonValue, options: EncodeOptions): String = {
    val folded = applyKeyFolding(value, options)
    folded match {
    case JNull | JBool(_) | JNumber(_) | JString(_) =>
      encodePrimitive(folded, options.delimiter)
    case JArray(values) =>
      val writer = new LineWriter(options.indent)
      encodeArray(None, values, writer, 0, options)
      writer.toString
    case JObj(fields) =>
      val writer = new LineWriter(options.indent)
      encodeObject(fields, writer, 0, options)
      writer.toString
    }
  }

  private def formatHeader(
      length: Int,
      key: Option[String],
      fields: List[String],
      delimiter: Delimiter,
  ): String = {
    val useLengthMarker = Delimiter.usesLengthMarker(delimiter)
    val baseDelimiter = Delimiter.base(delimiter)

    // Fast path: empty array with no key
    if (!useLengthMarker && key.isEmpty && fields.isEmpty && length <= 10) {
      return baseDelimiter match {
      case Delimiter.Comma | Delimiter.CommaWithLengthMarker => emptyHeadersComma(length)
      case Delimiter.Tab | Delimiter.TabWithLengthMarker     => emptyHeadersTab(length)
      case Delimiter.Pipe | Delimiter.PipeWithLengthMarker   => emptyHeadersPipe(length)
      }
    }

    // Slow path: use StringBuilder
    val builder = new StringBuilder(64)

    key.foreach(k => builder.append(encodeKey(k)))

    builder.append('[')
    if (useLengthMarker) builder.append('#')
    builder.append(length)
    baseDelimiter match {
    case Delimiter.Tab | Delimiter.TabWithLengthMarker     => builder.append('\t')
    case Delimiter.Pipe | Delimiter.PipeWithLengthMarker   => builder.append('|')
    case Delimiter.Comma | Delimiter.CommaWithLengthMarker => // no suffix
    }
    builder.append(']')

    if (fields.nonEmpty) {
      builder.append('{')
      var i = 0
      while (i < fields.length) {
        if (i > 0) builder.append(delimiter.char)
        builder.append(encodeKey(fields(i)))
        i += 1
      }
      builder.append('}')
    }

    builder.append(':')
    builder.result()
  }

  private def encodeObject(
      fields: scala.collection.immutable.VectorMap[String, JsonValue],
      writer: EncodeLineWriter,
      depth: Int,
      options: EncodeOptions,
      allowFolding: Boolean = true,
  ): Unit = {
    val prepared = prepareObjectFields(fields, options, allowFolding)
    prepared.foreach {
      case FieldEntry(k, v, disableChildFolding) =>
        encodeKeyValue(k, v, writer, depth, options, allowFolding && !disableChildFolding)
    }
  }

  private val IdentifierSegmentPattern = "^[A-Za-z_][A-Za-z0-9_]*$".r

  private def isIdentifierSegment(segment: String): Boolean =
    IdentifierSegmentPattern.matches(segment)

  private def prepareObjectFields(
      fields: VectorMap[String, JsonValue],
      options: EncodeOptions,
      allowFolding: Boolean,
  ): Vector[FieldEntry] = {
    if (!allowFolding || options.keyFolding != KeyFolding.Safe || options.flattenDepth < 2)
      fields.toVector.map { case (k, v) => FieldEntry(k, v, disableChildFolding = false) }
    else {
      val literalKeys = fields.keySet
      var emitted = Set.empty[String]
      val builder = Vector.newBuilder[FieldEntry]

      fields.foreach {
        case (key, value) =>
          val FoldResult(foldedKey, foldedValue, disableChild) =
            foldKeyIfEligible(key, value, literalKeys, emitted, options.flattenDepth)
          emitted += foldedKey
          builder += FieldEntry(foldedKey, foldedValue, disableChild)
      }

      builder.result()
    }
  }

  private def applyKeyFolding(value: JsonValue, options: EncodeOptions): JsonValue = value

  private def foldKeyIfEligible(
      key: String,
      value: JsonValue,
      literalKeys: Set[String],
      emittedKeys: Set[String],
      flattenDepth: Int,
  ): FoldResult = {
    def collectChain(
        currentKey: String,
        currentValue: JsonValue,
        acc: Vector[String],
    ): (Vector[String], JsonValue) =
      currentValue match {
      case JObj(obj) if obj.size == 1 =>
        val (nextKey, nextVal) = obj.head
        collectChain(nextKey, nextVal, acc :+ currentKey)
      case other => (acc :+ currentKey, other)
      }

    val (segments, leaf) = value match {
    case JObj(obj) if obj.nonEmpty && obj.size == 1 =>
      collectChain(key, JObj(obj), Vector.empty)
    case other => (Vector(key), other)
    }

    if (segments.length < 2) FoldResult(key, value, disableChildFolding = false)
    else {
      val depthLimit = math.min(segments.length, flattenDepth)
      val prefix = segments.take(depthLimit)

      val safeSegments =
        prefix.forall(seg => isIdentifierSegment(seg) && isValidUnquotedKey(seg))
      if (!safeSegments) FoldResult(key, value, disableChildFolding = true)
      else {
        val foldedKey = prefix.mkString(".")
        val collidesWithLiteral =
          literalKeys.exists(existing => existing == foldedKey && existing != key)
        val collidesWithEmitted =
          emittedKeys.contains(foldedKey) && foldedKey != key

        if (collidesWithLiteral || collidesWithEmitted)
          FoldResult(key, value, disableChildFolding = true)
        else {
          val remainder = segments.drop(depthLimit)
          val foldedValue =
            if (remainder.isEmpty) leaf
            else remainder.foldRight(leaf: JsonValue) {
              case (segment, acc) => JObj(VectorMap(segment -> acc))
            }
          val disableChildren = remainder.nonEmpty
          FoldResult(foldedKey, foldedValue, disableChildren)
        }
      }
    }
  }

  private def encodeKeyValue(
      key: String,
      value: JsonValue,
      writer: EncodeLineWriter,
      depth: Int,
      options: EncodeOptions,
      allowChildFolding: Boolean,
  ): Unit = value match {
  case JNull | JBool(_) | JNumber(_) | JString(_) =>
    writer match {
    case sw: StreamLineWriter => sw.pushKeyValuePrimitive(depth, key, value, options.delimiter)
    case _                    =>
      writer.push(depth, s"${encodeKey(key)}: ${encodePrimitive(value, options.delimiter)}")
    }
  case JArray(values) =>
    encodeArray(Some(key), values, writer, depth, options, allowChildFolding)
  case JObj(obj) =>
    writer match {
    case sw: StreamLineWriter => sw.pushKeyOnly(depth, key)
    case _                    => writer.push(depth, s"${encodeKey(key)}:")
    }
    if (obj.nonEmpty) {
      encodeObject(obj, writer, depth + 1, options, allowChildFolding)
    }
  }

  private def isArrayOfPrimitives(values: Vector[JsonValue]): Boolean =
    values.forall {
      case JString(_) | JNumber(_) | JBool(_) | JNull => true
      case _                                          => false
    }

  private def isArrayOfObjects(values: Vector[JsonValue]): Boolean =
    values.forall {
      case JObj(_) => true
      case _       => false
    }

  private def extractTabularHeader(
      rows: Vector[scala.collection.immutable.VectorMap[String, JsonValue]]
  ): Option[List[String]] = {
    // Early exit optimization using iterator.forall for short-circuit
    if (rows.isEmpty) None
    else {
      val first = rows.head
      val keys = first.keys.toList
      if (keys.isEmpty) None
      else {
        val firstKeySet = first.keySet
        // Use iterator for early exit on non-uniform rows
        val uniform = rows.iterator.forall { row =>
          row.keySet == firstKeySet && row.valuesIterator.forall(isPrimitive)
        }
        if (uniform) Some(keys) else None
      }
    }
  }

  private def isPrimitive(v: JsonValue): Boolean = v match {
  case JString(_) | JNumber(_) | JBool(_) | JNull => true
  case _                                          => false
  }

  private def encodeArray(
      key: Option[String],
      values: Vector[JsonValue],
      writer: EncodeLineWriter,
      depth: Int,
      options: EncodeOptions,
      allowFolding: Boolean = true,
  ): Unit = {
    if (values.isEmpty) {
      writer.push(depth, formatHeader(0, key, Nil, options.delimiter))
    } else if (isArrayOfPrimitives(values)) {
      val header = formatHeader(values.length, key, Nil, options.delimiter)
      writer match {
      case sw: StreamLineWriter =>
        sw.pushDelimitedPrimitives(depth, header, values, options.delimiter)
      case _ =>
        // Pre-allocate StringBuilder capacity to avoid resizing during append
        // Estimate: ~10 chars per value + delimiters
        val estimatedSize = values.length * 11
        val sb = new StringBuilder(estimatedSize)
        var i = 0
        while (i < values.length) {
          if (i > 0) sb.append(options.delimiter.char)
          sb.append(encodePrimitive(values(i), options.delimiter))
          i += 1
        }
        val joined = sb.result()
        val line = s"$header $joined"
        writer.push(depth, line)
      }
    } else if (isArrayOfObjects(values)) {
      val rows = values.collect {
        case JObj(m) => m
      }
      extractTabularHeader(rows) match {
      case Some(headerFields) =>
        val header =
          formatHeader(rows.length, key, headerFields, options.delimiter)
        writer.push(depth, header)
        rows.foreach {
          row =>
            writer match {
            case sw: StreamLineWriter =>
              val vs = headerFields.iterator
                .map(k => row(k))
                .toVector
              sw.pushRowPrimitives(depth + 1, vs, options.delimiter)
            case _ =>
              // Pre-allocate StringBuilder capacity to avoid resizing
              val estimatedSize = headerFields.length * 11
              val sb = new StringBuilder(estimatedSize)
              var i = 0
              while (i < headerFields.length) {
                if (i > 0) sb.append(options.delimiter.char)
                val k = headerFields(i)
                sb.append(encodePrimitive(row(k), options.delimiter))
                i += 1
              }
              writer.push(depth + 1, sb.result())
            }
        }
      case None =>
        val header =
          formatHeader(values.length, key, Nil, options.delimiter)
        writer.push(depth, header)
        values.foreach(v => encodeListItem(v, writer, depth + 1, options, allowFolding))
      }
    } else {
      val header = formatHeader(values.length, key, Nil, options.delimiter)
      writer.push(depth, header)
      values.foreach(v => encodeListItem(v, writer, depth + 1, options, allowFolding))
    }
  }

  // TODO: Check feasibility for tailrec
  private def encodeListItem(
      value: JsonValue,
      writer: EncodeLineWriter,
      depth: Int,
      options: EncodeOptions,
      allowFolding: Boolean,
  ): Unit = value match {
  case JNull | JBool(_) | JNumber(_) | JString(_) =>
    writer.pushListItem(depth, encodePrimitive(value, options.delimiter))
  case JArray(inner) if isArrayOfPrimitives(inner) =>
    val header = formatHeader(inner.length, None, Nil, options.delimiter)
    writer match {
    case sw: StreamLineWriter =>
      sw.pushListItemDelimitedPrimitives(depth, header, inner, options.delimiter)
    case _ =>
      // Pre-allocate StringBuilder capacity to avoid resizing
      val estimatedSize = inner.length * 11
      val sb = new StringBuilder(estimatedSize)
      var i = 0
      while (i < inner.length) {
        if (i > 0) sb.append(options.delimiter.char)
        sb.append(encodePrimitive(inner(i), options.delimiter))
        i += 1
      }
      val joined = sb.result()
      val line = if (inner.isEmpty) header else s"$header $joined"
      writer.pushListItem(depth, line)
    }
  case JObj(fields) =>
    val prepared = prepareObjectFields(fields, options, allowFolding)
    if (prepared.isEmpty) {
      writer.pushListItem(depth, "")
    } else {
      val headEntry = prepared.head
      val firstKey = headEntry.key
      val firstVal = headEntry.value
      val headAllow = allowFolding && !headEntry.disableChildFolding
      firstVal match {
      case JNull | JBool(_) | JNumber(_) | JString(_) =>
        writer.pushListItem(
          depth,
          s"${encodeKey(firstKey)}: ${encodePrimitive(firstVal, options.delimiter)}",
        )
      case JArray(arr) if isArrayOfPrimitives(arr) =>
        val header =
          formatHeader(arr.length, Some(firstKey), Nil, options.delimiter)
        writer match {
        case sw: StreamLineWriter =>
          sw.pushListItemDelimitedPrimitives(depth, header, arr, options.delimiter)
        case _ =>
          // Pre-allocate StringBuilder capacity to avoid resizing
          val estimatedSize = arr.length * 11
          val sb = new StringBuilder(estimatedSize)
          var i = 0
          while (i < arr.length) {
            if (i > 0) sb.append(options.delimiter.char)
            sb.append(encodePrimitive(arr(i), options.delimiter))
            i += 1
          }
          val joined = sb.result()
          val line = if (arr.isEmpty) header else s"$header $joined"
          writer.pushListItem(depth, line)
        }
      case JArray(arr) if isArrayOfObjects(arr) =>
        val objectRows = arr.collect {
          case JObj(m) => m
        }
        extractTabularHeader(objectRows) match {
        case Some(headerFields) =>
          val header = formatHeader(
            arr.length,
            Some(firstKey),
            headerFields,
            options.delimiter,
          )
          writer.pushListItem(depth, header)
          objectRows.foreach {
            row =>
              // Pre-allocate StringBuilder capacity to avoid resizing
              val estimatedSize = headerFields.length * 11
              val sb = new StringBuilder(estimatedSize)
              var i = 0
              while (i < headerFields.length) {
                if (i > 0) sb.append(options.delimiter.char)
                val key = headerFields(i)
                sb.append(encodePrimitive(row(key), options.delimiter))
                i += 1
              }
              writer.push(depth + 2, sb.result())
          }
        case None =>
          val header = formatHeader(
            arr.length,
            Some(firstKey),
            Nil,
            options.delimiter,
          )
          writer.pushListItem(depth, header)
          arr.foreach(elem => encodeListItem(elem, writer, depth + 2, options, headAllow))
        }
      case JArray(arr) =>
        val header = formatHeader(
          arr.length,
          Some(firstKey),
          Nil,
          options.delimiter,
        )
        writer.pushListItem(depth, header)
        arr.foreach(elem => encodeListItem(elem, writer, depth + 2, options, headAllow))
      case JObj(next) =>
        writer.pushListItem(depth, s"${encodeKey(firstKey)}:")
        encodeObject(next, writer, depth + 2, options, headAllow)
      }
      prepared.tail.foreach {
        case FieldEntry(k, v, disableChild) =>
          encodeKeyValue(k, v, writer, depth + 1, options, allowFolding && !disableChild)
      }
    }
  case JArray(other) =>
    val header = formatHeader(other.length, None, Nil, options.delimiter)
    writer.pushListItem(depth, header)
    other.foreach(elem => encodeListItem(elem, writer, depth + 1, options, allowFolding))
  }

}
