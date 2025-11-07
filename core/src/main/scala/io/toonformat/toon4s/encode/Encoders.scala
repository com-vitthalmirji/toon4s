package io.toonformat.toon4s
package encode

import io.toonformat.toon4s.JsonValue._
import io.toonformat.toon4s.{Delimiter, EncodeOptions}
import Primitives._

object Encoders {
  def encode(value: JsonValue, options: EncodeOptions): String = value match {
    case JNull | JBool(_) | JNumber(_) | JString(_) =>
      encodePrimitive(value, options.delimiter)
    case JArray(values)                             =>
      val writer = new LineWriter(options.indent)
      encodeArray(None, values, writer, 0, options)
      writer.toString
    case JObj(fields)                               =>
      val writer = new LineWriter(options.indent)
      encodeObject(fields, writer, 0, options)
      writer.toString
  }

  private def formatHeader(
      length: Int,
      key: Option[String],
      fields: List[String],
      delimiter: Delimiter,
      lengthMarker: Boolean
  ): String = {
    val delimiterSuffix = delimiter match {
      case Delimiter.Tab   => "\t"
      case Delimiter.Pipe  => "|"
      case Delimiter.Comma => ""
    }
    val lengthLabel     = (if (lengthMarker) s"#${length}" else length.toString) + delimiterSuffix
    val lengthPart      = s"[${lengthLabel}]"
    val keyPart         = key.map(encodeKey).getOrElse("")
    val fieldsPart      =
      if (fields.nonEmpty) {
        val joined = fields.map(encodeKey).mkString(delimiter.char.toString)
        s"{${joined}}"
      } else ""
    s"${keyPart}${lengthPart}${fieldsPart}:"
  }

  private def encodeObject(
      fields: Map[String, JsonValue],
      writer: LineWriter,
      depth: Int,
      options: EncodeOptions
  ): Unit = {
    fields.foreach {
      case (k, v) =>
        encodeKeyValue(k, v, writer, depth, options)
    }
  }

  private def encodeKeyValue(
      key: String,
      value: JsonValue,
      writer: LineWriter,
      depth: Int,
      options: EncodeOptions
  ): Unit = value match {
    case JNull | JBool(_) | JNumber(_) | JString(_) =>
      writer.push(depth, s"${encodeKey(key)}: ${encodePrimitive(value, options.delimiter)}")
    case JArray(values)                             =>
      encodeArray(Some(key), values, writer, depth, options)
    case JObj(obj)                                  =>
      writer.push(depth, s"${encodeKey(key)}:")
      if (obj.nonEmpty) {
        encodeObject(obj, writer, depth + 1, options)
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

  private def extractTabularHeader(rows: Vector[Map[String, JsonValue]]): Option[List[String]] = {
    rows.headOption.flatMap {
      first =>
        val keys    = first.keys.toList
        val uniform = rows.forall {
          row =>
            row.keySet == first.keySet && row.forall {
              case (_, v) => isPrimitive(v)
            }
        }
        if (uniform && keys.nonEmpty) Some(keys) else None
    }
  }

  private def isPrimitive(v: JsonValue): Boolean = v match {
    case JString(_) | JNumber(_) | JBool(_) | JNull => true
    case _                                          => false
  }

  private def encodeArray(
      key: Option[String],
      values: Vector[JsonValue],
      writer: LineWriter,
      depth: Int,
      options: EncodeOptions
  ): Unit = {
    if (values.isEmpty) {
      writer.push(depth, formatHeader(0, key, Nil, options.delimiter, options.lengthMarker))
    } else if (isArrayOfPrimitives(values)) {
      val header = formatHeader(values.length, key, Nil, options.delimiter, options.lengthMarker)
      val joined = values
        .map(
          v => encodePrimitive(v, options.delimiter)
        )
        .mkString(options.delimiter.char.toString)
      val line   = if (values.isEmpty) header else s"$header $joined"
      writer.push(depth, line)
    } else if (isArrayOfObjects(values)) {
      val rows = values.collect {
        case JObj(m) => m
      }
      extractTabularHeader(rows) match {
        case Some(headerFields) =>
          val header =
            formatHeader(rows.length, key, headerFields, options.delimiter, options.lengthMarker)
          writer.push(depth, header)
          rows.foreach {
            row =>
              val line = headerFields
                .map(
                  k => encodePrimitive(row(k), options.delimiter)
                )
                .mkString(options.delimiter.char.toString)
              writer.push(depth + 1, line)
          }
        case None               =>
          val header =
            formatHeader(values.length, key, Nil, options.delimiter, options.lengthMarker)
          writer.push(depth, header)
          values.foreach(
            v => encodeListItem(v, writer, depth + 1, options)
          )
      }
    } else {
      val header = formatHeader(values.length, key, Nil, options.delimiter, options.lengthMarker)
      writer.push(depth, header)
      values.foreach(
        v => encodeListItem(v, writer, depth + 1, options)
      )
    }
  }

  private def encodeListItem(
      value: JsonValue,
      writer: LineWriter,
      depth: Int,
      options: EncodeOptions
  ): Unit = value match {
    case JNull | JBool(_) | JNumber(_) | JString(_)  =>
      writer.pushListItem(depth, encodePrimitive(value, options.delimiter))
    case JArray(inner) if isArrayOfPrimitives(inner) =>
      val header = formatHeader(inner.length, None, Nil, options.delimiter, options.lengthMarker)
      val joined = inner
        .map(
          elem => encodePrimitive(elem, options.delimiter)
        )
        .mkString(options.delimiter.char.toString)
      val line   = if (inner.isEmpty) header else s"$header $joined"
      writer.pushListItem(depth, line)
    case JObj(fields)                                =>
      if (fields.isEmpty) {
        writer.pushListItem(depth, "")
      } else {
        val (firstKey, firstVal) = fields.head
        firstVal match {
          case JNull | JBool(_) | JNumber(_) | JString(_) =>
            writer.pushListItem(
              depth,
              s"${encodeKey(firstKey)}: ${encodePrimitive(firstVal, options.delimiter)}"
            )
          case JArray(arr) if isArrayOfPrimitives(arr)    =>
            val header =
              formatHeader(arr.length, Some(firstKey), Nil, options.delimiter, options.lengthMarker)
            val joined = arr
              .map(
                elem => encodePrimitive(elem, options.delimiter)
              )
              .mkString(options.delimiter.char.toString)
            val line   = if (arr.isEmpty) header else s"$header $joined"
            writer.pushListItem(depth, line)
          case JArray(arr) if isArrayOfObjects(arr)       =>
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
                  options.lengthMarker
                )
                writer.pushListItem(depth, header)
                objectRows.foreach {
                  row =>
                    val line = headerFields
                      .map(
                        key => encodePrimitive(row(key), options.delimiter)
                      )
                      .mkString(options.delimiter.char.toString)
                    writer.push(depth + 1, line)
                }
              case None               =>
                val header = formatHeader(
                  arr.length,
                  Some(firstKey),
                  Nil,
                  options.delimiter,
                  options.lengthMarker
                )
                writer.pushListItem(depth, header)
                arr.foreach(
                  elem => encodeListItem(elem, writer, depth + 1, options)
                )
            }
          case JArray(arr)                                =>
            val header = formatHeader(
              arr.length,
              Some(firstKey),
              Nil,
              options.delimiter,
              options.lengthMarker
            )
            writer.pushListItem(depth, header)
            arr.foreach(
              elem => encodeListItem(elem, writer, depth + 1, options)
            )
          case JObj(next)                                 =>
            writer.pushListItem(depth, s"${encodeKey(firstKey)}:")
            encodeObject(next, writer, depth + 2, options)
        }
        fields.tail.foreach {
          case (k, v) =>
            encodeKeyValue(k, v, writer, depth + 1, options)
        }
      }
    case JArray(other)                               =>
      val header = formatHeader(other.length, None, Nil, options.delimiter, options.lengthMarker)
      writer.pushListItem(depth, header)
      other.foreach(
        elem => encodeListItem(elem, writer, depth + 1, options)
      )
  }
}
