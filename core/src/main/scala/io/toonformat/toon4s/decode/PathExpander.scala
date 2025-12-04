package io.toonformat.toon4s
package decode

import scala.collection.immutable.VectorMap

import io.toonformat.toon4s.JsonValue
import io.toonformat.toon4s.JsonValue._
import io.toonformat.toon4s.error.DecodeError

private[decode] object PathExpander {

  private val IdentifierSegmentPattern = "^[A-Za-z_][A-Za-z0-9_]*$".r

  private val QuotedKeyPrefix = "\u0001"

  def expand(value: JsonValue, options: DecodeOptions): JsonValue = {
    if (options.expandPaths != PathExpansion.Safe) stripQuoted(value)
    else expandValue(value, options.strictness == Strictness.Strict)
  }

  private def stripQuoted(value: JsonValue): JsonValue = value match {
  case JObj(fields) =>
    val cleaned = fields.map {
      case (k, v) =>
        val key = if (k.startsWith(QuotedKeyPrefix)) k.drop(1) else k
        key -> stripQuoted(v)
    }
    JObj(VectorMap.from(cleaned))
  case JArray(values) => JArray(values.map(stripQuoted))
  case other          => other
  }

  private def expandValue(value: JsonValue, strict: Boolean): JsonValue = value match {
  case JObj(fields) =>
    JObj(expandObject(fields, strict))
  case JArray(values) =>
    JArray(values.map(v => expandValue(v, strict)))
  case other => other
  }

  private def expandObject(
      fields: VectorMap[String, JsonValue],
      strict: Boolean,
  ): VectorMap[String, JsonValue] = {
    var acc = VectorMap.empty[String, JsonValue]
    fields.foreach {
      case (key, rawValue) =>
        val value = expandValue(rawValue, strict)
        val (rawKey, wasQuoted) =
          if (key.startsWith(QuotedKeyPrefix)) (key.drop(1), true) else (key, false)
        if (wasQuoted) acc = acc + (rawKey -> value)
        else {
          val segments =
            if (rawKey.contains(".") && isEligiblePath(rawKey)) rawKey.split("\\.", -1).toVector
            else Vector(rawKey)
          acc = mergeSegments(acc, segments, value, strict, pathKey = rawKey)
        }
    }
    acc
  }

  private def isEligiblePath(key: String): Boolean = {
    val segments = key.split("\\.", -1)
    segments.nonEmpty && segments.forall(seg => IdentifierSegmentPattern.matches(seg))
  }

  private def mergeSegments(
      acc: VectorMap[String, JsonValue],
      segments: Vector[String],
      value: JsonValue,
      strict: Boolean,
      pathKey: String,
  ): VectorMap[String, JsonValue] = segments match {
  case head +: tail if tail.isEmpty =>
    mergeLeaf(acc, head, value, strict, pathKey)
  case head +: tail =>
    val child: VectorMap[String, JsonValue] = acc.get(head) match {
    case Some(JObj(obj)) => obj
    case Some(existing)  =>
      if (strict)
        throw DecodeError.Syntax(
          s"Path expansion conflict at '$head' (existing=${typeName(existing)}, incoming=object) while expanding '$pathKey'"
        )
      else VectorMap.empty
    case None => VectorMap.empty
    }
    val mergedChild = mergeSegments(child, tail, value, strict, pathKey)
    acc.updated(head, JObj(mergedChild))
  case _ => acc
  }

  private def mergeLeaf(
      acc: VectorMap[String, JsonValue],
      key: String,
      value: JsonValue,
      strict: Boolean,
      pathKey: String,
  ): VectorMap[String, JsonValue] = {
    acc.get(key) match {
    case None                 => acc + (key -> value)
    case Some(JObj(existing)) =>
      value match {
      case JObj(incoming) =>
        val merged = mergeObjects(existing, incoming, strict, pathKey)
        acc.updated(key, JObj(merged))
      case _ =>
        if (strict)
          throw DecodeError.Syntax(
            s"Path expansion conflict at '$key' (existing=object, incoming=${typeName(value)}) while expanding '$pathKey'"
          )
        else acc.updated(key, value)
      }
    case Some(existing) =>
      value match {
      case JObj(incoming) =>
        if (strict)
          throw DecodeError.Syntax(
            s"Path expansion conflict at '$key' (existing=${typeName(existing)}, incoming=object) while expanding '$pathKey'"
          )
        else acc.updated(key, JObj(incoming))
      case _ =>
        if (strict)
          throw DecodeError.Syntax(
            s"Path expansion conflict at '$key' (existing=${typeName(existing)}, incoming=${typeName(value)}) while expanding '$pathKey'"
          )
        else acc.updated(key, value)
      }
    }
  }

  private def typeName(value: JsonValue): String = value match {
  case JObj(_)    => "object"
  case JArray(_)  => "array"
  case JString(_) => "string"
  case JNumber(_) => "number"
  case JBool(_)   => "boolean"
  case JNull      => "null"
  }

  private def mergeObjects(
      left: VectorMap[String, JsonValue],
      right: VectorMap[String, JsonValue],
      strict: Boolean,
      pathKey: String,
  ): VectorMap[String, JsonValue] = {
    var acc = left
    right.foreach {
      case (k, v) =>
        acc = mergeSegments(acc, Vector(k), v, strict, pathKey)
    }
    acc
  }

}
