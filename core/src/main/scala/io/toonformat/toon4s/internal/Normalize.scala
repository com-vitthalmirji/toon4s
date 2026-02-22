package io.toonformat.toon4s
package internal

import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoField, TemporalAccessor}

import scala.collection.{Map => ScalaMap}
import scala.collection.immutable.VectorMap
import scala.util.Try

import io.toonformat.toon4s.JsonValue._

private[toon4s] object Normalize {

  // TODO: Check feasibility for tailrec
  def toJson(value: Any): JsonValue = value match {
  case jv: JsonValue           => jv
  case null                    => JNull
  case s: String               => JString(s)
  case b: Boolean              => JBool(b)
  case i: Byte                 => JNumber(BigDecimal(i))
  case s: Short                => JNumber(BigDecimal(s))
  case i: Int                  => JNumber(BigDecimal(i))
  case l: Long                 => JNumber(BigDecimal(l))
  case bi: BigInt              => normalizeBigInt(bi)
  case f: Float if f.isFinite  => JNumber(BigDecimal.decimal(f))
  case d: Double if d.isFinite => JNumber(BigDecimal(d))
  case f: Float                => JNull
  case d: Double               => JNull
  case bd: BigDecimal          => JNumber(bd)
  case Some(v)                 => toJson(v)
  case None                    => JNull
  case m: ScalaMap[_, _]       =>
    val iter = m.iterator
    if (!iter.hasNext) JObj(VectorMap.empty)
    else {
      val builder = VectorMap.newBuilder[String, JsonValue]
      while (iter.hasNext) {
        val (k, v) = iter.next()
        builder.addOne(stringifyKey(k), toJson(v))
      }
      JObj(builder.result())
    }
  case set: collection.Set[_] =>
    iteratorToJArray(set.iterator)
  case array: Array[_] =>
    iteratorToJArray(array.iterator)
  case temporal: TemporalAccessor =>
    JString(formatTemporal(temporal))
  case date: java.util.Date =>
    JString(date.toInstant.toString)
  case it: Iterable[_] =>
    iteratorToJArray(it.iterator)
  case p: Product =>
    val names = p.productElementNames
    val elems = p.productIterator
    if (!names.hasNext) JObj(VectorMap.empty)
    else {
      val builder = VectorMap.newBuilder[String, JsonValue]
      while (names.hasNext) {
        builder.addOne(names.next(), toJson(elems.next()))
      }
      JObj(builder.result())
    }
  case other =>
    JString(String.valueOf(other))
  }

  private def iteratorToJArray(iter: Iterator[_]): JArray = {
    if (!iter.hasNext) JArray(Vector.empty)
    else {
      val builder = Vector.newBuilder[JsonValue]
      while (iter.hasNext) {
        builder.addOne(toJson(iter.next()))
      }
      JArray(builder.result())
    }
  }

  private val SafeIntegerMax = BigInt("9007199254740991")

  private val SafeIntegerMin = -SafeIntegerMax

  private def normalizeBigInt(value: BigInt): JsonValue = {
    if (value >= SafeIntegerMin && value <= SafeIntegerMax) JNumber(BigDecimal(value))
    else JString(value.toString)
  }

  private def stringifyKey(key: Any): String = key match {
  case s: String => s
  case other     => String.valueOf(other)
  }

  private def formatTemporal(temporal: TemporalAccessor): String = temporal match {
  case instant: Instant             => instant.toString
  case zoned: ZonedDateTime         => zoned.toInstant.toString
  case offset: OffsetDateTime       => offset.toInstant.toString
  case localDateTime: LocalDateTime =>
    localDateTime.atOffset(ZoneOffset.UTC).toInstant.toString
  case localDate: LocalDate =>
    localDate.atStartOfDay(ZoneOffset.UTC).toInstant.toString
  case localTime: LocalTime =>
    localTime
      .atDate(LocalDate.ofEpochDay(0))
      .atOffset(ZoneOffset.UTC)
      .toInstant
      .toString
  case _ if temporal.isSupported(ChronoField.INSTANT_SECONDS) =>
    val seconds = temporal.getLong(ChronoField.INSTANT_SECONDS)
    val nanos = temporal.get(ChronoField.NANO_OF_SECOND)
    Instant.ofEpochSecond(seconds, nanos).toString
  case _ =>
    Try(DateTimeFormatter.ISO_DATE_TIME.format(temporal)).getOrElse(temporal.toString)
  }

}
