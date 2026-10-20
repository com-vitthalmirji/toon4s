package io.toonformat.toon4s
package internal

import scala.collection.immutable.VectorMap
import scala.jdk.CollectionConverters._

import io.toonformat.toon4s.JsonValue._

/**
 * Recognizes the `java.*` collection and value types that JToon4s callers (or Scala callers passing
 * Java values through) hand to the encoder. Kept separate from [[Normalize]] so its general-purpose
 * dispatch does not need to know about Java-specific types.
 */
private[toon4s] object JavaNormalize {

  def toJson(value: Any)(
      recur: Any => JsonValue,
      stringifyKey: Any => String,
      bigIntToJson: BigInt => JsonValue,
  ): Option[JsonValue] = value match {
  case m: java.util.Map[_, _] =>
    val iter = m.entrySet.iterator
    if (!iter.hasNext) Some(JObj(VectorMap.empty))
    else {
      val builder = VectorMap.newBuilder[String, JsonValue]
      while (iter.hasNext) {
        val entry = iter.next()
        builder.addOne(stringifyKey(entry.getKey), recur(entry.getValue))
      }
      Some(JObj(builder.result()))
    }
  case it: java.lang.Iterable[_] =>
    val iter = it.iterator.asScala
    if (!iter.hasNext) Some(JArray(Vector.empty))
    else {
      val builder = Vector.newBuilder[JsonValue]
      while (iter.hasNext) {
        builder.addOne(recur(iter.next()))
      }
      Some(JArray(builder.result()))
    }
  case bd: java.math.BigDecimal =>
    Some(JNumber(BigDecimal(bd)))
  case bi: java.math.BigInteger =>
    Some(bigIntToJson(BigInt(bi)))
  case opt: java.util.Optional[_] =>
    Some(if (opt.isPresent) recur(opt.get) else JNull)
  case _ =>
    None
  }

}
