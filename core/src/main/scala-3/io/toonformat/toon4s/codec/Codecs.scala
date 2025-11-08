package io.toonformat.toon4s.codec

import scala.collection.immutable.VectorMap
import scala.compiletime.{constValue, erasedValue}
import scala.deriving.Mirror

import io.toonformat.toon4s.JsonValue
import io.toonformat.toon4s.JsonValue._

trait Encoder[A] {

  def apply(a: A): JsonValue

}

object Encoder {

  def apply[A](using enc: Encoder[A]): Encoder[A] = enc

  inline def derived[A](using m: Mirror.Of[A]): Encoder[A] =
    inline m match
    case p: Mirror.ProductOf[A] => productEncoder(p)

  private inline def productEncoder[A](p: Mirror.ProductOf[A]): Encoder[A] =
    new Encoder[A] {
      def apply(a: A): JsonValue =
        val labels = constValueTuple[p.MirroredElemLabels]
        val values = a.asInstanceOf[Product].productIterator.toList
        val kvs = labels.zip(values).map {
          case (k, v) => k -> toJson(v)
        }
        JObj(VectorMap.from(kvs))
    }

  private inline def constValueTuple[T <: Tuple]: List[String] =
    inline erasedValue[T] match
    case _: EmptyTuple => Nil
    case _: (h *: t)   => constValue[h].toString :: constValueTuple[t]

  private def toJson(v: Any): JsonValue = v match {
  case null                    => JNull
  case s: String               => JString(s)
  case b: Boolean              => JBool(b)
  case i: Byte                 => JNumber(BigDecimal(i))
  case s: Short                => JNumber(BigDecimal(s))
  case i: Int                  => JNumber(BigDecimal(i))
  case l: Long                 => JNumber(BigDecimal(l))
  case bi: BigInt              => JNumber(BigDecimal(bi))
  case f: Float if f.isFinite  => JNumber(BigDecimal.decimal(f))
  case d: Double if d.isFinite => JNumber(BigDecimal(d))
  case f: Float                => JNull
  case d: Double               => JNull
  case bd: BigDecimal          => JNumber(bd)
  case Some(x)                 => toJson(x)
  case None                    => JNull
  case m: Map[?, ?]            =>
    val it = m.asInstanceOf[Map[String, Any]].iterator.map {
      case (k, v) => k -> toJson(v)
    }
    JObj(VectorMap.from(it))
  case it: Iterable[?] => JArray(it.iterator.map(toJson).toVector)
  case p: Product      =>
    val names = p.productElementNames.toList
    val values = p.productIterator.toList
    val kvs = names.zip(values).map {
      case (k, v) => k -> toJson(v)
    }
    JObj(VectorMap.from(kvs))
  case other => JString(String.valueOf(other))
  }

}
