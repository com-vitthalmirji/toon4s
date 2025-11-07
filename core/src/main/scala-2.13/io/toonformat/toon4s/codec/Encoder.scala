package io.toonformat.toon4s.codec

import io.toonformat.toon4s.JsonValue
import io.toonformat.toon4s.JsonValue._
import scala.collection.immutable.VectorMap

trait Encoder[A] {
  def apply(a: A): JsonValue
}

object Encoder {
  def apply[A](implicit enc: Encoder[A]): Encoder[A] = enc

  // Primitives
  implicit val stringEncoder: Encoder[String]     = (s: String) => JString(s)
  implicit val boolEncoder: Encoder[Boolean]      = (b: Boolean) => JBool(b)
  implicit val bigDecEncoder: Encoder[BigDecimal] = (n: BigDecimal) => JNumber(n)
  implicit val intEncoder: Encoder[Int]           = (n: Int) => JNumber(BigDecimal(n))
  implicit val longEncoder: Encoder[Long]         = (n: Long) => JNumber(BigDecimal(n))
  implicit val doubleEncoder: Encoder[Double]     = (d: Double) =>
    if (d.isInfinity || d.isNaN) JNull else JNumber(BigDecimal(d))
  implicit val floatEncoder: Encoder[Float]       = (f: Float) =>
    if (f.isInfinite || f.isNaN) JNull else JNumber(BigDecimal.decimal(f))
  implicit val bigIntEncoder: Encoder[BigInt]     = (n: BigInt) => JNumber(BigDecimal(n))

  // Collections
  implicit def optionEncoder[A](implicit ev: Encoder[A]): Encoder[Option[A]] =
    (oa: Option[A]) => oa.map(ev.apply).getOrElse(JNull)

  implicit def iterableEncoder[A, C[X] <: Iterable[X]](implicit ev: Encoder[A]): Encoder[C[A]] =
    (it: C[A]) => JArray(it.iterator.map(ev.apply).toVector)

  implicit def vectorEncoder[A](implicit ev: Encoder[A]): Encoder[Vector[A]] =
    (v: Vector[A]) => JArray(v.map(ev.apply))

  implicit def listEncoder[A](implicit ev: Encoder[A]): Encoder[List[A]] =
    (v: List[A]) => JArray(v.iterator.map(ev.apply).toVector)

  implicit def mapEncoder[A](implicit ev: Encoder[A]): Encoder[Map[String, A]] =
    (m: Map[String, A]) =>
      JObj(VectorMap.from(m.iterator.map {
        case (k, v) => k -> ev.apply(v)
      }))

  // Generic Product (case classes / tuples) using Product API
  implicit def productEncoder[A <: Product]: Encoder[A] = (p: A) => {
    val names  = p.productElementNames
    val values = p.productIterator
    val kvs    = names
      .zip(values)
      .map {
        case (k, v) => k -> anyToJson(v)
      }
      .toList
    JObj(VectorMap.from(kvs))
  }

  private def anyToJson(v: Any): JsonValue = v match {
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
    case Some(x)                 => anyToJson(x)
    case None                    => JNull
    case m: Map[_, _]            =>
      val it = m.asInstanceOf[Map[String, Any]].iterator.map {
        case (k, v) => k -> anyToJson(v)
      }
      JObj(VectorMap.from(it))
    case it: Iterable[_]         => JArray(it.iterator.map(anyToJson).toVector)
    case p: Product              => productEncoder[Product].apply(p)
    case other                   => JString(String.valueOf(other))
  }
}
