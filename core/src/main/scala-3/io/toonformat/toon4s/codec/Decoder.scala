package io.toonformat.toon4s.codec

import io.toonformat.toon4s.JsonValue
import io.toonformat.toon4s.JsonValue._
import io.toonformat.toon4s.error.DecodeError
import scala.collection.immutable.VectorMap
import scala.deriving.Mirror
import scala.compiletime.{erasedValue, summonInline, constValue}
import java.time.{Instant, LocalDate, LocalDateTime, OffsetDateTime, ZonedDateTime, LocalTime}

trait Decoder[A]:
  def apply(v: JsonValue): Either[DecodeError, A]

object Decoder:
  inline def apply[A](using d: Decoder[A]): Decoder[A] = d

  // Primitive instances
  given Decoder[String] with
    def apply(v: JsonValue) = v match
      case JString(s) => Right(s)
      case other      => Left(DecodeError.Mapping(s"Expected string, found $other"))

  given Decoder[Boolean] with
    def apply(v: JsonValue) = v match
      case JBool(b) => Right(b)
      case other    => Left(DecodeError.Mapping(s"Expected boolean, found $other"))

  given Decoder[BigDecimal] with
    def apply(v: JsonValue) = v match
      case JNumber(n) => Right(n)
      case other      => Left(DecodeError.Mapping(s"Expected number, found $other"))

  given Decoder[Int] with
    def apply(v: JsonValue) = v match
      case JNumber(n) if n.isValidInt => Right(n.toInt)
      case other                      => Left(DecodeError.Mapping(s"Expected int, found $other"))

  given Decoder[Long] with
    def apply(v: JsonValue) = v match
      case JNumber(n) if n.isValidLong => Right(n.toLong)
      case other                       => Left(DecodeError.Mapping(s"Expected long, found $other"))

  given Decoder[Double] with
    def apply(v: JsonValue) = v match
      case JNumber(n) => Right(n.toDouble)
      case other      => Left(DecodeError.Mapping(s"Expected double, found $other"))

  // Collections
  given [A](using d: Decoder[A]): Decoder[List[A]] with
    def apply(v: JsonValue) = v match
      case JArray(xs) =>
        xs.foldLeft[Either[DecodeError, List[A]]](Right(Nil)) {
          (acc, j) =>
            for
              list <- acc
              a    <- d(j)
            yield a :: list
        }.map(_.reverse)
      case other      => Left(DecodeError.Mapping(s"Expected array, found $other"))

  given [A](using d: Decoder[A]): Decoder[Vector[A]] with
    def apply(v: JsonValue) = v match
      case JArray(xs) =>
        val b                       = Vector.newBuilder[A]
        val it                      = xs.iterator
        var err: DecodeError | Null = null
        while (it.hasNext && err == null) do
          d(it.next()) match
            case Right(a) => b += a
            case Left(e)  => err = e
        if err == null then Right(b.result()) else Left(err.nn)
      case other      => Left(DecodeError.Mapping(s"Expected array, found $other"))

  given [A](using d: Decoder[A]): Decoder[Option[A]] with
    def apply(v: JsonValue) = v match
      case JNull => Right(None)
      case x     => d(x).map(Some(_))

  given [A](using d: Decoder[A]): Decoder[Map[String, A]] with
    def apply(v: JsonValue) = v match
      case JObj(fields) =>
        val b                       = Map.newBuilder[String, A]
        val it                      = fields.iterator
        var err: DecodeError | Null = null
        while (it.hasNext && err == null) do
          val (k, j) = it.next()
          d(j) match
            case Right(a) => b += k -> a
            case Left(e)  => err = e
        if err == null then Right(b.result()) else Left(err.nn)
      case other        => Left(DecodeError.Mapping(s"Expected object, found $other"))

  // Product derivation (case classes)
  inline given derived[A](using m: Mirror.ProductOf[A]): Decoder[A] =
    val labels = constValueTuple[m.MirroredElemLabels]
    val decs   = summonAll[m.MirroredElemTypes]
    val flags  = optionFlags[m.MirroredElemTypes]
    new Decoder[A] {
      def apply(v: JsonValue) = v match
        case JObj(fields) =>
          decodeProduct[A](fields, labels, decs, flags)
        case other        => Left(DecodeError.Mapping(s"Expected object for product, found $other"))
    }

  private inline def constValueTuple[T <: Tuple]: List[String] =
    inline erasedValue[T] match
      case _: EmptyTuple => Nil
      case _: (h *: t)   => constValue[h].toString :: constValueTuple[t]

  private inline def summonAll[T <: Tuple]: List[Decoder[_]] =
    inline erasedValue[T] match
      case _: EmptyTuple => Nil
      case _: (h *: t)   => summonInline[Decoder[h]] :: summonAll[t]

  private def decodeProduct[A](
      fields: VectorMap[String, JsonValue],
      labels: List[String],
      decs: List[Decoder[_]],
      optFlags: List[Boolean]
  )(using m: Mirror.ProductOf[A]): Either[DecodeError, A] =
    val builder                 = new Array[Any](labels.length)
    var i                       = 0
    var err: DecodeError | Null = null
    while (i < labels.length && err == null) do
      val label = labels(i)
      val dec   = decs(i).asInstanceOf[Decoder[Any]]
      val isOpt = optFlags(i)
      fields.get(label) match
        case Some(value) =>
          dec(value) match
            case Right(a) => builder(i) = a
            case Left(e)  => err = e
        case None        =>
          if isOpt then builder(i) = None
          else err = DecodeError.Mapping(s"Missing required field '$label'")
      i += 1
    if err == null then
      try
        Right(m.fromProduct(new Product {
          def canEqual(that: Any): Boolean = true
          def productArity: Int            = builder.length
          def productElement(n: Int): Any  = builder(n)
        }))
      catch case t: Throwable => Left(DecodeError.Mapping(t.getMessage))
    else Left(err.nn)

  private inline def isOption[A]: Boolean = inline erasedValue[A] match
    case _: Option[?] => true
    case _            => false

  private inline def optionFlags[T <: Tuple]: List[Boolean] =
    inline erasedValue[T] match
      case _: EmptyTuple => Nil
      case _: (h *: t)   => isOption[h] :: optionFlags[t]

  // java.time instances (strict ISO-8601 parsing)
  given Decoder[Instant] with
    def apply(v: JsonValue) = v match
      case JString(s) =>
        try Right(Instant.parse(s))
        catch
          case _: Throwable => Left(DecodeError.Mapping(s"Instant expected (ISO-8601), found: $s"))
      case other      => Left(DecodeError.Mapping(s"Expected string for Instant, found $other"))

  given Decoder[LocalDate] with
    def apply(v: JsonValue) = v match
      case JString(s) =>
        try Right(LocalDate.parse(s))
        catch
          case _: Throwable =>
            Left(DecodeError.Mapping(s"LocalDate expected (YYYY-MM-DD), found: $s"))
      case other      => Left(DecodeError.Mapping(s"Expected string for LocalDate, found $other"))

  given Decoder[LocalDateTime] with
    def apply(v: JsonValue) = v match
      case JString(s) =>
        try Right(LocalDateTime.parse(s))
        catch
          case _: Throwable =>
            Left(
              DecodeError.Mapping(s"LocalDateTime expected (YYYY-MM-DDThh:mm:ss[.SSS]), found: $s")
            )
      case other      => Left(DecodeError.Mapping(s"Expected string for LocalDateTime, found $other"))

  given Decoder[OffsetDateTime] with
    def apply(v: JsonValue) = v match
      case JString(s) =>
        try Right(OffsetDateTime.parse(s))
        catch case _: Throwable => Left(DecodeError.Mapping(s"OffsetDateTime expected, found: $s"))
      case other      => Left(DecodeError.Mapping(s"Expected string for OffsetDateTime, found $other"))

  given Decoder[ZonedDateTime] with
    def apply(v: JsonValue) = v match
      case JString(s) =>
        try Right(ZonedDateTime.parse(s))
        catch case _: Throwable => Left(DecodeError.Mapping(s"ZonedDateTime expected, found: $s"))
      case other      => Left(DecodeError.Mapping(s"Expected string for ZonedDateTime, found $other"))

  given Decoder[LocalTime] with
    def apply(v: JsonValue) = v match
      case JString(s) =>
        try Right(LocalTime.parse(s))
        catch
          case _: Throwable =>
            Left(DecodeError.Mapping(s"LocalTime expected (hh:mm[:ss[.SSS]]), found: $s"))
      case other      => Left(DecodeError.Mapping(s"Expected string for LocalTime, found $other"))
