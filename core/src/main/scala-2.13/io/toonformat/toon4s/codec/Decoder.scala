package io.toonformat.toon4s.codec

import io.toonformat.toon4s.JsonValue
import io.toonformat.toon4s.JsonValue._
import io.toonformat.toon4s.error.DecodeError
import java.time.{Instant, LocalDate, LocalDateTime, OffsetDateTime, ZonedDateTime, LocalTime}

trait Decoder[A] {
  def apply(v: JsonValue): Either[DecodeError, A]
}

object Decoder {
  def apply[A](implicit d: Decoder[A]): Decoder[A] = d

  // Primitives
  implicit val stringDecoder: Decoder[String] = {
    case JString(s) => Right(s)
    case other      => Left(DecodeError.Mapping(s"Expected string, found $other"))
  }

  implicit val booleanDecoder: Decoder[Boolean] = {
    case JBool(b) => Right(b)
    case other    => Left(DecodeError.Mapping(s"Expected boolean, found $other"))
  }

  implicit val bigDecimalDecoder: Decoder[BigDecimal] = {
    case JNumber(n) => Right(n)
    case other      => Left(DecodeError.Mapping(s"Expected number, found $other"))
  }

  implicit val intDecoder: Decoder[Int] = {
    case JNumber(n) if n.isValidInt => Right(n.toInt)
    case other                      => Left(DecodeError.Mapping(s"Expected int, found $other"))
  }

  implicit val longDecoder: Decoder[Long] = {
    case JNumber(n) if n.isValidLong => Right(n.toLong)
    case other                       => Left(DecodeError.Mapping(s"Expected long, found $other"))
  }

  implicit val doubleDecoder: Decoder[Double] = {
    case JNumber(n) => Right(n.toDouble)
    case other      => Left(DecodeError.Mapping(s"Expected double, found $other"))
  }

  // Collections
  implicit def optionDecoder[A](implicit d: Decoder[A]): Decoder[Option[A]] = {
    case JNull => Right(None)
    case x     => d(x).map(Some(_))
  }

  implicit def listDecoder[A](implicit d: Decoder[A]): Decoder[List[A]] = {
    case JArray(xs) =>
      xs.foldLeft[Either[DecodeError, List[A]]](Right(Nil)) {
        case (Right(acc), j)  => d(j).map(_ :: acc)
        case (l @ Left(_), _) => l
      }.map(_.reverse)
    case other      => Left(DecodeError.Mapping(s"Expected array, found $other"))
  }

  implicit def vectorDecoder[A](implicit d: Decoder[A]): Decoder[Vector[A]] = {
    case JArray(xs) =>
      xs.foldLeft[Either[DecodeError, Vector[A]]](Right(Vector.empty)) {
        case (Right(acc), j)  => d(j).map(acc :+ _)
        case (l @ Left(_), _) => l
      }
    case other      => Left(DecodeError.Mapping(s"Expected array, found $other"))
  }

  implicit def mapDecoder[A](implicit d: Decoder[A]): Decoder[Map[String, A]] = {
    case JObj(fields) =>
      fields.foldLeft[Either[DecodeError, Map[String, A]]](Right(Map.empty)) {
        case (Right(acc), (k, j)) =>
          d(j).map(
            a => acc + (k -> a)
          )
        case (l @ Left(_), _)     => l
      }
    case other        => Left(DecodeError.Mapping(s"Expected object, found $other"))
  }

  // java.time instances (strict ISO-8601 parsing)
  implicit val instantDecoder: Decoder[Instant] = {
    case JString(s) =>
      try Right(Instant.parse(s))
      catch {
        case _: Throwable => Left(DecodeError.Mapping(s"Instant expected (ISO-8601), found: $s"))
      }
    case other      => Left(DecodeError.Mapping(s"Expected string for Instant, found $other"))
  }

  implicit val localDateDecoder: Decoder[LocalDate] = {
    case JString(s) =>
      try Right(LocalDate.parse(s))
      catch {
        case _: Throwable =>
          Left(DecodeError.Mapping(s"LocalDate expected (YYYY-MM-DD), found: $s"))
      }
    case other      => Left(DecodeError.Mapping(s"Expected string for LocalDate, found $other"))
  }

  implicit val localDateTimeDecoder: Decoder[LocalDateTime] = {
    case JString(s) =>
      try Right(LocalDateTime.parse(s))
      catch { case _: Throwable => Left(DecodeError.Mapping(s"LocalDateTime expected, found: $s")) }
    case other      => Left(DecodeError.Mapping(s"Expected string for LocalDateTime, found $other"))
  }

  implicit val offsetDateTimeDecoder: Decoder[OffsetDateTime] = {
    case JString(s) =>
      try Right(OffsetDateTime.parse(s))
      catch {
        case _: Throwable => Left(DecodeError.Mapping(s"OffsetDateTime expected, found: $s"))
      }
    case other      => Left(DecodeError.Mapping(s"Expected string for OffsetDateTime, found $other"))
  }

  implicit val zonedDateTimeDecoder: Decoder[ZonedDateTime] = {
    case JString(s) =>
      try Right(ZonedDateTime.parse(s))
      catch { case _: Throwable => Left(DecodeError.Mapping(s"ZonedDateTime expected, found: $s")) }
    case other      => Left(DecodeError.Mapping(s"Expected string for ZonedDateTime, found $other"))
  }

  implicit val localTimeDecoder: Decoder[LocalTime] = {
    case JString(s) =>
      try Right(LocalTime.parse(s))
      catch {
        case _: Throwable =>
          Left(DecodeError.Mapping(s"LocalTime expected (hh:mm[:ss[.SSS]]), found: $s"))
      }
    case other      => Left(DecodeError.Mapping(s"Expected string for LocalTime, found $other"))
  }
}
