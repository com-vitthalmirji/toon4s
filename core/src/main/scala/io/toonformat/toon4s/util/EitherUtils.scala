package io.toonformat.toon4s.util

import scala.util.Try

object EitherUtils {

  def fromTry[E, A](t: Try[A])(toErr: Throwable => E): Either[E, A] =
    t.toEither.left.map(toErr)

  implicit final class TryOps[A](private val t: Try[A]) extends AnyVal {

    def toEitherMap[E](toErr: Throwable => E): Either[E, A] = fromTry(t)(toErr)

  }

}
