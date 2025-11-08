package io.toonformat

import io.toonformat.toon4s.error.{DecodeError, EncodeError, ToonError}

package object toon4s {

  type Result[+A] = Either[ToonError, A]

  type DecodeResult[+A] = Either[DecodeError, A]

  type EncodeResult[+A] = Either[EncodeError, A]

}
