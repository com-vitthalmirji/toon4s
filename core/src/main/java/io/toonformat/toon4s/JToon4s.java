package io.toonformat.toon4s;

import java.io.Reader;
import java.io.Writer;

import scala.util.Either;

/**
 * Java-friendly façade for toon4s.
 * Exposes stdlib-only signatures so Java callers avoid Scala collections in APIs.
 */
public final class JToon4s {
  private JToon4s() {}

  public static Either<io.toonformat.toon4s.error.EncodeError, String> encode(Object value, EncodeOptions options) {
    return Toon.encode(value, options);
  }

  public static Either<io.toonformat.toon4s.error.EncodeError, Void> encodeTo(Object value, Writer out, EncodeOptions options) {
    return Toon.encodeTo(value, out, options).map(u -> null);
  }

  public static Either<io.toonformat.toon4s.error.DecodeError, JsonValue> decode(String toon, DecodeOptions options) {
    return Toon.decode(toon, options);
  }

  public static Either<io.toonformat.toon4s.error.DecodeError, JsonValue> decodeFrom(Reader in, DecodeOptions options) {
    return Toon.decodeFrom(in, options);
  }
}
