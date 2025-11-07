package io.toonformat.toon4s

final case class EncodeOptions(
    indent: Int = 2,
    delimiter: Delimiter = Delimiter.Comma,
    lengthMarker: Boolean = false
)
