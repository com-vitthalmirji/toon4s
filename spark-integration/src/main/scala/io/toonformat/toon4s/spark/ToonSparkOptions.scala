package io.toonformat.toon4s.spark

import io.toonformat.toon4s.EncodeOptions

/**
 * Stable options model for Spark TOON encoding and metrics APIs.
 *
 * Keep this surface small and backward compatible across patch releases.
 */
final case class ToonSparkOptions(
    key: String = "data",
    maxRowsPerChunk: Int = 1000,
    encodeOptions: EncodeOptions = EncodeOptions(),
)

object ToonSparkOptions {

  val default: ToonSparkOptions = ToonSparkOptions()

}
