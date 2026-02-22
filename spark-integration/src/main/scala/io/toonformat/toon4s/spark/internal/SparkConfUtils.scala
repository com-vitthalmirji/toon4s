package io.toonformat.toon4s.spark.internal

import org.apache.spark.sql.SparkSession

private[spark] object SparkConfUtils {

  def readPositiveInt(
      spark: SparkSession,
      key: String,
      defaultValue: Int,
  ): Int =
    spark.conf
      .getOption(key)
      .flatMap(_.trim.toIntOption)
      .filter(_ > 0)
      .getOrElse(defaultValue)

  def readPositiveLong(
      spark: SparkSession,
      key: String,
      defaultValue: Long,
  ): Long =
    spark.conf
      .getOption(key)
      .flatMap(_.trim.toLongOption)
      .filter(_ > 0L)
      .getOrElse(defaultValue)

  /** Compute UTF-8 byte length without allocating a byte array. */
  def utf8ByteLength(s: String): Long = {
    var bytes = 0L
    var i = 0
    while (i < s.length) {
      val c = s.charAt(i)
      if (c <= 0x7F) bytes += 1L
      else if (c <= 0x7FF) bytes += 2L
      else if (Character.isHighSurrogate(c)) {
        bytes += 4L
        i += 1
      } else bytes += 3L
      i += 1
    }
    bytes
  }

}
