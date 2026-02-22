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

}
