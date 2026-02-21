package io.toonformat.toon4s.spark.extensions

import io.toonformat.toon4s.{DecodeOptions, EncodeOptions, Toon}
import io.toonformat.toon4s.JsonValue.JString
import io.toonformat.toon4s.spark.ToonMetrics
import org.apache.spark.sql.{SparkSessionExtensions, SparkSessionExtensionsProvider}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, Literal, ScalaUDF}
import org.apache.spark.sql.types.{IntegerType, StringType}

/**
 * Spark SQL extensions provider that auto-registers TOON SQL functions.
 *
 * Usage: spark.sql.extensions=io.toonformat.toon4s.spark.extensions.ToonSparkSessionExtensions
 */
class ToonSparkSessionExtensions extends SparkSessionExtensionsProvider {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    ToonSparkSessionExtensions.functions.foreach(extensions.injectFunction)
  }

}

private object ToonSparkSessionExtensions {

  private type FunctionDescription =
    (FunctionIdentifier, ExpressionInfo, Seq[Expression] => Expression)

  private val functions: Seq[FunctionDescription] = Seq(
    unaryStringFunction("toon_encode_row")(value => encodeToToonString(value)),
    unaryStringFunction("toon_decode_row")(value => decodeToString(value)),
    unaryStringFunction("toon_encode_string")(value => encodeToToonString(value)),
    unaryStringFunction("toon_decode_string")(value => decodeToString(value)),
    unaryIntFunction("toon_estimate_tokens") { value =>
      val text = Option(value).map(_.toString).getOrElse("")
      if (text.isEmpty) 0 else ToonMetrics.estimateTokens(text)
    },
  )

  private def unaryStringFunction(name: String)(
      fn: Any => String
  ): FunctionDescription =
    unaryFunction(name, StringType, nullable = false, defaultValue = "")(fn)

  private def unaryIntFunction(name: String)(
      fn: Any => Int
  ): FunctionDescription =
    unaryFunction(name, IntegerType, nullable = false, defaultValue = 0)(fn)

  private def unaryFunction[A](
      name: String,
      returnType: org.apache.spark.sql.types.DataType,
      nullable: Boolean,
      defaultValue: A,
  )(fn: Any => A): FunctionDescription = {
    (
      FunctionIdentifier(name),
      new ExpressionInfo(classOf[ToonSparkSessionExtensions].getName, name),
      { children: Seq[Expression] =>
        children.headOption match {
        case Some(input) =>
          val function: AnyRef = (value: Any) => fn(value)
          ScalaUDF(
            function = function,
            dataType = returnType,
            children = Seq(input),
            inputEncoders = Seq(None),
            outputEncoder = None,
            udfName = Some(name),
            nullable = nullable,
          )
        case None =>
          Literal(defaultValue, returnType)
        }
      },
    )
  }

  private def decodeToString(value: Any): String = {
    Option(value)
      .map(_.toString)
      .flatMap(text =>
        Toon.decode(text, DecodeOptions()).toOption.map {
          case JString(s) => s
          case json       => json.toString
        }
      )
      .getOrElse("")
  }

  private def encodeToToonString(value: Any): String =
    Option(value)
      .map(v => Toon.encode(JString(v.toString), EncodeOptions()).getOrElse(s"\"$v\""))
      .getOrElse("null")

}
