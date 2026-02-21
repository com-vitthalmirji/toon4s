package io.toonformat.toon4s.spark.extensions

import io.toonformat.toon4s.{DecodeOptions, EncodeOptions, Toon}
import io.toonformat.toon4s.JsonValue.JString
import io.toonformat.toon4s.spark.ToonMetrics
import org.apache.spark.sql.{SparkSessionExtensions, SparkSessionExtensionsProvider}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, ScalaUDF}
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
    unaryStringFunction("toon_encode_row")(value => if (value == null) null else value.toString),
    unaryStringFunction("toon_decode_row")(value => decodeToString(value)),
    unaryStringFunction("toon_encode_string") { value =>
      if (value == null) "null"
      else Toon.encode(JString(value.toString), EncodeOptions()).getOrElse(s"\"$value\"")
    },
    unaryStringFunction("toon_decode_string")(value => decodeToString(value)),
    unaryIntFunction("toon_estimate_tokens") { value =>
      val text = Option(value).map(_.toString).getOrElse("")
      if (text.isEmpty) 0 else ToonMetrics.estimateTokens(text)
    },
  )

  private def unaryStringFunction(name: String)(
      fn: Any => String
  ): FunctionDescription =
    unaryFunction(name, StringType, nullable = true)(fn)

  private def unaryIntFunction(name: String)(
      fn: Any => Int
  ): FunctionDescription =
    unaryFunction(name, IntegerType, nullable = false)(fn)

  private def unaryFunction[A](
      name: String,
      returnType: org.apache.spark.sql.types.DataType,
      nullable: Boolean,
  )(fn: Any => A): FunctionDescription = {
    (
      FunctionIdentifier(name),
      new ExpressionInfo(classOf[ToonSparkSessionExtensions].getName, name),
      { children: Seq[Expression] =>
        if (children.lengthCompare(1) != 0) {
          throw new IllegalArgumentException(s"$name expects exactly one argument")
        }
        val function: AnyRef = ((value: Any) => fn(value)).asInstanceOf[AnyRef]
        ScalaUDF(
          function = function,
          dataType = returnType,
          children = children,
          inputEncoders = Seq(None),
          outputEncoder = None,
          udfName = Some(name),
          nullable = nullable,
        )
      },
    )
  }

  private def decodeToString(value: Any): String = {
    val input = Option(value).map(_.toString).orNull
    if (input == null) {
      null
    } else {
      Toon.decode(input, DecodeOptions()).fold(
        _ => null,
        {
          case JString(s) => s
          case json       => json.toString
        },
      )
    }
  }

}
