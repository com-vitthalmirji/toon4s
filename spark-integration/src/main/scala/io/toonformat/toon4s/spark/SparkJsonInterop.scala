package io.toonformat.toon4s.spark

import scala.collection.immutable.VectorMap
import scala.jdk.CollectionConverters._
import scala.util.Try

import io.toonformat.toon4s.JsonValue
import io.toonformat.toon4s.JsonValue._
import io.toonformat.toon4s.spark.error.SparkToonError
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * Pure functional conversions between Spark Row and toon4s JsonValue.
 *
 * ==Design principles==
 *   - All functions are pure (referentially transparent, no side effects)
 *   - Exhaustive pattern matching on Spark DataTypes
 *   - Defensive null handling throughout
 *   - Immutable data structures (Vector, VectorMap)
 *   - Type-safe conversions with explicit error handling
 *
 * ==Usage==
 * {{{
 * import io.toonformat.toon4s.spark.SparkJsonInterop
 *
 * val row: Row = df.collect().head
 * val schema: StructType = df.schema
 *
 * // Convert Row to JsonValue
 * val json: JsonValue = SparkJsonInterop.rowToJsonValue(row, schema)
 *
 * // Convert back to Row
 * val reconstructed: Row = SparkJsonInterop.jsonValueToRow(json, schema)
 * }}}
 */
object SparkJsonInterop {

  /**
   * Convert Spark Row to JsonValue object.
   *
   * Pure function that transforms a Spark Row into a JObj with field names from schema. Handles
   * null values explicitly as JNull.
   *
   * @param row
   *   Spark Row with values
   * @param schema
   *   StructType defining field types and names
   * @return
   *   JsonValue representing the row as JObj
   *
   * @example
   *   {{{
   * val df = Seq((1, "Alice", 25)).toDF("id", "name", "age")
   * val row = df.collect().head
   * val json = rowToJsonValue(row, df.schema)
   * // json: JObj(VectorMap("id" -> JNumber(1), "name" -> JString("Alice"), "age" -> JNumber(25)))
   *   }}}
   */
  def rowToJsonValue(row: Row, schema: StructType): JsonValue = {
    val fields = schema.fields.zipWithIndex.map {
      case (field, idx) =>
        val value =
          if (row.isNullAt(idx)) JNull
          else fieldToJsonValue(row.get(idx), field.dataType)

        field.name -> value
    }
    JObj(VectorMap.from(fields))
  }

  /**
   * Convert Spark field value to JsonValue.
   *
   * Uses exhaustive pattern matching on DataType for type-safe conversion. Handles nested
   * structures recursively. Unsupported types (e.g., CalendarIntervalType, UserDefinedType) are
   * converted to string representations using toString as a best-effort fallback.
   *
   * @param value
   *   The value from Spark Row
   * @param dataType
   *   Spark DataType for the field
   * @return
   *   Corresponding JsonValue
   */
  private def fieldToJsonValue(value: Any, dataType: DataType): JsonValue = dataType match {
  // ========== Primitive Types ==========

  case StringType =>
    JString(value.asInstanceOf[String])

  case IntegerType =>
    JNumber(BigDecimal(value.asInstanceOf[Int]))

  case LongType =>
    JNumber(BigDecimal(value.asInstanceOf[Long]))

  case DoubleType =>
    val d = value.asInstanceOf[Double]
    if (d.isNaN || d.isInfinity) JNull // NaN and Infinity not supported in JSON
    else JNumber(BigDecimal(d))

  case FloatType =>
    val f = value.asInstanceOf[Float]
    if (f.isNaN || f.isInfinity) JNull
    else JNumber(BigDecimal(f.toDouble))

  case BooleanType =>
    JBool(value.asInstanceOf[Boolean])

  case ByteType =>
    JNumber(BigDecimal(value.asInstanceOf[Byte].toInt))

  case ShortType =>
    JNumber(BigDecimal(value.asInstanceOf[Short].toInt))

  case DecimalType() =>
    JNumber(BigDecimal(value.asInstanceOf[java.math.BigDecimal]))

  // ========== Complex Types (Recursive) ==========

  case ArrayType(elementType, _) =>
    val seq: scala.collection.Seq[_] = value match {
    case s: scala.collection.Seq[_] => s
    case a: Array[_]                => a.toSeq
    case l: java.util.List[_]       => l.asScala.toSeq
    case other                      =>
      throw new IllegalArgumentException(
        s"Unsupported array value type: ${other.getClass.getName}"
      )
    }
    val jsonValues = seq.map { elem =>
      if (elem == null) JNull
      else fieldToJsonValue(elem, elementType)
    }.toVector
    JArray(jsonValues)

  case structType: StructType =>
    val row = value.asInstanceOf[Row]
    rowToJsonValue(row, structType)

  case MapType(keyType, valueType, _) =>
    val map = value.asInstanceOf[Map[_, _]]
    val pairs: Map[String, JsonValue] = map.map {
      case (k, v) =>
        val keyStr = k.toString // Keys must be strings in JSON
        val jsonValue =
          if (v == null) JNull
          else fieldToJsonValue(v, valueType)
        keyStr -> jsonValue
    }
    JObj(VectorMap.from(pairs))

  // ========== Temporal Types ==========

  case DateType =>
    JString(value.asInstanceOf[java.sql.Date].toString)

  case TimestampType =>
    // Use ISO-8601 format for timestamps
    val timestamp = value.asInstanceOf[java.sql.Timestamp]
    JString(timestamp.toInstant.toString)

  // ========== Binary Types ==========

  case BinaryType =>
    val bytes = value.asInstanceOf[Array[Byte]]
    JString(java.util.Base64.getEncoder.encodeToString(bytes))

  // ========== Null Type ==========

  case NullType =>
    JNull

  // ========== Unsupported Types ==========

  case other =>
    // Fallback: convert to string representation
    JString(value.toString)
  }

  /**
   * Convert JsonValue to Spark-compatible value.
   *
   * Used for round-trip conversion (TOON -> DataFrame). Returns Scala types that Spark can handle.
   *
   * @param json
   *   JsonValue to convert
   * @return
   *   Spark-compatible value (primitives, Seq, Map)
   *
   * @example
   *   {{{
   * val json = JObj(VectorMap("name" -> JString("Alice"), "age" -> JNumber(25)))
   * val value = jsonValueToValue(json)
   * // value: Map("name" -> "Alice", "age" -> 25.0)
   *   }}}
   */
  def jsonValueToValue(json: JsonValue): Any = json match {
  case JString(s)     => s
  case JNumber(n)     => n.toDouble // Spark uses Double for numeric types
  case JBool(b)       => b
  case JNull          => null
  case JArray(values) =>
    values.map(jsonValueToValue).toSeq // Spark expects Seq, not Vector
  case JObj(fields) =>
    fields.map { case (k, v) => k -> jsonValueToValue(v) }.toMap
  }

  /**
   * Convert JsonValue to Spark Row with schema validation.
   *
   * Validates that the JsonValue is a JObj and reconstructs a Row matching the expected schema.
   * Missing fields are filled with null.
   *
   * @param json
   *   JsonValue (must be JObj)
   * @param schema
   *   Expected StructType
   * @return
   *   Spark Row matching schema
   *
   * @throws java.lang.IllegalArgumentException
   *   if json is not JObj
   *
   * @example
   *   {{{
   * val json = JObj(VectorMap("id" -> JNumber(1), "name" -> JString("Alice")))
   * val schema = StructType(Seq(
   *   StructField("id", IntegerType),
   *   StructField("name", StringType)
   * ))
   * val row = jsonValueToRow(json, schema)
   * // row: Row(1, "Alice")
   *   }}}
   */
  def jsonValueToRow(json: JsonValue, schema: StructType): Row = json match {
  case JObj(fields) =>
    val values = schema.fields.map { field =>
      fields.get(field.name) match {
      case Some(value) => jsonValueToTypedValue(value, field.dataType)
      case None        => null // Missing field defaults to null
      }
    }
    Row.fromSeq(values.toSeq)

  case _ =>
    throw new IllegalArgumentException(
      s"Expected JObj for row conversion, got ${json.getClass.getSimpleName}"
    )
  }

  /**
   * Convert JsonValue to typed value matching Spark DataType.
   *
   * Performs type coercion to match the expected Spark DataType. Handles nested structures
   * recursively.
   *
   * @param json
   *   JsonValue to convert
   * @param dataType
   *   Target Spark DataType
   * @return
   *   Value of appropriate type for Spark
   */
  private def jsonValueToTypedValue(json: JsonValue, dataType: DataType): Any = {
    (json, dataType) match {
    // ========== Null Handling ==========
    case (JNull, _) => null

    // ========== Primitive Conversions ==========
    case (JString(s), StringType)    => s
    case (JNumber(n), IntegerType)   => n.toInt
    case (JNumber(n), LongType)      => n.toLong
    case (JNumber(n), DoubleType)    => n.toDouble
    case (JNumber(n), FloatType)     => n.toFloat
    case (JNumber(n), ByteType)      => n.toByte
    case (JNumber(n), ShortType)     => n.toShort
    case (JNumber(n), DecimalType()) => n.underlying()
    case (JBool(b), BooleanType)     => b

    // ========== Complex Type Conversions ==========
    case (JArray(values), ArrayType(elemType, _)) =>
      values.map(v => jsonValueToTypedValue(v, elemType)).toSeq

    case (obj: JObj, structType: StructType) =>
      jsonValueToRow(obj, structType)

    case (JObj(fields), MapType(keyType, valueType, _)) =>
      fields.map {
        case (k, v) =>
          k -> jsonValueToTypedValue(v, valueType)
      }.toMap

    // ========== Temporal Type Conversions ==========
    case (JString(s), DateType) =>
      java.sql.Date.valueOf(s)

    case (JString(s), TimestampType) =>
      java.sql.Timestamp.valueOf(s)

    // ========== Binary Conversions ==========
    case (JString(s), BinaryType) =>
      java.util.Base64.getDecoder.decode(s)

    // ========== Type Coercion ==========
    case (JString(s), IntegerType) =>
      Try(s.toInt).getOrElse(0)

    case (JString(s), LongType) =>
      Try(s.toLong).getOrElse(0L)

    case (JString(s), DoubleType) =>
      Try(s.toDouble).getOrElse(0.0)

    case (JString(s), BooleanType) =>
      s.toLowerCase match {
      case "true" | "1" | "yes" => true
      case _                    => false
      }

    // ========== Fallback ==========
    case (JString(s), _) => s
    case (JNumber(n), _) => n.toDouble
    case (JBool(b), _)   => b
    case _               => null
    }
  }

  /**
   * Safe conversion with Either error handling.
   *
   * Wraps rowToJsonValue with Try for explicit error handling.
   *
   * @param row
   *   Spark Row
   * @param schema
   *   StructType
   * @return
   *   Either error or JsonValue
   */
  def rowToJsonValueSafe(row: Row, schema: StructType): Either[SparkToonError, JsonValue] = {
    Try(rowToJsonValue(row, schema)).toEither.left.map { ex =>
      SparkToonError.ConversionError(
        s"Failed to convert Row to JsonValue: ${ex.getMessage}",
        Some(ex),
      )
    }
  }

  /**
   * Safe row reconstruction with Either error handling.
   *
   * @param json
   *   JsonValue
   * @param schema
   *   StructType
   * @return
   *   Either error or Row
   */
  def jsonValueToRowSafe(json: JsonValue, schema: StructType): Either[SparkToonError, Row] = {
    Try(jsonValueToRow(json, schema)).toEither.left.map { ex =>
      SparkToonError.ConversionError(
        s"Failed to convert JsonValue to Row: ${ex.getMessage}",
        Some(ex),
      )
    }
  }

}
