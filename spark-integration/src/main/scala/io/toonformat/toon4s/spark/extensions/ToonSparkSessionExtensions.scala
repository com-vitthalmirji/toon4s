package io.toonformat.toon4s.spark.extensions

import io.toonformat.toon4s.spark.ToonUDFs
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions, SparkSessionExtensionsProvider}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Spark SQL extensions provider that auto-registers TOON SQL UDFs.
 *
 * Usage:
 *   spark.sql.extensions=io.toonformat.toon4s.spark.extensions.ToonSparkSessionExtensions
 */
class ToonSparkSessionExtensions extends SparkSessionExtensionsProvider {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser {
      (session: SparkSession, parser: ParserInterface) =>
        new ParserInterface {
          private def ensureUdfsRegistered(): Unit = ToonSparkSessionExtensions.ensureRegistered(session)

          override def parsePlan(sqlText: String): LogicalPlan = {
            ensureUdfsRegistered()
            parser.parsePlan(sqlText)
          }

          override def parseExpression(sqlText: String): Expression =
            parser.parseExpression(sqlText)

          override def parseTableIdentifier(sqlText: String): TableIdentifier =
            parser.parseTableIdentifier(sqlText)

          override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
            parser.parseFunctionIdentifier(sqlText)

          override def parseMultipartIdentifier(sqlText: String): Seq[String] =
            parser.parseMultipartIdentifier(sqlText)

          override def parseQuery(sqlText: String): LogicalPlan = {
            ensureUdfsRegistered()
            parser.parseQuery(sqlText)
          }

          override def parseTableSchema(sqlText: String): StructType =
            parser.parseTableSchema(sqlText)

          override def parseDataType(sqlText: String): DataType =
            parser.parseDataType(sqlText)
        }
    }
  }
}

private object ToonSparkSessionExtensions {
  private val registeredSessions = scala.collection.mutable.Set.empty[Int]

  def ensureRegistered(session: SparkSession): Unit = synchronized {
    val sessionId = System.identityHashCode(session.sessionState)
    if (!registeredSessions.contains(sessionId)) {
      ToonUDFs.register(session)
      registeredSessions += sessionId
    }
  }
}
