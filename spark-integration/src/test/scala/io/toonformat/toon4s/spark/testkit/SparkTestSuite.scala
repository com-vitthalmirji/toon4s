package io.toonformat.toon4s.spark.testkit

import munit.FunSuite
import org.apache.spark.sql.SparkSession

trait SparkTestSuite extends FunSuite {

  private var sparkSession: SparkSession = _

  protected def sparkMaster: String = "local[1]"

  protected def sparkAppName: String = getClass.getSimpleName.stripSuffix("$")

  protected def sparkConfig: Map[String, String] = Map(
    "spark.ui.enabled" -> "false",
    "spark.sql.shuffle.partitions" -> "1",
  )

  final protected def spark: SparkSession = sparkSession

  implicit final protected def sparkImplicit: SparkSession = sparkSession

  override def beforeAll(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    val builder = SparkSession.builder().master(sparkMaster).appName(sparkAppName)
    sparkConfig.foreach { case (key, value) => builder.config(key, value) }
    sparkSession = builder.getOrCreate()
  }

  override def afterAll(): Unit = {
    Option(sparkSession).foreach(_.stop())
    sparkSession = null
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

}
