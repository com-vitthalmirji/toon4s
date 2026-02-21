package io.toonformat.toon4s.spark.datasource

import java.nio.file.Files

import munit.FunSuite
import org.apache.spark.sql.SparkSession

class ToonDataSourceV2Test extends FunSuite {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("ToonDataSourceV2Test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("format(toon): write and read TOON documents") {
    val tempDir = Files.createTempDirectory("toon-ds-v2-test").toFile
    try {
      val sparkSession = spark
      import sparkSession.implicits._
      val df = Seq(
        (1, "Alice", 25),
        (2, "Bob", 30),
        (3, "Cara", 35),
      ).toDF("id", "name", "age")

      df.repartition(2)
        .write
        .format("toon")
        .mode("overwrite")
        .option("path", tempDir.getAbsolutePath)
        .option("key", "users")
        .save()

      val readDf = spark.read
        .format("toon")
        .option("path", tempDir.getAbsolutePath)
        .load()

      assertEquals(readDf.schema.fieldNames.toSeq, Seq("toon"))
      val payloads = readDf.collect().map(_.getString(0)).toSeq
      assert(payloads.nonEmpty)
      assert(payloads.forall(_.contains("users")))
      assert(payloads.exists(_.contains("Alice")))
      assert(payloads.exists(_.contains("Bob")))
    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("format(toon): requires path option") {
    val sparkSession = spark
    import sparkSession.implicits._
    val df = Seq((1, "Alice")).toDF("id", "name")

    intercept[IllegalArgumentException] {
      df.write
        .format("toon")
        .mode("overwrite")
        .save()
    }
  }

  private def deleteRecursively(file: java.io.File): Unit = {
    if (!file.exists()) return
    if (file.isDirectory) {
      val children = file.listFiles()
      if (children != null) {
        children.foreach(deleteRecursively)
      }
    }
    file.delete()
    ()
  }
}
