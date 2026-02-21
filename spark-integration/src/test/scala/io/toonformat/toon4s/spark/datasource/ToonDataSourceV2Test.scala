package io.toonformat.toon4s.spark.datasource

import java.nio.file.Files

import scala.util.Using

import munit.FunSuite
import org.apache.spark.sql.SparkSession

class ToonDataSourceV2Test extends FunSuite {

  private val isWindows = System.getProperty("os.name", "").toLowerCase.contains("win")

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
    Option(spark).foreach(_.stop())
  }

  test("format(toon): write and read TOON documents") {
    assume(!isWindows, "TOON datasource write tests need winutils on Windows CI")
    Using.resource(tempDirectory("toon-ds-v2-test")) { tempDir =>
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
        .option("path", tempDir.file.getAbsolutePath)
        .option("key", "users")
        .save()

      val readDf = spark.read
        .format("toon")
        .option("path", tempDir.file.getAbsolutePath)
        .load()

      assertEquals(readDf.schema.fieldNames.toSeq, Seq("toon"))
      val payloads = readDf.collect().map(_.getString(0)).toSeq
      assert(payloads.nonEmpty)
      assert(payloads.forall(_.contains("users")))
      assert(payloads.exists(_.contains("Alice")))
      assert(payloads.exists(_.contains("Bob")))
    }
  }

  test("format(toon): splits partition output with maxRowsPerFile") {
    assume(!isWindows, "TOON datasource write tests need winutils on Windows CI")
    Using.resource(tempDirectory("toon-ds-v2-split-test")) { tempDir =>
      val sparkSession = spark
      import sparkSession.implicits._
      val df = Seq(
        (1, "Alice"),
        (2, "Bob"),
        (3, "Cara"),
      ).toDF("id", "name")

      df.repartition(1)
        .write
        .format("toon")
        .mode("overwrite")
        .option("path", tempDir.file.getAbsolutePath)
        .option("key", "users")
        .option("maxRowsPerFile", "1")
        .save()

      val payloads = spark.read
        .format("toon")
        .option("path", tempDir.file.getAbsolutePath)
        .load()
        .collect()
        .map(_.getString(0))
        .toSeq

      assertEquals(payloads.size, 3)
      assert(payloads.forall(_.contains("users")))
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

  test("format(toon): maxRowsPerFile must be positive") {
    val sparkSession = spark
    import sparkSession.implicits._
    val df = Seq((1, "Alice")).toDF("id", "name")

    Using.resource(tempDirectory("toon-ds-v2-invalid-option")) { tempDir =>
      intercept[IllegalArgumentException] {
        df.write
          .format("toon")
          .mode("overwrite")
          .option("path", tempDir.file.getAbsolutePath)
          .option("maxRowsPerFile", "0")
          .save()
      }
    }
  }

  private def tempDirectory(prefix: String): TempDirectory =
    TempDirectory(Files.createTempDirectory(prefix).toFile)

}

final private case class TempDirectory(file: java.io.File) extends AutoCloseable {

  override def close(): Unit = {
    def deleteRecursively(target: java.io.File): Unit = {
      if (!target.exists()) {
        ()
      } else {
        if (target.isDirectory) {
          Option(target.listFiles()).toSeq.flatten.foreach(deleteRecursively)
        }
        target.delete()
        ()
      }
    }
    deleteRecursively(file)
  }

}
