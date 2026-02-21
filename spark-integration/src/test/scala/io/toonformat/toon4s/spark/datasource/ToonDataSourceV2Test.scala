package io.toonformat.toon4s.spark.datasource

import java.nio.file.Files

import scala.util.Using

import io.toonformat.toon4s.spark.testkit.SparkTestSuite
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class ToonDataSourceV2Test extends SparkTestSuite {

  private val isWindows = System.getProperty("os.name", "").toLowerCase.contains("win")

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
      val payloads = readDf.take(100).map(_.getString(0)).toSeq
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
        .take(10)
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

  test("format(toon): commit fails when rename cannot replace existing file") {
    assume(!isWindows, "TOON datasource write tests need winutils on Windows CI")
    Using.resource(tempDirectory("toon-ds-v2-commit-failure")) { tempDir =>
      val outputDir = tempDir.file.toPath.resolve("output").toFile
      assert(outputDir.mkdirs())

      val queryId = "query-commit-failure"
      val tempQueryDir = outputDir.toPath.resolve("_temporary").resolve(queryId)
      Files.createDirectories(tempQueryDir)

      val partFileName = "part-0-0-0.toon"
      val tempPart = tempQueryDir.resolve(partFileName)
      val finalPart = outputDir.toPath.resolve(partFileName)

      Files.writeString(tempPart, """{"users":[]}""")
      Files.writeString(finalPart, "existing-destination")

      val conf = new Configuration(spark.sparkContext.hadoopConfiguration)
      conf.set("fs.defaultFS", "failrename:///")
      conf.set("fs.failrename.impl", classOf[FailRenameLocalFileSystem].getName)

      val writer = new ToonBatchWrite(
        queryId = queryId,
        outputPath = s"failrename://${outputDir.getAbsolutePath}",
        key = "users",
        maxRowsPerFile = 10,
        schema = StructType(Seq(StructField("id", IntegerType))),
        conf = conf,
      )

      val ex = intercept[IllegalArgumentException] {
        writer.commit(Array.empty)
      }
      assert(Option(ex.getMessage).exists(_.nonEmpty))
    }
  }

  private def tempDirectory(prefix: String): TempDirectory =
    TempDirectory(Files.createTempDirectory(prefix).toFile)

}

final private[datasource] class FailRenameLocalFileSystem extends LocalFileSystem {

  override def getScheme: String = "failrename"

  override def rename(src: Path, dst: Path): Boolean = false

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
