package io.toonformat.toon4s.spark.datasource

import java.net.URI
import java.nio.file.{Files, Path => JPath}

import scala.jdk.CollectionConverters._
import scala.util.Using

import io.toonformat.toon4s.spark.testkit.SparkTestSuite
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, RawLocalFileSystem}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

// The failure side of format("toon") writes: when the job commit fails, the write fails and no
// partial output or staging files are left behind; an aborted job or task removes only its own files.
class ToonDataSourceV2AbortTest extends SparkTestSuite {

  private val isWindows = System.getProperty("os.name", "").toLowerCase.contains("win")

  private val schema = StructType(Seq(StructField("id", IntegerType)))

  // data files only: the local file system also writes a .crc checksum beside each one
  private def filesUnder(dir: JPath): List[JPath] =
    if (!Files.exists(dir)) Nil
    else
      Using.resource(Files.walk(dir))(
        _.iterator().asScala.filter(p =>
          Files.isRegularFile(p) && p.getFileName.toString.endsWith(".toon")
        ).toList
      )

  private def withTempDir[A](prefix: String)(f: JPath => A): A =
    Using.resource(TempDirectory(Files.createTempDirectory(prefix).toFile))(d => f(d.file.toPath))

  test("format(toon): a failed job commit fails the write and leaves no part or staging files") {
    assume(!isWindows, "TOON datasource write tests need winutils on Windows CI")
    withTempDir("toon-ds-v2-abort") { dir =>
      val output = dir.resolve("output")
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      // every rename fails, so the job commit cannot move the task output into place
      hadoopConf.set("fs.failrename.impl", classOf[FailRenameRawFileSystem].getName)
      hadoopConf.setBoolean("fs.failrename.impl.disable.cache", true)
      try {
        val sparkSession = spark
        import sparkSession.implicits._
        val failure = intercept[Exception] {
          Seq(1, 2, 3, 4).toDF("id")
            .repartition(2)
            .write
            .format("toon")
            .mode("append")
            .option("path", s"failrename://${output.toAbsolutePath}")
            .option("key", "rows")
            .save()
        }
        assert(
          Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null)
            .exists(e => Option(e.getMessage).exists(_.contains("Failed to rename"))),
          Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null).map(e =>
            s"${e.getClass.getName}: ${e.getMessage}"
          ).mkString("\n"),
        )
        assertEquals(
          filesUnder(output),
          Nil,
          "no part file reached the output, and staging was removed",
        )
      } finally {
        hadoopConf.unset("fs.failrename.impl")
        hadoopConf.unset("fs.failrename.impl.disable.cache")
      }
    }
  }

  private def localConf: Configuration = new Configuration(spark.sparkContext.hadoopConfiguration)

  private def batchWrite(output: JPath, queryId: String): ToonBatchWrite =
    new ToonBatchWrite(queryId, output.toUri.toString, "rows", 1, schema, localConf)

  private def factory(output: JPath, queryId: String): ToonDataWriterFactory =
    new ToonDataWriterFactory(
      output.toUri.toString,
      queryId,
      "rows",
      1,
      schema,
      new SerializableConfiguration(localConf),
    )

  test("job abort removes the query's staging files and leaves other queries' alone") {
    assume(!isWindows, "TOON datasource write tests need winutils on Windows CI")
    withTempDir("toon-ds-v2-job-abort") { output =>
      Seq("aborted", "other").foreach { queryId =>
        val writer = factory(output, queryId).createWriter(0, 0L)
        (1 to 3).foreach(i => writer.write(InternalRow(i)))
        writer.commit()
        writer.close()
      }
      assertEquals(filesUnder(output.resolve("_temporary").resolve("aborted")).size, 3)

      batchWrite(output, "aborted").abort(Array.empty)

      assertEquals(filesUnder(output.resolve("_temporary").resolve("aborted")), Nil)
      assertEquals(filesUnder(output.resolve("_temporary").resolve("other")).size, 3)
      assertEquals(
        filesUnder(output).count(_.getParent == output),
        0,
        "nothing was moved into the output",
      )
    }
  }

  test("task abort deletes the files that task wrote and no others") {
    assume(!isWindows, "TOON datasource write tests need winutils on Windows CI")
    withTempDir("toon-ds-v2-task-abort") { output =>
      val writers = factory(output, "query")
      val kept = writers.createWriter(0, 0L)
      val aborted = writers.createWriter(1, 1L)
      (1 to 2).foreach(i => kept.write(InternalRow(i)))
      (1 to 2).foreach(i => aborted.write(InternalRow(i)))
      kept.commit()

      aborted.abort()

      val staged =
        filesUnder(output.resolve("_temporary").resolve("query")).map(_.getFileName.toString)
      assertEquals(staged.size, 2)
      assert(staged.forall(_.contains("-0-0-")), staged)
    }
  }

  test("abort is safe when nothing was staged") {
    assume(!isWindows, "TOON datasource write tests need winutils on Windows CI")
    withTempDir("toon-ds-v2-empty-abort") { output =>
      batchWrite(output, "never-ran").abort(Array.empty)
      assertEquals(filesUnder(output), Nil)
    }
  }

}

// The local disk under its own scheme, with every rename failing: tasks can stage their files, but the
// job commit cannot move them into place. Registered as the file system for "failrename://" paths, and
// self-consistent about its scheme, so Spark's tasks and the driver both accept those paths.
final private[datasource] class FailRenameRawFileSystem extends RawLocalFileSystem {

  override def getUri: URI = URI.create("failrename:///")

  override def getScheme: String = "failrename"

  override def rename(src: Path, dst: Path): Boolean = false

}
