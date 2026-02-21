package io.toonformat.toon4s.spark

import munit.FunSuite
import org.apache.spark.sql.SparkSession

class SparkDatasetOpsTest extends FunSuite {

  private var sparkInstance: SparkSession = _

  override def beforeAll(): Unit = {
    sparkInstance = SparkSession
      .builder()
      .master("local[*]")
      .appName("toon4s-spark-dataset-test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    Option(sparkInstance).foreach(_.stop())
  }

  test("Dataset[T] toToon encodes runtime data") {
    val spark = sparkInstance
    import spark.implicits._
    import io.toonformat.toon4s.spark.SparkDatasetOps._

    val dataset = Seq(
      UserRecord(1, "Alice", 25),
      UserRecord(2, "Bob", 30),
    ).toDS()

    val encodedResult = dataset.toToon(key = "users")

    assert(encodedResult.isRight)
    encodedResult.foreach { chunks =>
      assertEquals(chunks.size, 1)
      assert(chunks.head.contains("users"))
      assert(chunks.head.contains("Alice"))
    }
  }

  test("Dataset[T] toonMetrics computes runtime metrics") {
    val spark = sparkInstance
    import spark.implicits._
    import io.toonformat.toon4s.spark.SparkDatasetOps._

    val dataset = Seq(
      UserRecord(1, "Alice", 25),
      UserRecord(2, "Bob", 30),
      UserRecord(3, "Charlie", 35),
    ).toDS()

    val metricsResult = dataset.toonMetrics(key = "users")

    assert(metricsResult.isRight)
    metricsResult.foreach { metrics =>
      assertEquals(metrics.rowCount, 3)
      assertEquals(metrics.columnCount, 3)
      assert(metrics.jsonTokenCount > 0)
    }
  }

  test("Dataset[T] toonMetrics supports caller chunk size") {
    val spark = sparkInstance
    import spark.implicits._
    import io.toonformat.toon4s.spark.SparkDatasetOps._
    import io.toonformat.toon4s.EncodeOptions

    val dataset = (1 to 20).map(i => UserRecord(i, s"user$i", 20 + i)).toDS()
    val metricsResult = dataset.toonMetrics("users", maxRowsPerChunk = 4, EncodeOptions())

    assert(metricsResult.isRight)
    metricsResult.foreach { metrics =>
      assertEquals(metrics.rowCount, 20)
      assertEquals(metrics.columnCount, 3)
    }
  }

  test("Dataset[T] stable options API works") {
    val spark = sparkInstance
    import spark.implicits._
    import io.toonformat.toon4s.spark.SparkDatasetOps._
    import io.toonformat.toon4s.EncodeOptions

    val dataset = (1 to 9).map(i => UserRecord(i, s"user$i", 20 + i)).toDS()
    val options = ToonSparkOptions(
      key = "users",
      maxRowsPerChunk = 3,
      encodeOptions = EncodeOptions(),
    )

    val encodeResult = dataset.toToon(options)
    val metricsResult = dataset.toonMetrics(options)

    assert(encodeResult.isRight)
    encodeResult.foreach(chunks => assertEquals(chunks.size, 3))

    assert(metricsResult.isRight)
    metricsResult.foreach { metrics =>
      assertEquals(metrics.rowCount, 9)
      assertEquals(metrics.columnCount, 3)
    }
  }

}

final case class UserRecord(id: Int, name: String, age: Int)
