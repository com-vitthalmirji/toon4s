package io.toonformat.toon4s.spark

import io.toonformat.toon4s.spark.testkit.SparkTestSuite

class SparkDatasetOpsTest extends SparkTestSuite {

  override protected def sparkAppName: String = "toon4s-spark-dataset-test"

  override protected def sparkConfig: Map[String, String] =
    super.sparkConfig + ("spark.sql.warehouse.dir" -> "target/spark-warehouse")

  test("Dataset[T] toToon encodes runtime data") {
    val session = spark
    import session.implicits._
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
    val session = spark
    import session.implicits._
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
    val session = spark
    import session.implicits._
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
    val session = spark
    import session.implicits._
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
