package io.toonformat.toon4s.spark

import scala.jdk.CollectionConverters._

import io.toonformat.toon4s.spark.SparkToonOps._
import munit.FunSuite
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

class SparkToonOpsTest extends FunSuite {

  implicit private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkToonOpsTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("toToon: encode simple DataFrame") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("age", IntegerType),
    ))

    val data = Seq(
      Row(1, "Alice", 25),
      Row(2, "Bob", 30),
    )

    val df = spark.createDataFrame(data.asJava, schema)
    val result = df.toToon(key = "users", maxRowsPerChunk = 100)

    assert(result.isRight)
    result.foreach { chunks =>
      assertEquals(chunks.size, 1)
      assert(chunks.head.contains("users"))
      assert(chunks.head.contains("Alice"))
      assert(chunks.head.contains("Bob"))
    }
  }

  test("toToon: handle empty DataFrame") {
    val df = spark.emptyDataFrame

    val result = df.toToon()

    assert(result.isRight)
    result.foreach(chunks => assertEquals(chunks.size, 1))
  }

  test("toToon: chunk large DataFrame") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
    ))

    val data = (1 to 250).map(i => Row(i, s"user$i"))
    val df = spark.createDataFrame(data.asJava, schema)

    val result = df.toToon(maxRowsPerChunk = 100)

    assert(result.isRight)
    result.foreach { chunks =>
      assertEquals(chunks.size, 3) // 100 + 100 + 50
    }
  }

  test("toToon: reject non-positive chunk size") {
    val schema = StructType(Seq(
      StructField("id", IntegerType)
    ))
    val df = spark.createDataFrame(Seq(Row(1)).asJava, schema)

    val result = df.toToon(maxRowsPerChunk = 0)
    assert(result.isLeft)
  }

  test("toonMetrics: compute token metrics") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
    ))

    val data = Seq(
      Row(1, "Alice"),
      Row(2, "Bob"),
    )

    val df = spark.createDataFrame(data.asJava, schema)
    val result = df.toonMetrics(key = "data")

    assert(result.isRight)
    result.foreach { metrics =>
      assert(metrics.jsonTokenCount > 0)
      assert(metrics.toonTokenCount > 0)
      assert(metrics.rowCount == 2)
      assert(metrics.columnCount == 2)
    }
  }

  test("toonMetrics: verify token savings") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("score", DoubleType),
    ))

    val data = Seq(
      Row(1, "value1", 100.0),
      Row(2, "value2", 200.0),
      Row(3, "value3", 300.0),
    )

    val df = spark.createDataFrame(data.asJava, schema)
    val result = df.toonMetrics()

    assert(result.isRight)
    result.foreach { metrics =>
      // TOON should provide some savings for tabular data
      assert(metrics.toonTokenCount <= metrics.jsonTokenCount)
    }
  }

  test("fromToon: decode TOON to DataFrame") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("score", DoubleType),
    ))

    val data = Seq(
      Row(1, "Alice", 25.5),
      Row(2, "Bob", 30.0),
    )

    val originalDf = spark.createDataFrame(data.asJava, schema)
    val toonResult = originalDf.toToon(key = "data")

    assert(toonResult.isRight)

    toonResult.foreach { toonChunks =>
      val decodedResult = SparkToonOps.fromToon(toonChunks, schema)(spark)

      assert(decodedResult.isRight)
      decodedResult.foreach { decodedDf =>
        assertEquals(decodedDf.count(), 2L)
        assertEquals(decodedDf.schema, schema)
      }
    }
  }

  test("round-trip: DataFrame -> TOON -> DataFrame") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("active", BooleanType),
    ))

    val data = Seq(
      Row(1, "Alice", true),
      Row(2, "Bob", false),
      Row(3, "Charlie", true),
    )

    val originalDf = spark.createDataFrame(data.asJava, schema)

    val roundTrip = for {
      toonChunks <- originalDf.toToon(key = "data")
      decodedDf <- SparkToonOps.fromToon(toonChunks, schema)(spark)
    } yield decodedDf

    assert(roundTrip.isRight)
    roundTrip.foreach { decodedDf =>
      assertEquals(decodedDf.count(), 3L)
      assertEquals(decodedDf.schema, originalDf.schema)
    }
  }

  test("toToon: handle complex schema") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("tags", ArrayType(StringType)),
    ))

    val data = Seq(
      Row(1, "Alice", Seq("tag1", "tag2").asJava),
      Row(2, "Bob", Seq("tag3").asJava),
    )

    val df = spark.createDataFrame(data.asJava, schema)
    val result = df.toToon()

    assert(result.isRight)
    result.foreach { chunks =>
      assert(chunks.head.contains("tag1"))
      assert(chunks.head.contains("tag2"))
    }
  }

  test("toonMetrics: handle large DataFrames") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("score", DoubleType),
    ))

    val data = (1 to 1000).map(i => Row(i, s"user$i", i * 10.0))
    val df = spark.createDataFrame(data.asJava, schema)

    val result = df.toonMetrics()

    assert(result.isRight)
    result.foreach { metrics =>
      assertEquals(metrics.rowCount, 1000)
      assert(metrics.jsonTokenCount > metrics.toonTokenCount)
    }
  }

}
