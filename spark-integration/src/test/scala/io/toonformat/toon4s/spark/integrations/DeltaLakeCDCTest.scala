package io.toonformat.toon4s.spark.integrations

import munit.FunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

class DeltaLakeCDCTest extends FunSuite {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("DeltaLakeCDCTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("withCachedBatch: unpersist after successful processing") {
    val df = spark.range(0, 10).toDF("id")

    val result = DeltaLakeCDC.withCachedBatch(df) { cached =>
      assert(cached.storageLevel != StorageLevel.NONE)
      Right(cached.count())
    }

    assertEquals(result, Right(10L))
    assertEquals(df.storageLevel, StorageLevel.NONE)
  }

  test("withCachedBatch: unpersist when processing throws") {
    val df = spark.range(0, 5).toDF("id")

    val result = DeltaLakeCDC.withCachedBatch(df)(_ => throw new IllegalStateException("boom"))

    assert(result.isLeft)
    result.left.foreach(err =>
      assert(err.message.contains("Unexpected error while processing cached CDC batch"))
    )
    assertEquals(df.storageLevel, StorageLevel.NONE)
  }

}
