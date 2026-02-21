package io.toonformat.toon4s.spark.integrations

import io.toonformat.toon4s.spark.testkit.SparkTestSuite
import org.apache.spark.storage.StorageLevel

class DeltaLakeCDCTest extends SparkTestSuite {

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
