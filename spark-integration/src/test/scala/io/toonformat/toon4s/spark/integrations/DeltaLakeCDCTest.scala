package io.toonformat.toon4s.spark.integrations

import io.toonformat.toon4s.spark.testkit.SparkTestSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
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

  test("collectChangeTypes: enforce max cardinality safety limit") {
    val schema = StructType(Seq(
      StructField("_change_type", StringType, nullable = false),
      StructField("_commit_version", LongType, nullable = false),
    ))
    val rows = Seq(
      Row("insert", 1L),
      Row("update_preimage", 1L),
      Row("delete", 1L),
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

    val result = DeltaLakeCDC.collectChangeTypes(df, maxCollectedChangeTypes = 2)

    assert(result.isLeft)
    result.left.foreach(err =>
      assert(err.message.contains("cardinality exceeds safety limit"))
    )
  }

  test("collectCommitVersions: returns min and max commit versions") {
    val schema = StructType(Seq(
      StructField("_change_type", StringType, nullable = false),
      StructField("_commit_version", LongType, nullable = false),
    ))
    val rows = Seq(
      Row("insert", 8L),
      Row("update_postimage", 12L),
      Row("delete", 10L),
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

    val result = DeltaLakeCDC.collectCommitVersions(df)

    assertEquals(result, Right((8L, 12L)))
  }

  test("resolveChunkSize: rejects non-positive configured chunk size") {
    val schema = StructType(Seq(StructField("id", LongType, nullable = false)))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1L), Row(2L))),
      schema,
    )
    val config = DeltaLakeCDC.DeltaCDCConfig(
      tableName = "ignored",
      checkpointLocation = "/tmp/checkpoint",
      maxRowsPerChunk = Some(0),
    )

    val result = DeltaLakeCDC.resolveChunkSize(df, config, batchId = 1L)

    assert(result.isLeft)
    result.left.foreach(err =>
      assert(err.message.contains("maxRowsPerChunk must be greater than 0"))
    )
  }

  test("resolveChunkSize: adaptive branch returns positive chunk size") {
    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("event_type", StringType, nullable = false),
    ))
    val rows = (1L to 50L).map(id => Row(id, "insert"))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    val config = DeltaLakeCDC.DeltaCDCConfig(
      tableName = "ignored",
      checkpointLocation = "/tmp/checkpoint",
      maxRowsPerChunk = None,
    )

    val result = DeltaLakeCDC.resolveChunkSize(df, config, batchId = 99L)

    assert(result.isRight)
    result.foreach(chunkSize => assert(chunkSize > 0))
  }

}
