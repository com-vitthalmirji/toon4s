package io.toonformat.toon4s.spark

import scala.jdk.CollectionConverters._

import io.toonformat.toon4s.spark.SparkToonOps._
import io.toonformat.toon4s.spark.testkit.SparkTestSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * Reference recipe: compose toon4s with a model-calling SQL function such as Databricks ai_query.
 * toon4s encodes each chunk to compact TOON, then ai_query runs over those chunks. On Databricks
 * ai_query is built in; here it is mocked so the recipe runs locally and in CI.
 */
class AiQueryRecipeTest extends SparkTestSuite {

  test("toon4s chunks feed a mocked ai_query SQL call") {
    // Stand in for the Databricks ai_query(endpoint, prompt) built in.
    spark.udf.register(
      "ai_query",
      (endpoint: String, prompt: String) => s"$endpoint analyzed ${prompt.length} chars",
    )

    val schema = StructType(Seq(
      StructField("id", LongType),
      StructField("name", StringType),
      StructField("score", DoubleType),
    ))
    val data = (1 to 10).map(i => Row(i.toLong, s"user$i", i.toDouble))
    val df = spark.createDataFrame(data.asJava, schema)

    val chunks = df.toToonDataset(ToonSparkOptions(key = "rows", maxRowsPerChunk = 5))

    // Real TOON, not JSON: the tabular header is present.
    assert(chunks.collect().forall(_.contains("rows[")))

    chunks.createOrReplaceTempView("toon_chunks")
    val responses = spark
      .sql("SELECT ai_query('my-endpoint', value) AS analysis FROM toon_chunks")
      .collect()
      .map(_.getString(0))

    assertEquals(responses.length, 2)
    assert(responses.forall(_.startsWith("my-endpoint analyzed")))
  }

}
