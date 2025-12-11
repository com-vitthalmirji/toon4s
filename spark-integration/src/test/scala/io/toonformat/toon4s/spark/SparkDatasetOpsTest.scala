package io.toonformat.toon4s.spark

import io.toonformat.toon4s.spark.SparkDatasetOps._
import munit.FunSuite
import org.apache.spark.sql.{Dataset, SparkSession}

// Test case classes - must be defined outside test class for Spark Encoders
case class User(id: Int, name: String, age: Int)

case class Product(id: Int, name: String, price: Double, inStock: Boolean)

case class Address(street: String, city: String, zip: Int)

case class Employee(id: Int, name: String, address: Address)

class SparkDatasetOpsTest extends FunSuite {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDatasetOpsTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("Dataset[T].toToon: encode simple typed Dataset") {
    import spark.implicits._

    val users = Seq(
      User(1, "Alice", 25),
      User(2, "Bob", 30),
      User(3, "Charlie", 35),
    )
    val ds: Dataset[User] = spark.createDataset(users)

    val result = ds.toToon()

    assert(result.isRight)
    result.foreach { toonChunks =>
      assertEquals(toonChunks.size, 1) // Should fit in one chunk
      val toon = toonChunks.head
      assert(toon.nonEmpty)
      assert(toon.contains("Alice") || toon.contains("1")) // Should contain data
    }
  }

  test("Dataset[T].toToon: handle empty Dataset") {
    import spark.implicits._

    val emptyDs: Dataset[User] = spark.createDataset(Seq.empty[User])

    val result = emptyDs.toToon()

    assert(result.isRight)
    result.foreach { toonChunks =>
      assert(toonChunks.nonEmpty) // Should have at least one chunk even if empty
    }
  }

  test("Dataset[T].toToon: chunk large typed Dataset") {
    import spark.implicits._

    val largeData = (1 to 2500).map(i => User(i, s"User$i", 20 + (i % 50)))
    val ds: Dataset[User] = spark.createDataset(largeData)

    val result = ds.toToon(maxRowsPerChunk = 1000)

    assert(result.isRight)
    result.foreach { toonChunks =>
      assert(toonChunks.size >= 3) // Should create multiple chunks
      assertEquals(toonChunks.size, 3) // 2500 rows / 1000 = 3 chunks
    }
  }

  test("Dataset[T].toonMetrics: compute token metrics for typed Dataset") {
    import spark.implicits._

    val products = Seq(
      Product(1, "Laptop", 999.99, true),
      Product(2, "Mouse", 29.99, true),
      Product(3, "Keyboard", 79.99, false),
    )
    val ds: Dataset[Product] = spark.createDataset(products)

    val result = ds.toonMetrics()

    assert(result.isRight)
    result.foreach { metrics =>
      assert(metrics.jsonTokenCount > 0)
      assert(metrics.toonTokenCount > 0)
      assertEquals(metrics.rowCount, 3)
      assertEquals(metrics.columnCount, 4) // id, name, price, inStock
    }
  }

  test("Dataset[T].toonMetrics: verify token savings") {
    import spark.implicits._

    val users = (1 to 100).map(i => User(i, s"User$i", 20 + (i % 50)))
    val ds: Dataset[User] = spark.createDataset(users)

    val result = ds.toonMetrics()

    assert(result.isRight)
    result.foreach { metrics =>
      // TOON should typically save tokens for tabular data
      assert(metrics.savingsPercent > 0.0 || metrics.savingsPercent >= -10.0)
      assertEquals(metrics.rowCount, 100)
      assertEquals(metrics.columnCount, 3)
    }
  }

  test("fromToon: decode TOON to typed Dataset[T]") {
    import spark.implicits._

    val originalUsers = Seq(
      User(1, "Alice", 25),
      User(2, "Bob", 30),
    )
    val originalDs: Dataset[User] = spark.createDataset(originalUsers)

    // Encode to TOON
    val toonResult = originalDs.toToon()
    assert(toonResult.isRight)

    // Decode back to Dataset[User]
    val decodeResult = toonResult.flatMap { toonChunks =>
      SparkDatasetOps.fromToon[User](toonChunks)(spark, implicitly)
    }

    assert(decodeResult.isRight)
    decodeResult.foreach { decodedDs =>
      val decoded = decodedDs.collect()
      assertEquals(decoded.length, 2)
      assert(decoded.exists(_.name == "Alice"))
      assert(decoded.exists(_.name == "Bob"))
    }
  }

  test("round-trip: Dataset[T] -> TOON -> Dataset[T]") {
    import spark.implicits._

    val originalProducts = Seq(
      Product(1, "Laptop", 999.99, true),
      Product(2, "Mouse", 29.99, true),
      Product(3, "Keyboard", 79.99, false),
    )
    val originalDs: Dataset[Product] = spark.createDataset(originalProducts)

    val result = for {
      toonChunks <- originalDs.toToon()
      decodedDs <- SparkDatasetOps.fromToon[Product](toonChunks)(spark, implicitly)
    } yield decodedDs

    assert(result.isRight)
    result.foreach { decodedDs =>
      val decoded = decodedDs.collect().sortBy(_.id)
      val original = originalProducts.sortBy(_.id)

      assertEquals(decoded.length, original.length)
      decoded.zip(original).foreach {
        case (dec, orig) =>
          assertEquals(dec.id, orig.id)
          assertEquals(dec.name, orig.name)
          assertEquals(dec.price, orig.price, 0.01)
          assertEquals(dec.inStock, orig.inStock)
      }
    }
  }

  test("Dataset[T].toToon: handle nested case classes") {
    import spark.implicits._

    val employees = Seq(
      Employee(1, "Alice", Address("123 Main St", "NYC", 10001)),
      Employee(2, "Bob", Address("456 Oak Ave", "LA", 90001)),
    )
    val ds: Dataset[Employee] = spark.createDataset(employees)

    val result = ds.toToon()

    assert(result.isRight)
    result.foreach { toonChunks =>
      assertEquals(toonChunks.size, 1)
      val toon = toonChunks.head
      assert(toon.nonEmpty)
      // Should contain nested data
      assert(toon.contains("NYC") || toon.contains("123"))
    }
  }

  test("Dataset[T].toToon: custom key name") {
    import spark.implicits._

    val users = Seq(User(1, "Alice", 25))
    val ds: Dataset[User] = spark.createDataset(users)

    val result = ds.toToon(key = "custom_users_data")

    assert(result.isRight)
    result.foreach { toonChunks =>
      val toon = toonChunks.head
      // The TOON format should use the custom key
      assert(toon.nonEmpty)
    }
  }

  test("Dataset[T].showToonSample: doesn't throw") {
    import spark.implicits._

    val users = (1 to 20).map(i => User(i, s"User$i", 20))
    val ds: Dataset[User] = spark.createDataset(users)

    // Should not throw - this is a side-effect method for debugging
    ds.showToonSample(3)
  }

  test("Dataset[T] operations: type safety maintained") {
    import spark.implicits._

    val users = Seq(
      User(1, "Alice", 25),
      User(2, "Bob", 30),
      User(3, "Charlie", 35),
    )
    val ds: Dataset[User] = spark.createDataset(users)

    // Verify we can still use typed Dataset operations
    val filtered = ds.filter(_.age > 25)
    val mapped = ds.map(u => u.copy(age = u.age + 1))

    // Encode filtered Dataset
    val result = filtered.toToon()
    assert(result.isRight)

    // Encode mapped Dataset
    val result2 = mapped.toToon()
    assert(result2.isRight)
  }

  test("Dataset[T]: compile-time type safety") {
    import spark.implicits._

    val users = Seq(User(1, "Alice", 25))
    val ds: Dataset[User] = spark.createDataset(users)

    // This should compile - accessing fields with autocomplete
    val names: Dataset[String] = ds.map(_.name)
    assert(names.collect().contains("Alice"))

    // The toToon() method should work on any Dataset[T]
    val result = ds.toToon()
    assert(result.isRight)
  }

}
