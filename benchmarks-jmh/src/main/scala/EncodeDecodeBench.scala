package io.toonformat.toon4s.jmh

import java.util.concurrent.TimeUnit

import scala.collection.immutable.VectorMap

import io.toonformat.toon4s._
import io.toonformat.toon4s.JsonValue._
import io.toonformat.toon4s.internal.JObjMap
import io.toonformat.toon4s.json.SimpleJson
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.Throughput))
class EncodeDecodeBench {

  // Sample data
  private var scalaObj: Any = _

  private var scalaLargeUniform: Any = _

  private var scalaIrregular: Any = _

  private var scalaRealWorld: Any = _

  private val tabularToon: String =
    """users[2]{id,name}:
      |  1,Ada
      |  2,Bob
      |""".stripMargin

  private val listToon: String =
    """items[2]:
      |  - id: 1
      |    name: First
      |  - id: 2
      |    name: Second
      |""".stripMargin

  private val nestedToon: String =
    """orders[1]:
      |  - id: 1001
      |    items[2]{sku,qty}:
      |      A1,2
      |      B2,1
      |""".stripMargin

  private val encOpts = EncodeOptions(indent = 2)

  private val decOpts = DecodeOptions()

  @Setup(Level.Trial)
  def setup(): Unit = {
    // small
    scalaObj = Map(
      "users" -> Vector(
        Map("id" -> 1, "name" -> "Ada", "tags" -> Vector("reading", "gaming")),
        Map("id" -> 2, "name" -> "Bob", "tags" -> Vector("writing")),
      )
    )

    // large uniform (10k rows)
    val n = 10000
    val arr = Vector.tabulate(n)(i => Map("id" -> i, "name" -> s"u$i", "score" -> (i % 100)))
    scalaLargeUniform = Map("users" -> arr)

    // irregular (mixed shapes)
    val irr = Vector[Any](
      Map("id" -> 1, "name" -> "a", "tags" -> Vector("x")),
      Map("id" -> 2, "prefs" -> Map("k" -> "v")),
      Map("id" -> 3, "scores" -> Vector(1, 2, 3)),
      Map("id" -> 4, "mixed" -> Vector(Map("a" -> 1), Vector(1, 2))),
    )
    scalaIrregular = Map("items" -> irr)

    // real-world nested (orders, items, addresses)
    val orders = Vector.tabulate(200) {
      i =>
        Map(
          "id" -> (1000 + i),
          "user" -> Map("id" -> i, "name" -> s"user$i"),
          "items" -> Vector(
            Map("sku" -> s"A$i", "qty" -> (1 + (i % 3)), "price" -> BigDecimal(9.99 + (i % 5))),
            Map("sku" -> s"B$i", "qty" -> 1),
          ),
          "shipping" -> Map("city" -> "Metropolis", "zip" -> f"$i%05d"),
        )
    }
    scalaRealWorld = Map("orders" -> orders)
  }

  @Benchmark def encode_object(): String = Toon.encode(scalaObj, encOpts).fold(throw _, identity)

  @Benchmark def decode_tabular(): JsonValue =
    Toon.decode(tabularToon, decOpts).fold(throw _, identity)

  @Benchmark def decode_list(): JsonValue = Toon.decode(listToon, decOpts).fold(throw _, identity)

  @Benchmark def decode_nested(): JsonValue =
    Toon.decode(nestedToon, decOpts).fold(throw _, identity)

  // Large/irregular/real-world
  @Benchmark def encode_large_uniform(): String =
    Toon.encode(scalaLargeUniform, encOpts).fold(throw _, identity)

  @Benchmark def encode_irregular(): String =
    Toon.encode(scalaIrregular, encOpts).fold(throw _, identity)

  @Benchmark def encode_real_world(): String =
    Toon.encode(scalaRealWorld, encOpts).fold(throw _, identity)

  // Allocation comparison: building a small ordered map (5 fields), VectorMap vs JObjMap.
  private val mapKeys = Array("id", "name", "email", "age", "active")

  private val mapVals: Array[JsonValue] =
    Array(JNumber(1), JString("Alice"), JString("a@b.c"), JNumber(30), JBool(true))

  @Benchmark def build_vectormap(): VectorMap[String, JsonValue] = {
    val b = VectorMap.newBuilder[String, JsonValue]
    var i = 0
    while (i < 5) { b += ((mapKeys(i), mapVals(i))); i += 1 }
    b.result()
  }

  @Benchmark def build_jobjmap(): JObjMap = {
    val b = JObjMap.newBuilder
    var i = 0
    while (i < 5) { b.add(mapKeys(i), mapVals(i)); i += 1 }
    b.result()
  }

}
