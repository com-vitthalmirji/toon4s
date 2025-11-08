package io.toonformat.toon4s

import io.toonformat.toon4s.decode.Streaming
import munit.FunSuite

class StreamingSpec extends FunSuite {

  test("foreachTabular streams rows under tabular header") {
    val toon =
      """users[2]{id,name}:
        |  1,Ada
        |  2,Bob
        |""".stripMargin
    val reader = new java.io.StringReader(toon)
    val acc =
      scala.collection.mutable.ArrayBuffer.empty[(Option[String], List[String], Vector[String])]
    val res = Streaming.foreachTabular(reader) {
      (k, f, v) => acc += ((k, f, v))
    }
    assert(res.isRight)
    assertEquals(acc.length, 2)
    assertEquals(acc.head._1, Some("users"))
    assertEquals(acc.head._2, List("id", "name"))
    assertEquals(acc.head._3, Vector("1", "Ada"))
  }

  test("foreachArrays streams nested headers and rows with path") {
    val toon =
      """orders[1]:
        |  - id: 1001
        |    items[2]{sku,qty}:
        |      A1,2
        |      B2,1
        |""".stripMargin
    val reader = new java.io.StringReader(toon)
    val headers = scala.collection.mutable.ArrayBuffer.empty[(Vector[String], String)]
    val rows = scala.collection.mutable.ArrayBuffer.empty[(Vector[String], Vector[String])]
    val res = Streaming.foreachArrays(reader)(
      (path, h) =>
        headers += ((path, h.key.getOrElse(""))),
      (path, _, values) => rows += ((path, values)),
    )
    assert(res.isRight)
    assert(headers.map(_._2).contains("items"))
    assertEquals(rows.map(_._2).toList, List(Vector("A1", "2"), Vector("B2", "1")))
  }

}
