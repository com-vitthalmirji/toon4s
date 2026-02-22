package io.toonformat.toon4s

import scala.collection.immutable.VectorMap
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

import io.toonformat.toon4s.JsonValue._
import munit.FunSuite

class ThreadSafetySpec extends FunSuite {

  private implicit val executionContext: ExecutionContext = ExecutionContext.global

  private val sample = JObj(
    VectorMap(
      "users" -> JArray(
        Vector(
          JObj(VectorMap("id" -> JNumber(1), "name" -> JString("Alice"))),
          JObj(VectorMap("id" -> JNumber(2), "name" -> JString("Bob"))),
          JObj(VectorMap("id" -> JNumber(3), "name" -> JString("Carol"))),
        )
      )
    )
  )

  test("concurrent encode and decode remain deterministic") {
    val options = EncodeOptions.withLengthMarker(EncodeOptions(), enabled = true)
    val expectedEncoded = Toon.encode(sample, options).getOrElse("")
    val tasks = Vector.fill(200) {
      Future {
        val encoded = Toon.encode(sample, options).getOrElse("")
        val decoded = Toon.decode(encoded)
        (encoded, decoded)
      }
    }

    val results = Await.result(Future.sequence(tasks), 30.seconds)
    results.foreach {
      case (encoded, decoded) =>
        assertEquals(encoded, expectedEncoded)
        assertEquals(decoded, Right(sample))
    }
  }

  test("concurrent decode supports both [N] and [#N] headers") {
    val baseToon =
      """users[2]{id,name}:
        |  1,Alice
        |  2,Bob
        |""".stripMargin
    val markedToon =
      """users[#2]{id,name}:
        |  1,Alice
        |  2,Bob
        |""".stripMargin

    val tasks = Vector.tabulate(200) { index =>
      Future {
        val input = if (index % 2 == 0) baseToon else markedToon
        Toon.decode(input)
      }
    }

    val results = Await.result(Future.sequence(tasks), 30.seconds)
    assert(results.forall(_.isRight))
  }

}
