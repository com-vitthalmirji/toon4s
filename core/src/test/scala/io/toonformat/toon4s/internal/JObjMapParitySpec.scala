package io.toonformat.toon4s
package internal

import scala.collection.immutable.VectorMap
import scala.util.Random

import io.toonformat.toon4s.JsonValue._
import munit.FunSuite

/**
 * JObjMap must behave identically to VectorMap for everything the codebase relies on: insertion
 * order on iteration, lookup, equality, and duplicate-key handling. This is the gate before wiring
 * it into JObj.
 */
class JObjMapParitySpec extends FunSuite {

  private def randValue(rnd: Random): JsonValue =
    rnd.nextInt(4) match {
    case 0 => JString("s" + rnd.nextInt(100))
    case 1 => JNumber(BigDecimal(rnd.nextInt(1000)))
    case 2 => JBool(rnd.nextBoolean())
    case _ => JNull
    }

  private def randomPairs(rnd: Random): Vector[(String, JsonValue)] = {
    val n = rnd.nextInt(8)
    // small key space so duplicates occur often
    Vector.fill(n)(("k" + rnd.nextInt(5), randValue(rnd)))
  }

  test("construction, order, lookup, equality match VectorMap over random inputs (incl. dup keys)") {
    val rnd = new Random(123L)
    var i = 0
    while (i < 50000) {
      val pairs = randomPairs(rnd)
      val jm = JObjMap.from(pairs)
      val vm = VectorMap.from(pairs)

      assertEquals(jm.iterator.toList, vm.iterator.toList, s"order $pairs")
      assertEquals(jm.size, vm.size)
      assertEquals(jm.keysIterator.toList, vm.keysIterator.toList)
      assertEquals(jm.valuesIterator.toList, vm.valuesIterator.toList)
      (0 to 6).foreach { k =>
        val key = "k" + k
        assertEquals(jm.get(key), vm.get(key), s"get $key in $pairs")
        assertEquals(jm.contains(key), vm.contains(key))
      }
      // Map equality is order-independent; JObjMap must equal the VectorMap with same entries.
      assertEquals[Any, Any](jm, vm, s"equals $pairs")
      assertEquals(jm.hashCode(), vm.hashCode(), s"hashCode $pairs")

      // foreach order parity
      val jmBuf = Vector.newBuilder[(String, JsonValue)]
      jm.foreach(jmBuf += _)
      assertEquals(jmBuf.result(), vm.iterator.toList.toVector)

      i += 1
    }
  }

  test("updated and removed match VectorMap") {
    val rnd = new Random(7L)
    var i = 0
    while (i < 20000) {
      val pairs = randomPairs(rnd)
      val jm = JObjMap.from(pairs)
      val vm = VectorMap.from(pairs)
      val key = "k" + rnd.nextInt(6)
      val v = randValue(rnd)
      assertEquals(jm.updated(key, v).iterator.toList, vm.updated(key, v).iterator.toList)
      assertEquals(jm.removed(key).iterator.toList, vm.removed(key).iterator.toList)
      i += 1
    }
  }
}
