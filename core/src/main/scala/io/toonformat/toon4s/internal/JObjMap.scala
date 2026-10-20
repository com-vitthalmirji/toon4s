package io.toonformat.toon4s
package internal

import scala.collection.immutable.{AbstractMap, VectorMap, Map => IMap}
import scala.collection.mutable.ArrayBuffer

/**
 * Insertion-ordered immutable map backed by parallel arrays, sized for small build-once maps such
 * as TOON objects. Construction is cheap (two flat arrays, no Vector trie or HashMap), iteration is
 * O(1) per element in insertion order, and lookup is O(n).
 *
 * Equality and hashCode follow standard Map semantics (order-independent), so a JObjMap compares
 * equal to a VectorMap with the same entries.
 */
final class JObjMap private[internal] (
    private val ks: Array[String],
    private val vs: Array[JsonValue],
) extends AbstractMap[String, JsonValue] {

  override def size: Int = ks.length

  override def knownSize: Int = ks.length

  override def isEmpty: Boolean = ks.length == 0

  private def indexOf(key: String): Int = {
    var i = 0
    while (i < ks.length) {
      if (ks(i) == key) return i
      i += 1
    }
    -1
  }

  def get(key: String): Option[JsonValue] = {
    val i = indexOf(key)
    if (i < 0) None else Some(vs(i))
  }

  override def contains(key: String): Boolean = indexOf(key) >= 0

  def iterator: Iterator[(String, JsonValue)] = new Iterator[(String, JsonValue)] {
    private var i = 0
    def hasNext: Boolean = i < ks.length
    def next(): (String, JsonValue) = {
      val pair = (ks(i), vs(i))
      i += 1
      pair
    }
  }

  override def keysIterator: Iterator[String] = ks.iterator

  override def valuesIterator: Iterator[JsonValue] = vs.iterator

  override def foreach[U](f: ((String, JsonValue)) => U): Unit = {
    var i = 0
    while (i < ks.length) {
      f((ks(i), vs(i)))
      i += 1
    }
  }

  def removed(key: String): IMap[String, JsonValue] = {
    val idx = indexOf(key)
    if (idx < 0) this
    else {
      val nk = new Array[String](ks.length - 1)
      val nv = new Array[JsonValue](vs.length - 1)
      var i = 0
      var j = 0
      while (i < ks.length) {
        if (i != idx) {
          nk(j) = ks(i)
          nv(j) = vs(i)
          j += 1
        }
        i += 1
      }
      new JObjMap(nk, nv)
    }
  }

  def updated[V1 >: JsonValue](key: String, value: V1): IMap[String, V1] =
    value match {
    case jv: JsonValue =>
      val idx = indexOf(key)
      if (idx < 0) {
        val nk = new Array[String](ks.length + 1)
        val nv = new Array[JsonValue](vs.length + 1)
        System.arraycopy(ks, 0, nk, 0, ks.length)
        System.arraycopy(vs, 0, nv, 0, vs.length)
        nk(ks.length) = key
        nv(vs.length) = jv
        new JObjMap(nk, nv)
      } else {
        val nv = vs.clone()
        nv(idx) = jv
        new JObjMap(ks, nv)
      }
    case _ =>
      // Widening beyond JsonValue is a cold path: fall back to a generic ordered map.
      VectorMap.from(this).updated(key, value)
    }
}

object JObjMap {

  val empty: JObjMap = new JObjMap(Array.empty, Array.empty)

  def newBuilder: Builder = new Builder

  /** Build from pairs; on a duplicate key the last value wins while the first position is kept. */
  def from(pairs: IterableOnce[(String, JsonValue)]): JObjMap = {
    val b = new Builder
    pairs.iterator.foreach(kv => b.add(kv._1, kv._2))
    b.result()
  }

  final class Builder {
    private val ks = ArrayBuffer.empty[String]
    private val vs = ArrayBuffer.empty[JsonValue]

    def add(key: String, value: JsonValue): Unit = {
      var i = 0
      val n = ks.length
      while (i < n) {
        if (ks(i) == key) {
          vs(i) = value
          return
        }
        i += 1
      }
      ks += key
      vs += value
    }

    def +=(kv: (String, JsonValue)): this.type = {
      add(kv._1, kv._2)
      this
    }

    def result(): JObjMap = new JObjMap(ks.toArray, vs.toArray)
  }
}
