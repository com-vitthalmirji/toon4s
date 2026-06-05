package io.toonformat.toon4s
package encode

import scala.util.Random

import munit.FunSuite

/**
 * The no-box formatDouble must be byte-identical to the original BigDecimal based formatting for
 * every finite double. The reference is BigDecimal(d).stripTrailingZeros.toPlainString.
 */
class DoubleFormatEquivalenceSpec extends FunSuite {

  private def oldFormat(d: Double): String = {
    val s = BigDecimal(d).bigDecimal.stripTrailingZeros.toPlainString
    if (s == "-0") "0" else s
  }

  private def check(d: Double): Unit =
    assertEquals(Primitives.formatDouble(d), oldFormat(d), s"d=$d bits=${java.lang.Double.doubleToLongBits(d)}")

  private val curated = List(
    0.0d, -0.0d, 1.0d, -1.0d, 0.5d, -0.5d, 3.14d, 100.0d, 0.001d, 0.0001d, 1e7d, 1e-3d, 1e-4d,
    1e10d, 1e20d, 1e-20d, 1.5e3d, 1.23e10d, 123.456d, 9999999.0d, 10000000.0d,
    Double.MinValue, Double.MaxValue, Double.MinPositiveValue, -Double.MinPositiveValue,
    java.lang.Double.MIN_VALUE, 2.5d, -2.5d, 12345678.9d, 0.1d, 0.2d, 0.3d,
  )

  test("curated doubles are byte-identical") {
    curated.foreach(check)
  }

  test("powers of ten are byte-identical") {
    (-320 to 308).foreach { k =>
      val d = java.lang.Double.parseDouble("1e" + k)
      if (!d.isInfinite && d != 0.0) check(d)
    }
  }

  test("random bit patterns are byte-identical") {
    val rnd = new Random(42L)
    var i = 0
    while (i < 2000000) {
      val d = java.lang.Double.longBitsToDouble(rnd.nextLong())
      if (!d.isNaN && !d.isInfinite) check(d)
      i += 1
    }
  }

  test("random scaled doubles are byte-identical") {
    val rnd = new Random(7L)
    var i = 0
    while (i < 500000) {
      val d = rnd.nextGaussian() * math.pow(10.0, rnd.nextInt(40) - 20)
      if (!d.isNaN && !d.isInfinite) check(d)
      i += 1
    }
  }

  test("microbench new vs old") {
    assume(sys.env.get("TOON4S_BENCH").contains("true"), "set TOON4S_BENCH=true to run")
    val rnd = new Random(1L)
    val data = Array.fill(2000000)(rnd.nextGaussian() * 1000.0)
    def runNew(): Long = {
      var acc = 0L; var i = 0
      while (i < data.length) { acc += Primitives.formatDouble(data(i)).length; i += 1 }
      acc
    }
    def runOld(): Long = {
      var acc = 0L; var i = 0
      while (i < data.length) { acc += oldFormat(data(i)).length; i += 1 }
      acc
    }
    (1 to 3).foreach(_ => { runNew(); runOld() })
    val t0 = System.nanoTime(); runNew(); val tn = System.nanoTime() - t0
    val t1 = System.nanoTime(); runOld(); val to = System.nanoTime() - t1
    println(f"formatDouble new=${tn / 1e6}%.1fms old=${to / 1e6}%.1fms speedup=${to.toDouble / tn}%.2fx")
  }
}
