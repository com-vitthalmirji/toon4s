package io.toonformat.toon4s.compare

import java.util.concurrent.TimeUnit

import io.toonformat.toon4s._

object CompareMain {

  def main(args: Array[String]): Unit = {
    val jtoonJar = sys.env.get("JTOON_JAR")
    if (jtoonJar.isEmpty) {
      System.err.println(
        "[compare] Skipping JToon comparison: set JTOON_JAR to the built JToon jar path."
      )
      return
    }
    // Samples
    val tabularToon =
      """users[2]{id,name}:
        |  1,Ada
        |  2,Bob
        |""".stripMargin

    val listToon =
      """items[2]:
        |  - id: 1
        |    name: First
        |  - id: 2
        |    name: Second
        |""".stripMargin

    val nestedToon =
      """orders[1]:
        |  - id: 1001
        |    items[2]{sku,qty}:
        |      A1,2
        |      B2,1
        |""".stripMargin

    val scalaObj: Any = Map(
      "users" -> Vector(
        Map("id" -> 1, "name" -> "Ada", "tags" -> Vector("reading", "gaming")),
        Map("id" -> 2, "name" -> "Bob", "tags" -> Vector("writing")),
      )
    )

    val encOpts = EncodeOptions(indent = 2)
    val decOpts = DecodeOptions()

    // Loops
    def bench(name: String)(f: => Unit): Double = {
      val iters = 20000
      // warmup
      var i = 0; while (i < 2000) { f; i += 1 }
      val t0 = System.nanoTime()
      i = 0; while (i < iters) { f; i += 1 }
      val t1 = System.nanoTime()
      val durMs = (t1 - t0) / 1e6
      val opsMs = iters / durMs
      println(f"$name%-20s | $opsMs%.2f ops/ms")
      opsMs
    }

    println("== toon4s (Scala) ==")
    bench("decode_tabular")(Toon.decode(tabularToon, decOpts).fold(throw _, _ => ()))
    bench("decode_list")(Toon.decode(listToon, decOpts).fold(throw _, _ => ()))
    bench("decode_nested")(Toon.decode(nestedToon, decOpts).fold(throw _, _ => ()))
    bench("encode_object")(Toon.encode(scalaObj, encOpts).fold(throw _, _ => ()))

    try {
      val cls = Class.forName("com.felipestanzani.jtoon.JToon")
      val encOptsClz = Class.forName("com.felipestanzani.jtoon.EncodeOptions")
      val encMethod = cls.getMethod("encode", classOf[Object], encOptsClz)
      val jEncOpts = encOptsClz.getConstructor().newInstance().asInstanceOf[Object]

      // Minimal Java object similar to scalaObj
      val jUser1 = new java.util.HashMap[String, Object](); jUser1.put("id", Int.box(1));
      jUser1.put("name", "Ada")
      val jUser2 = new java.util.HashMap[String, Object](); jUser2.put("id", Int.box(2));
      jUser2.put("name", "Bob")
      val jUsers = new java.util.ArrayList[Object](); jUsers.add(jUser1); jUsers.add(jUser2)
      val jRoot = new java.util.HashMap[String, Object](); jRoot.put("users", jUsers)

      println("== JToon (Java) ==")
      bench("encode_object")(encMethod.invoke(null, jRoot, jEncOpts))
    } catch {
      case t: Throwable =>
        System.err.println(
          "[compare] Failed to load/run JToon classes. Did you set JTOON_JAR correctly?"
        )
        t.printStackTrace()
    }
  }

}
