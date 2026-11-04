//> using scala 3.3.8
//> using jvm 21
//> using dep "com.vitthalmirji::toon4s-core:0.9.3"

// Run with: scala-cli run examples/scala-cli/quickstart.scala

import io.toonformat.toon4s._

@main def quickstart(): Unit = {
  val payload = Map(
    "users" -> Vector(
      Map("id" -> 1, "name" -> "Ada", "tags" -> Vector("reading", "gaming")),
      Map("id" -> 2, "name" -> "Bob", "tags" -> Vector("writing")),
    )
  )

  val toon = Toon.encode(payload, EncodeOptions(indent = 2)).fold(throw _, identity)
  println(toon)

  val decoded = Toon.decode(toon).fold(throw _, identity)
  println(decoded)
}
