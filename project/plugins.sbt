addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")

addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.10.4")

addSbtPlugin(
  "com.github.sbt" % "sbt-ci-release" % "1.11.0"
) // Latest: Includes sbt-dynver, sbt-pgp, sbt-sonatype, sbt-git. Required for Central Portal (legacy OSSRH sunset 2025-06-30)

addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.5")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "1.1.4")

addSbtPlugin("org.scala-js" % "sbt-scalajs" % "1.17.0")

addSbtPlugin("org.portable-scala" % "sbt-scalajs-crossproject" % "1.3.2")
