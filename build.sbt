import sbt._
import sbt.Keys._

lazy val Scala3Latest   = "3.3.3"
lazy val Scala213Latest = "2.13.14"

ThisBuild / organization := "io.toonformat"
ThisBuild / scalaVersion := Scala3Latest
ThisBuild / crossScalaVersions := Seq(Scala3Latest, Scala213Latest)
ThisBuild / description := "Scala implementation of the Token-Oriented Object Notation (TOON) format."
ThisBuild / homepage := Some(url("https://github.com/vim89/toon4s"))
ThisBuild / licenses := List("MIT" -> url("https://opensource.org/licenses/MIT"))
ThisBuild / organizationName := "vim89"
ThisBuild / developers := List(
  Developer(
    id = "vim89",
    name = "vim89",
    email = "opensource@vim89.dev",
    url = url("https://github.com/vim89")
  )
)
ThisBuild / scmInfo := Some(
  ScmInfo(
    browseUrl = url("https://github.com/vim89/toon4s"),
    connection = "scm:git:https://github.com/vim89/toon4s.git"
  )
)
ThisBuild / publishMavenStyle := true
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / scalafmtOnCompile := true
ThisBuild / Compile / doc / sources := Seq.empty // avoid doc warnings early
ThisBuild / versionScheme := Some("early-semver")

// sbt-dynver configuration for automatic versioning from git tags
ThisBuild / dynverSeparator := "-" // Use '-' instead of '+' for better compatibility (docker, URLs, etc.)
ThisBuild / dynverVTagPrefix := true // Expect tags like v1.0.0 (default behavior)

val commonScalacOptions = Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xfatal-warnings"
)

// sbt-ci-release handles sonatype configuration automatically via environment variables:
// SONATYPE_USERNAME, SONATYPE_PASSWORD, SONATYPE_HOST (optional, defaults to s01.oss.sonatype.org)

lazy val root = (project in file("."))
  .aggregate(core, cli)
  .settings(
    name := "toon4s",
    publish / skip := true
  )

lazy val core = (project in file("core"))
  .settings(
    name := "toon4s-core",
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "0.7.29" % Test
    ),
    scalacOptions ++= commonScalacOptions
  )

lazy val cli = (project in file("cli"))
  .dependsOn(core)
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "toon4s-cli",
    libraryDependencies ++= Seq(
      "com.github.scopt" %% "scopt" % "4.1.0",
      "com.knuddels" % "jtokkit" % "1.1.0"
    ),
    scalacOptions ++= commonScalacOptions,
    Compile / mainClass := Some("io.toonformat.toon4s.cli.Main"),
    Compile / packageDoc / publishArtifact := false,
    maintainer := "vim89 <opensource@vim89.dev>",
    publish / skip := true
  )
