import sbt._
import sbt.Keys._
import sbt.{Credentials => SBCredentials}

lazy val Scala3Latest   = "3.3.3"
lazy val Scala213Latest = "2.13.14"

ThisBuild / organization := "io.toonformat"
ThisBuild / scalaVersion := Scala3Latest
ThisBuild / crossScalaVersions := Seq(Scala3Latest, Scala213Latest)
ThisBuild / version := "0.1.0"
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

val commonScalacOptions = Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xfatal-warnings"
)

lazy val sonatypeHost: String = sys.env.getOrElse("SONATYPE_HOST", "s01.oss.sonatype.org")

def sonatypeCredentials: Seq[SBCredentials] =
  (for {
    user <- sys.env.get("SONATYPE_USERNAME")
    pass <- sys.env.get("SONATYPE_PASSWORD")
  } yield SBCredentials("Sonatype Nexus Repository Manager", sonatypeHost, user, pass)).toSeq

ThisBuild / credentials ++= sonatypeCredentials
ThisBuild / sonatypeCredentialHost := sonatypeHost
ThisBuild / sonatypeRepository := s"https://$sonatypeHost"
ThisBuild / publishTo := sonatypePublishToBundle.value

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
