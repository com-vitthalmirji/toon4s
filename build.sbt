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
    name = "Vitthal Mirji",
    email = "vitthalmirji@gmail.com",
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
  .aggregate(core, cli, jmh, compare)
  .settings(
    name := "toon4s",
    publish / skip := true
  )

lazy val core = (project in file("core"))
  .enablePlugins(MimaPlugin)
  .settings(
    name := "toon4s-core",
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit"            % "1.2.1"  % Test,
      "org.scalacheck" %% "scalacheck"       % "1.19.0" % Test,
      "org.scalameta" %% "munit-scalacheck" % "1.2.0"  % Test
    ),
    scalacOptions ++= commonScalacOptions,
    // ScalaDoc configuration
    Compile / doc / scalacOptions ++= {
      if (scalaVersion.value.startsWith("3."))
        Seq(
          "-project", "toon4s-core",
          "-project-version", version.value,
          "-social-links:github::https://github.com/vim89/toon4s"
        )
      else
        Seq(
          "-groups",
          "-doc-title", "toon4s-core",
          "-doc-version", version.value
        )
    },
    // MiMa configuration for binary compatibility checking
    // Check against previous published versions to ensure no breaking changes
    mimaPreviousArtifacts := Set(
      // Uncomment when first version is published:
      // organization.value %% moduleName.value % "0.1.0"
    ),
    // Exclude known binary incompatible changes (add as needed)
    mimaBinaryIssueFilters := Seq(
      // Example: ProblemFilters.exclude[Problem]("io.toonformat.toon4s.InternalClass")
    )
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
    maintainer := "Vitthal Mirji <vitthalmirji@gmail.com>",
    publish / skip := true
  )

lazy val jmh = (project in file("benchmarks-jmh"))
  .dependsOn(core)
  .enablePlugins(JmhPlugin)
  .settings(
    name := "toon4s-jmh",
    publish / skip := true,
    scalacOptions ++= commonScalacOptions
  )

lazy val compare = (project in file("compare"))
  .dependsOn(core)
  .settings(
    name := "toon4s-compare",
    publish / skip := true,
    scalacOptions ++= commonScalacOptions,
    libraryDependencies ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.20.1"
    ),
    Compile / unmanagedJars ++= {
      sys.env.get("JTOON_JAR").toList.map(file)
    }
  )
// sbt aliases for quick vs heavy JMH runs
addCommandAlias(
  "jmhDev",
  "jmh/jmh:run -i 1 -wi 1 -r 500ms -w 500ms -f1 -t1 io.toonformat.toon4s.jmh.EncodeDecodeBench.*"
)

addCommandAlias(
  "jmhFull",
  "jmh/jmh:run -i 5 -wi 5 -r 2s -w 2s -f1 -t1 io.toonformat.toon4s.jmh.EncodeDecodeBench.decode_tabular io.toonformat.toon4s.jmh.EncodeDecodeBench.decode_list io.toonformat.toon4s.jmh.EncodeDecodeBench.decode_nested io.toonformat.toon4s.jmh.EncodeDecodeBench.encode_object"
)
