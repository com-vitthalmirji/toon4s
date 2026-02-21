import sbt._
import sbt.Keys._
import sbtdynver.DynVerPlugin.autoImport._

lazy val Scala3Latest = "3.3.3"

lazy val Scala213Latest = "2.13.16"

lazy val SparkSqlDefaultVersion = "3.5.0"

lazy val SparkSqlVersion =
  sys.props.getOrElse("toon4s.spark.version", SparkSqlDefaultVersion)

lazy val Llm4sVersion =
  sys.props.getOrElse("toon4s.llm4s.version", "0.1.16")

ThisBuild / organization := "com.vitthalmirji"

ThisBuild / scalaVersion := Scala3Latest

ThisBuild / crossScalaVersions := Seq(Scala3Latest, Scala213Latest)

ThisBuild / description := "Scala implementation of the Token-Oriented Object Notation (TOON) format."

ThisBuild / homepage := Some(url("https://github.com/com-vitthalmirji/toon4s"))

ThisBuild / licenses := List("MIT" -> url("https://opensource.org/licenses/MIT"))

ThisBuild / organizationName := "com-vitthalmirji"

ThisBuild / developers := List(
  Developer(
    id = "com-vitthalmirji",
    name = "Vitthal Mirji",
    email = "vitthalmirji@gmail.com",
    url = url("https://vitthalmirji.com/"),
  )
)

ThisBuild / scmInfo := Some(
  ScmInfo(
    browseUrl = url("https://github.com/com-vitthalmirji/toon4s"),
    connection = "scm:git:https://github.com/com-vitthalmirji/toon4s.git",
    devConnection = Some("scm:git:git@github.com:com-vitthalmirji/toon4s.git"),
  )
)

ThisBuild / scalafmtOnCompile := {
  // scalafmt has issues resolving the config path on Windows CI,
  // so only run it automatically on non-Windows platforms.
  val os = sys.props.getOrElse("os.name", "").toLowerCase
  !os.contains("windows")
}

ThisBuild / versionScheme := Some("early-semver")

// sbt-dynver configuration for automatic versioning from git tags
ThisBuild / dynverSeparator := "-" // Use '-' instead of '+' for better compatibility (docker, URLs, etc.)

ThisBuild / dynverVTagPrefix := true // Expect tags like v1.0.0 (default behavior)

ThisBuild / dynverSonatypeSnapshots := true // Append -SNAPSHOT for Sonatype compatibility on non-tag builds

ThisBuild / autoAPIMappings := true

val commonScalacOptions = Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xfatal-warnings",
)

// sbt-ci-release handles sonatype configuration automatically via environment variables:
// SONATYPE_USERNAME, SONATYPE_PASSWORD, SONATYPE_HOST (optional, defaults to s01.oss.sonatype.org)
// With dynverSonatypeSnapshots enabled, non-tag builds get -SNAPSHOT suffix and publish to Sonatype Snapshots
// The publishTo setting is automatically configured by sbt-ci-release for both snapshots and releases

lazy val root = (project in file("."))
  .aggregate(core, cli, jmh, sparkIntegration)
  .settings(
    name := "toon4s",
    publish / skip := true,
  )

lazy val core = (project in file("core"))
  .enablePlugins(MimaPlugin)
  .settings(
    name := "toon4s-core",
    libraryDependencies ++= Seq(
      "org.scalameta"  %% "munit"            % "1.2.1"  % Test,
      "org.scalacheck" %% "scalacheck"       % "1.19.0" % Test,
      "org.scalameta"  %% "munit-scalacheck" % "1.2.0"  % Test,
    ),
    scalacOptions ++= commonScalacOptions,
    // ScalaDoc configuration
    Compile / doc / scalacOptions ++= {
      if (scalaVersion.value.startsWith("3."))
        Seq(
          "-project",
          "toon4s-core",
          "-project-version",
          version.value,
          "-social-links:github::https://github.com/com-vitthalmirji/toon4s",
        )
      else
        Seq(
          "-groups",
          "-doc-title",
          "toon4s-core",
          "-doc-version",
          version.value,
        )
    },
    // MiMa configuration for binary compatibility checking
    mimaPreviousArtifacts := Set(
      organization.value %% moduleName.value % "0.7.0"
    ),
    // Exclude known binary incompatible changes (add as needed)
    mimaBinaryIssueFilters := Seq(
      // Example: ProblemFilters.exclude[Problem]("io.toonformat.toon4s.InternalClass")
    ),
  )

lazy val cli = (project in file("cli"))
  .dependsOn(core)
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "toon4s-cli",
    libraryDependencies ++= Seq(
      "com.github.scopt" %% "scopt"   % "4.1.0",
      "com.knuddels"      % "jtokkit" % "1.1.0",
    ),
    scalacOptions ++= commonScalacOptions,
    Compile / mainClass := Some("io.toonformat.toon4s.cli.Main"),
    Compile / packageDoc / publishArtifact := false,
    maintainer := "Vitthal Mirji <vitthalmirji@gmail.com>",
    publish / skip := true,
  )

lazy val jmh = (project in file("benchmarks-jmh"))
  .dependsOn(core)
  .enablePlugins(JmhPlugin)
  .settings(
    name := "toon4s-jmh",
    publish / skip := true,
    scalacOptions ++= commonScalacOptions,
  )

// sbt aliases for quick vs heavy JMH runs
addCommandAlias(
  "jmhDev",
  "jmh/jmh:run -i 1 -wi 1 -r 500ms -w 500ms -f1 -t1 io.toonformat.toon4s.jmh.EncodeDecodeBench.*",
)

addCommandAlias(
  "jmhFull",
  "jmh/jmh:run -i 5 -wi 5 -r 2s -w 2s -f1 -t1 io.toonformat.toon4s.jmh.EncodeDecodeBench.decode_tabular io.toonformat.toon4s.jmh.EncodeDecodeBench.decode_list io.toonformat.toon4s.jmh.EncodeDecodeBench.decode_nested io.toonformat.toon4s.jmh.EncodeDecodeBench.encode_object",
)

lazy val sparkIntegration = (project in file("spark-integration"))
  .dependsOn(core)
  .enablePlugins(MimaPlugin)
  .settings(
    name := "toon4s-spark",
    // Override scalaVersion to Scala 2.13 (Spark doesn't support Scala 3)
    scalaVersion := Scala213Latest,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % SparkSqlVersion % Provided,
      ("org.llm4s"       %% "core"      % Llm4sVersion % Provided).intransitive(),
      "org.scalameta"    %% "munit"     % "1.2.1" % Test,
      "org.scalacheck"   %% "scalacheck"       % "1.19.0" % Test,
      "org.scalameta"    %% "munit-scalacheck" % "1.2.0"  % Test,
    ),
    scalacOptions ++= commonScalacOptions,
    // Allow Scala 2.13 compiler to read Scala 3 TASTy from toon4s-core
    scalacOptions ++= {
      if (scalaVersion.value.startsWith("2.13")) Seq("-Ytasty-reader") else Seq.empty
    },
    // Cross-build for Spark compatibility (Spark supports Scala 2.13)
    crossScalaVersions := Seq(Scala213Latest),
    // Fix Spark class loader issues in tests - run tests in forked JVM
    Test / fork := true,
    // Add JVM options for Java 17+ compatibility with Spark
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    ),
    // ScalaDoc configuration
    Compile / doc / scalacOptions ++= Seq(
      "-groups",
      "-doc-title",
      "toon4s-spark",
      "-doc-version",
      version.value,
    ),
    // MiMa configuration for binary compatibility checking
    mimaPreviousArtifacts := Set(
      organization.value %% moduleName.value % "0.7.0"
    ),
    // Exclude known binary incompatible changes (add as needed)
    mimaBinaryIssueFilters := Seq(
      // Example: ProblemFilters.exclude[Problem]("io.toonformat.toon4s.spark.InternalClass")
    ),
  )
