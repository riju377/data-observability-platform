name := "data-observability-platform"
// Use io.github.yourusername for auto-approved namespace
organization := "io.github.riju377"
version := "1.4.0"
scalaVersion := "2.12.18"

// Spark dependencies (marked as "provided" so they're not bundled)
val sparkVersion = "3.5.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",

  // PostgreSQL driver (included in package)
  "org.postgresql" % "postgresql" % "42.7.1",

  // JSON processing (included)
  "io.circe" %% "circe-core" % "0.14.6",
  "io.circe" %% "circe-generic" % "0.14.6",
  "io.circe" %% "circe-parser" % "0.14.6",

  // Logging (Spark provides the backend, so logback is provided-only)
  "ch.qos.logback" % "logback-classic" % "1.2.12" % "provided",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",

  // Testing (test scope only)
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,
  "org.scalatestplus" %% "mockito-4-11" % "3.2.17.0" % Test,

  // Alert Channels
  "com.sun.mail" % "javax.mail" % "1.6.2",
  "org.scalaj" %% "scalaj-http" % "2.4.2"
)

// Assembly settings for building fat JAR (for direct download/EMR)
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}

// Don't run tests during assembly
assembly / test := {}

// Assembly JAR name
assembly / assemblyJarName := s"${name.value}-assembly-${version.value}.jar"

// ============================================
// Maven Central Publishing Setup (Central Portal)
// ============================================

// Required metadata for Maven Central
ThisBuild / description := "A data observability platform for Apache Spark with quality monitoring, lineage tracking, and anomaly detection."
ThisBuild / organizationName := "riju377"
ThisBuild / organizationHomepage := Some(url("https://github.com/riju377"))
ThisBuild / licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / homepage := Some(url("https://github.com/riju377/data-observability-platform"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/riju377/data-observability-platform"),
    "scm:git@github.com:riju377/data-observability-platform.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id = "riju377",
    name = "Riju Mondal",
    email = "rijumondal377@gmail.com",
    url = url("https://github.com/riju377")
  )
)

// ============================================
// Publishing settings (New Central Portal)
// https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html
// ============================================
ThisBuild / publishMavenStyle := true

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }

// Don't publish test artifacts
Test / publishArtifact := false

// Credentials for Central Portal
// sbt 1.11.x reads from SONATYPE_USERNAME and SONATYPE_PASSWORD env vars automatically
// For local use, create ~/.sbt/sonatype_central_credentials with:
//   host=central.sonatype.com
//   user=<your token username>
//   password=<your token password>
credentials += Credentials(Path.userHome / ".sbt" / "sonatype_central_credentials")

// New Central Portal publishing (sbt 1.11.0+)
// https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html
ThisBuild / sonatypeCredentialHost := "central.sonatype.com"
ThisBuild / publishTo := {
  val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
  if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
  else localStaging.value
}

// ============================================
// Compiler options
// ============================================
scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked"
)

// Parallel execution
Test / parallelExecution := false

// Resolver
resolvers += "Maven Central" at "https://repo1.maven.org/maven2/"

// Allow running Spark locally with sbt run (includes provided dependencies)
Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated
Compile / runMain := Defaults.runMainTask(Compile / fullClasspath, Compile / run / runner).evaluated
