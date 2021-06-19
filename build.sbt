// vim: set syntax=scala:
import ScalaProject._

val monixVersion = "2.3.3"

val akkaVersion = "2.6.15"

val scalaReflect = "org.scala-lang" % "scala-reflect" % "2.13.6"

crossScalaVersions := Seq("2.11.11", "2.12.1")

val commonSettings = Seq(
  organization := "net.gfxmonk",
  name := "capsul",
  description := "Minimal, thread-safe state encapsulation",
  version := "0.3.0",

  /* libraryDependencies += "io.monix" %% "monix-execution" % monixVersion, */
  libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.9" % "test"
)

enablePlugins(JCStressPlugin)

lazy val log = (project in file("log")).settings(
  commonSettings,
  hiddenProjectSettings,
  scalacOptions ++= Seq("-language:experimental.macros"),
  libraryDependencies += scalaReflect,
  name := "capsul-log"
)

lazy val core = (project in file("core")).settings(
  commonSettings,
  publicProjectSettings,
  /* libraryDependencies += scalaReflect, */
  name := "capsul",
).dependsOn(log % "compile-internal").dependsOn(log % "test")

lazy val mini = (project in file("mini")).settings(
  commonSettings,
  publicProjectSettings,
  name := "capsul-mini",
).dependsOn(core % "test")

lazy val perf = (project in file("perf")).settings(
  commonSettings,
  hiddenProjectSettings,
  libraryDependencies += "io.monix" %% "monix" % monixVersion,
  libraryDependencies += "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  name := "capsul-perf"
).dependsOn(core).dependsOn(log)

lazy val stress = (project in file("stress")).settings(
  commonSettings,
  hiddenProjectSettings,
  Jcstress / version := "0.4",
  name := "capsul-stress"
).dependsOn(core).dependsOn(mini).dependsOn(log)

lazy val examples = (project in file("examples")).settings(
  commonSettings,
  hiddenProjectSettings,
  libraryDependencies += "io.monix" %% "monix-eval" % monixVersion,
  libraryDependencies += "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  name := "capsul-examples"
).dependsOn(core).dependsOn(log % "compile-internal")

lazy val root = (project in file(".")).settings(
  name := "consul-root",
  commonSettings,
  hiddenProjectSettings
).aggregate(core, perf, examples)

