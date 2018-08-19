// vim: set syntax=scala:

val scalaVer = "2.12.1"

scalaVersion in ThisBuild := scalaVer

val monixVersion = "2.3.0"

val akkaVersion = "2.5.3"

val scalaReflect = "org.scala-lang" % "scala-reflect" % scalaVer

crossScalaVersions := Seq("2.11.11", "2.12.1")

val commonSettings = Seq(
  organization := "net.gfxmonk",
  name := "capsul",
  description := "Minimal, thread-safe state encapsulation",
  version := "0.3.0",

  /* libraryDependencies += "io.monix" %% "monix-execution" % monixVersion, */
  libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

enablePlugins(JCStressPlugin)

val hiddenProject = commonSettings ++ Seq(
  publish := {},
  publishLocal := {}
)

lazy val log = (project in file("log")).settings(
  hiddenProject,
  scalacOptions ++= Seq("-language:experimental.macros"),
  libraryDependencies += scalaReflect,
  name := "capsul-log"
)

lazy val publicProjectSettings = Seq(
  publishTo := {
    val v = version.value
    val nexus = "https://oss.sonatype.org/"
    if (v.trim.endsWith("SNAPSHOT"))
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },
  publishMavenStyle := true,
  publishArtifact in Test := false,

  licenses := Seq("MIT" -> url("http://www.opensource.org/licenses/mit-license.php")),
  homepage := Some(url("https://github.com/timbertson/capsul")),

  scmInfo := Some(
    ScmInfo(
      url("https://github.com/timbertson/capsul"),
      "scm:git@github.com:timbertson/capsul.git"
    )
  ),

  developers := List(
    Developer(
      id    = "gfxmonk",
      name  = "Tim Cuthbertson",
      email = "tim@gfxmonk.net",
      url   = url("http://gfxmonk.net")
    )
  ),

  sonatypeProfileName := "net.gfxmonk",

  credentials += Credentials(
    "Sonatype Nexus Repository Manager",
    "oss.sonatype.org",
    sys.env.getOrElse("SONATYPE_ACCOUNT", "timbertson"),
    sys.env.getOrElse("SONATYPE_PASSWORD", "******"))
)

lazy val core = (project in file("core")).settings(
  commonSettings,
  /* libraryDependencies += scalaReflect, */
  name := "capsul",
  publicProjectSettings,
).dependsOn(log % "compile-internal").dependsOn(log % "test")

lazy val mini = (project in file("mini")).settings(
  commonSettings,
  name := "capsul-mini",
  /* publicProjectSettings */
).dependsOn(core % "test")

lazy val perf = (project in file("perf")).settings(
  hiddenProject,
  libraryDependencies += "io.monix" %% "monix" % monixVersion,
  libraryDependencies += "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  name := "capsul-perf"
).dependsOn(core).dependsOn(log)

lazy val stress = (project in file("stress")).settings(
  hiddenProject,
  version in Jcstress := "0.4",
  name := "capsul-stress"
).dependsOn(core).dependsOn(mini).dependsOn(log)

lazy val examples = (project in file("examples")).settings(
  hiddenProject,
  libraryDependencies += "io.monix" %% "monix-eval" % monixVersion,
  libraryDependencies += "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  name := "capsul-examples"
).dependsOn(core).dependsOn(log % "compile-internal")

lazy val root = (project in file(".")).settings(
  name := "consul-root",
  hiddenProject
).aggregate(core, perf, examples)

