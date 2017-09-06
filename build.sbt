val scalaVer = "2.12.1"

val monixVersion = "2.3.0"

val commonSettings = Seq(
  scalaVersion := scalaVer,
  organization := "net.gfxmonk",
  name := "sequentialstate",
  description := "Minimal, thread-safe state encapsulation",
  version := "0.1.1-SNAPSHOT",

  libraryDependencies += "io.monix" %% "monix-execution" % monixVersion,
  libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

lazy val log = (project in file("log")).settings(
  commonSettings,
  scalacOptions ++= Seq("-language:experimental.macros"),
  libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVer,
  name := "sequentialstate-log"
)

lazy val core = (project in file("core")).settings(
  commonSettings,
  name := "sequentialstate"
) dependsOn log

lazy val examples = (project in file("examples")).settings(
  commonSettings,
  libraryDependencies += "io.monix" %% "monix" % monixVersion,
  libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.3",
  name := "sequentialstate-examples"
) dependsOn core


publishMavenStyle := true
publishTo := {
  val v = version.value
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}
publishArtifact in Test := false

licenses := Seq("MIT" -> url("http://www.opensource.org/licenses/mit-license.php"))
homepage := Some(url("https://github.com/timbertson/sequentialstate"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/timbertson/sequentialstate"),
    "scm:git@github.com:timbertson/sequentialstate.git"
  )
)

developers := List(
  Developer(
    id    = "gfxmonk",
    name  = "Tim Cuthbertson",
    email = "tim@gfxmonk.net",
    url   = url("http://gfxmonk.net")
  )
)

sonatypeProfileName := "net.gfxmonk"

credentials += Credentials(
  "Sonatype Nexus Repository Manager",
  "oss.sonatype.org",
  sys.env.getOrElse("SONATYPE_ACCOUNT", "timbertson"),
  sys.env.getOrElse("SONATYPE_PASSWORD", "******"))

