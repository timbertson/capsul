scalaVersion := "2.12.1"
organization := "net.gfxmonk"
name := "sequentialstate"
description := "Minimal, thread-safe state encapsulation"
version := "0.1.1-SNAPSHOT"

val monixVersion = "2.3.0"

libraryDependencies += "io.monix" %% "monix-execution" % monixVersion
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

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

