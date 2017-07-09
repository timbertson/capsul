scalaVersion := "2.12.1"

libraryDependencies += "io.monix" %% "monix" % "2.3.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies ++= Seq(
	"com.typesafe.akka" %% "akka-stream" % "2.5.3",
	"com.typesafe.akka" %% "akka-stream-testkit" % "2.5.3" % Test
)
