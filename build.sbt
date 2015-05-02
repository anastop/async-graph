name := "async_graph"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= 
	"com.typesafe.akka" %% "akka-actor" % "2.3.2" ::  
	"com.typesafe.akka" %% "akka-testkit" % "2.3.2" :: 
	"org.scalatest" % "scalatest_2.10" % "2.0" % "test" ::
	Nil

