name := "mongo-akka-streams"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-http-core-experimental" % "1.0",
  "org.reactivemongo" %% "reactivemongo" % "0.11.7",
  "org.reactivemongo" %% "play2-reactivemongo" % "0.11.7.play24",
  "com.typesafe.play" % "play-json_2.11" % "2.4.3",
  "ch.qos.logback" % "logback-classic" % "1.1.2"
)

resolvers += "Typesafe" at "https://repo.typesafe.com/typesafe/releases/"

mainClass in (Compile, run) := Some("Boot")