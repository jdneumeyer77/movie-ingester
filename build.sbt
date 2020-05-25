// The simplest possible sbt build file is just one line:

scalaVersion := "2.13.2"

name := "guild-de"
organization := "com.github.jdneumeyer77"
version := "1.0"

libraryDependencies ++= Seq(
  "com.github.pathikrit" %% "better-files-akka" % "3.9.1", // easier file handling.
  "com.nrinaudo" %% "kantan.csv" % "0.6.0", // csv parser
  "com.typesafe.play" %% "play-json" % "2.8.1",
   "com.nrinaudo" %% "kantan.csv-java8" % "0.6.0" // standalone play json parser
)
