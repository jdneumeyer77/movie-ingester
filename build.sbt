// The simplest possible sbt build file is just one line:

scalaVersion := "2.13.4"

name := "hello-world"
organization := "com.github.jdneumeyer77"
version := "1.0"

libraryDependencies ++= Seq(
    "com.github.pathikrit"  %% "better-files-akka"  % "3.9.1", // easier file handling.
    "com.nrinaudo" %% "kantan.csv" % "0.6.0", // csv parser
    "com.typesafe.play" %% "play-json" % "2.8.1", // standalone play json parser
)

