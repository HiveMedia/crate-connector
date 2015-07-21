name := "crateconnector"

organization := "au.net.hivemedia"

version := "1.0.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "io.crate"                      %  "crate-client"           % "0.49.3",
  "org.scala-lang.modules"        %% "scala-pickling"         % "0.10.1"
)
    