name := "crateconnector"

organization := "au.net.hivemedia"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "io.crate"                      %  "crate-client"                         % "0.49.3",
  "org.scala-lang.modules"        %% "scala-pickling"                       % "0.10.1",
  "org.scalatest"                 %  "scalatest_2.11"                       % "2.2.4"           % "test"
)

publishTo ++= version { v: String =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}