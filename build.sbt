name := "crateconnector"

organization := "au.net.hivemedia"

version := "1.0.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "io.crate"                      %  "crate-client"                         % "0.49.3",
  "org.json4s"                    %% "json4s-native"                        % "3.2.11",
  "org.scalatest"                 %  "scalatest_2.11"                       % "2.2.4"           % "test"
)

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}