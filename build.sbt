name := "reactivemongo-cursor-retry-bug"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.reactivemongo" %% "reactivemongo"  % "0.16.2" % Test,
  "org.scalatest"     %% "scalatest"      % "3.0.5"  % Test,
  "ch.qos.logback"    % "logback-classic" % "1.2.3"  % Test,
  "com.spotify"       % "docker-client"   % "8.15.1" % Test
)
