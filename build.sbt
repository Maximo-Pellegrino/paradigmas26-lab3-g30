name := "reddit-ner-scala"

version := "0.1.0"

scalaVersion := "2.13.18"

ThisBuild / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat

fork := true

ThisBuild / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
)

libraryDependencies ++= Seq(
  "org.json4s" %% "json4s-jackson" % "3.7.0-M11",
  "com.github.scopt" %% "scopt" % "4.1.0"
)
