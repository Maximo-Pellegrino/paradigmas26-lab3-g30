name := "reddit-ner-scala"

version := "0.1.0"

scalaVersion := "2.13.18"

ThisBuild / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat

fork := true

ThisBuild / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "-DSPARK_LOCAL_IP=127.0.0.1"
)

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-sql" % "3.5.1",
"org.apache.spark" %% "spark-core" % "3.5.1",
"org.json4s" %% "json4s-jackson" % "3.7.0-M11",
"com.github.scopt" %% "scopt" % "4.1.0"
)
