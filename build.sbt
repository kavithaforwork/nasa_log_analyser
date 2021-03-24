name := "NasaWebAccessStatsApp"
version := "1.0.1"
scalaVersion in ThisBuild := "2.11.8"
dependencyOverrides += "org.scala-lang" % "scala-library" % scalaVersion.value

resolvers += "Default Repository" at "https://repo1.maven.org/maven2/"
resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3" % "provided"
libraryDependencies += "com.typesafe" % "config" % "1.2.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"


test in assembly:={}

assemblyJarName in assembly := "NasaWebAccessStats.jar"

mainClass in assembly := Some("gov.nasa.loganalyzer.NasaWebAccessStats")

