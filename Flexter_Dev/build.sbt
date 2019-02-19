name := "Flexter_Dev"

version := "1.0"

scalaVersion := "2.11.8"

mainClass in (Compile, run) := Some("FlexterSplit")
mainClass in (Compile, packageBin) := Some("FlexterSplit")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.0.0" % "provided"

