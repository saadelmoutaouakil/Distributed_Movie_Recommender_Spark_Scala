name := "m2_yourid"
version := "1.0"

libraryDependencies += "org.rogach" %% "scallop" % "4.0.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"
libraryDependencies += "org.scalanlp" %% "breeze" % "0.13.2"
libraryDependencies += "org.scalanlp" %% "breeze-natives" % "0.13.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0" % Test
libraryDependencies += "com.lihaoyi" %% "ujson" % "1.5.0"

scalaVersion in ThisBuild := "2.11.12"
enablePlugins(JavaAppPackaging)
logBuffered in Test := false

test in assembly := {}
assemblyMergeStrategy in assembly := {   
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard   
  case x => MergeStrategy.first 
}
