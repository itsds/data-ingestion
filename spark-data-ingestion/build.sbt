name := "spark-data-ingestion"

version := "0.1"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.6"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.6"
libraryDependencies += "commons-io" % "commons-io" % "2.6"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.6"
libraryDependencies += "org.datasyslab" % "geospark" % "1.3.1" % "provided"








