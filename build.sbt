name := "click_prediction"

version := "0.1"

scalaVersion := "2.11.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2"

javacOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")