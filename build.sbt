name := "spipe"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.3"
val scalazVersion = "7.2.27"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
  "org.scalaz" %% "scalaz-core" % scalazVersion
)