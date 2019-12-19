name := "spark-neo4j"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVersion = "2.4.3"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-graphx" % sparkVersion,
    "graphframes" % "graphframes" % "0.7.0-spark2.4-s_2.11",
    "org.neo4j.driver" % "neo4j-java-driver" % "1.7.5",
    "org.scalatest" %% "scalatest" % "3.0.1" % Test
  )
}