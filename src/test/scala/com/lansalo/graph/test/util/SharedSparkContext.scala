package com.lansalo.graph.test.util

import org.apache.spark.sql.SparkSession

trait SharedSparkContext {

  //val config = new SparkConfig()
  implicit val spark: SparkSession = SparkSession.builder()
    .config("spark.ui.showConsoleProgress", false)
    .config("spark.neo4j.bolt.url", "bolt://neo4j@localhost:7687")
    .config("spark.neo4j.bolt.password", "gnogno")
    .master("local[1]")
    .getOrCreate()


}
