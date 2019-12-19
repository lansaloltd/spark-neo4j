package com.lansalo.graph

import com.lansalo.graph.GraphAnalysis._
import com.lansalo.graph.test.util.SharedSparkContext
import com.lansalo.model.{ClassifiedDocumentsPair, Document}
import com.lansalo.util.{Neo4JQuery, Neo4jDriver}
import org.apache.spark.sql.Dataset
import org.graphframes.GraphFrame
import org.scalatest.FunSuite

class GraphAnalysisTest extends FunSuite with SharedSparkContext {

  import TestFixture._
  import spark.implicits._

  test("Save duplicate documents graph as csv") {

    val dataDS: Dataset[ClassifiedDocumentsPair] = spark.createDataFrame(data).as[ClassifiedDocumentsPair]
    val duplicates: Dataset[ClassifiedDocumentsPair] = selectDuplicates(dataDS)
    val graph: GraphFrame = toGraphFrame(duplicates)

    // optionally do some processing on the graph and then persist is as csv
    persistToCSV(graph)
  }

  // Database as to be up and running, listening at bolt://localhost:7687 and password should be "password"
  // You can change those in Neo4jDriver.getDriver (where the driver is created)
  test("Populate Neo4J db from csv") {
    val (vertexPath, edgePath) = renameCsvFiles()
    val driver = Neo4jDriver.getDriver
    Neo4JQuery.importGraphDB(vertexPath, edgePath)(driver)

    // when you finish, close the driver
    driver.close()
  }
}

object TestFixture {

  val data: Seq[ClassifiedDocumentsPair] = Seq(
    ClassifiedDocumentsPair((Document(1, "title1"), Document(2, "title2")), 0.889, prediction = true),
    ClassifiedDocumentsPair((Document(1, "title1"), Document(3, "title3")), 0.79, prediction = true),
    ClassifiedDocumentsPair((Document(1, "title1"), Document(4, "title4")), 0.55, prediction = true),
    ClassifiedDocumentsPair((Document(1, "title1"), Document(5, "title5")), 0.912, prediction = true),
    ClassifiedDocumentsPair((Document(5, "title5"), Document(6, "title6")), 0.88, prediction = true),
    ClassifiedDocumentsPair((Document(6, "title6"), Document(7, "title7")), 0.78, prediction = true),
    ClassifiedDocumentsPair((Document(7, "title7"), Document(8, "title8")), 0.679, prediction = true),
    ClassifiedDocumentsPair((Document(8, "title8"), Document(9, "title9")), 0.889, prediction = true),
    ClassifiedDocumentsPair((Document(3, "title3"), Document(7, "title7")), 0.889, prediction = true),
    ClassifiedDocumentsPair((Document(7, "title7"), Document(9, "title9")), 0.889, prediction = true),
    ClassifiedDocumentsPair((Document(10, "title10"), Document(1, "title1")), 0.12, prediction = false),
    ClassifiedDocumentsPair((Document(10, "title10"), Document(9, "title9")), 0.22, prediction = false),
    ClassifiedDocumentsPair((Document(10, "title10"), Document(9, "title9")), 0.24, prediction = false),
    ClassifiedDocumentsPair((Document(10, "title10"), Document(11, "title11")), 0.889, prediction = true),
    ClassifiedDocumentsPair((Document(10, "title10"), Document(12, "title12")), 0.889, prediction = true),
    ClassifiedDocumentsPair((Document(11, "title11"), Document(12, "title12")), 0.889, prediction = true),
    ClassifiedDocumentsPair((Document(13, "title13"), Document(12, "title12")), 0.23, prediction = false),
    ClassifiedDocumentsPair((Document(13, "title13"), Document(11, "title11")), 0.89, prediction = true),
    ClassifiedDocumentsPair((Document(20, "title20"), Document(21, "title21")), 0.77, prediction = true),
    ClassifiedDocumentsPair((Document(20, "title20"), Document(22, "title22")), 0.87, prediction = true),
    ClassifiedDocumentsPair((Document(20, "title20"), Document(23, "title23")), 0.67, prediction = true),
    ClassifiedDocumentsPair((Document(20, "title20"), Document(24, "title24")), 0.69, prediction = true),
    ClassifiedDocumentsPair((Document(20, "title20"), Document(25, "title25")), 0.666660, prediction = true),
    ClassifiedDocumentsPair((Document(20, "title20"), Document(26, "title26")), 0.89, prediction = true),
    ClassifiedDocumentsPair((Document(20, "title20"), Document(27, "title27")), 0.87, prediction = true),
    ClassifiedDocumentsPair((Document(20, "title20"), Document(28, "title28")), 0.979, prediction = true),
    ClassifiedDocumentsPair((Document(30, "title30"), Document(31, "title31")), 0.78, prediction = true),
    ClassifiedDocumentsPair((Document(30, "title30"), Document(32, "title32")), 0.89, prediction = true),
    ClassifiedDocumentsPair((Document(40, "title40"), Document(41, "title41")), 0.89, prediction = true),
    ClassifiedDocumentsPair((Document(41, "title41"), Document(42, "title42")), 0.89, prediction = true),
    ClassifiedDocumentsPair((Document(41, "title41"), Document(43, "title43")), 0.89, prediction = true),
    ClassifiedDocumentsPair((Document(43, "title43"), Document(44, "title44")), 0.89, prediction = true),
    ClassifiedDocumentsPair((Document(43, "title43"), Document(45, "title45")), 0.89, prediction = true)
  )
}
