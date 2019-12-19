package com.lansalo.graph

import java.io.File

import com.lansalo.model.ClassifiedDocumentsPair
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.graphframes.GraphFrame

object GraphAnalysis {

  val vertexOutputPath: String = "./target/csv-exports/vertex"
  val vertexCsvFileName: String = "vertex.csv"
  val edgeOutputPath: String = "./target/csv-exports/edge"
  val edgeCsvFileName: String = "edge.csv"

  // we select all those duplicate documents pair that as been marked as such by out ML algorithm
  // Note, prediction would normally be a Double (1.0/0.0) but in this example is rendered with a boolean for semplicity
  def selectDuplicates(input: Dataset[ClassifiedDocumentsPair])(implicit spark: SparkSession): Dataset[ClassifiedDocumentsPair] =
    input.filter(_.prediction)

  def toGraphX(duplicates: Dataset[ClassifiedDocumentsPair], bidirectional: Boolean = false)(implicit spark: SparkSession): Graph[String, Double] = {
    import spark.implicits._

    val vertices: RDD[(VertexId, String)] = duplicates.flatMap {
      case ClassifiedDocumentsPair((doc1, doc2), _, _) => Seq((doc1.id, doc1.title), (doc2.id, doc2.title))
    }.rdd.distinct()

    val edges: RDD[Edge[Double]] = duplicates.flatMap {
      case ClassifiedDocumentsPair((doc1, doc2), confidence, _) =>
        if (bidirectional)
          Seq(Edge(doc1.id, doc2.id, confidence), Edge(doc2.id, doc1.id, confidence))
        else
          Seq(Edge(doc1.id, doc2.id, confidence))
    }.rdd

    Graph(vertices, edges)
  }

  def toGraphFrame(duplicates: Dataset[ClassifiedDocumentsPair], bidirectional: Boolean = false)(implicit spark: SparkSession): GraphFrame = {
    import spark.implicits._
    val vertices: DataFrame = duplicates.flatMap {
      case ClassifiedDocumentsPair((doc1, doc2), _, _) => Seq((doc1.id, doc1.title), (doc2.id, doc2.title))
    }.toDF("id", "title").distinct()

    val edges: DataFrame = duplicates.flatMap {
      case ClassifiedDocumentsPair((doc1, doc2), confidence, _) =>
        if (bidirectional)
          Seq((doc1.id, doc2.id, confidence), (doc2.id, doc1.id, confidence))
        else
          Seq((doc1.id, doc2.id, confidence))
    }.toDF("src", "dst", "confidence")

    GraphFrame(vertices, edges)
  }

  def persistToCSV(graphFrame: GraphFrame): Unit = {
    graphFrame.edges.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(edgeOutputPath)
    graphFrame.vertices.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(vertexOutputPath)
  }

  private[graph] def renameCsvFiles(): (String, String) = {
    val vertexPath = new File(s"$vertexOutputPath/$vertexCsvFileName")
    val edgePath = new File(s"$edgeOutputPath/$edgeCsvFileName")
    new File(vertexOutputPath).listFiles().filter(_.getName.endsWith(".csv")).head.renameTo(vertexPath)
    new File(edgeOutputPath).listFiles().filter(_.getName.endsWith(".csv")).head.renameTo(edgePath)

    (vertexPath.getCanonicalPath, edgePath.getCanonicalPath)
  }

}
