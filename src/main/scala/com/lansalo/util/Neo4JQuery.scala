package com.lansalo.util

import org.neo4j.driver.v1.{Driver, Session, StatementResult}

object Neo4JQuery {

  def importGraphDB(vertexCsvPath: String, edgeCsvPath: String)(driver: Driver): StatementResult = {
    val session: Session = driver.session()
    try {
      session.run(createVertexesFromCsvQuery(vertexCsvPath))
      session.run(createEdgesFromCsvQuery(edgeCsvPath))
    }
    finally {
      session.close()
    }
  }

  /**
   * comment line # dbms.directories.import=import
   * set dbms.security.allow_csv_import_from_file_urls=true
   */
  private def createVertexesFromCsvQuery(vertexCsvPath: String): String = {
    s"""LOAD CSV WITH HEADERS FROM "file:///$vertexCsvPath" AS node
       | CREATE (doc:Document {
       | id: node.id,
       | title: node.title})
       |""".stripMargin
  }

  /**
   * Neo4j CAN'T store bidirectional relationships.
   * No way around this, however relationships can be treated as bidirectional when querying the graph
   */
  private def createEdgesFromCsvQuery(edgeCsvPath: String): String = {
    s"""LOAD CSV WITH HEADERS FROM "file:///$edgeCsvPath" AS edge
       | MATCH (a:Document),(b:Document)
       | WHERE a.id = edge.src AND b.id = edge.dst
       | CREATE (a)-[r: SAMEAS { confidence: edge.confidence }]->(b)
       |""".stripMargin
  }


  /**
   * Find graph where there is a path between two vertexes long at least `length`
   * NOTE: Does ignore relationship direction
   *
   * @param length
   * @return query as a String
   */
  private def findPathOfLength(length: Int): String =
    s"""MATCH p=(doca)-[:SAMEAS*$length]-(docb)
       |WHERE NOT((doca)-[:SAMEAS]-(docb))
       |RETURN p LIMIT 10""".stripMargin


  /**
   * Find graph with where at least one node has degree > of argument degree
   * NOTE: Does NOT ignore relationship direction
   *
   * @param degree
   * @return
   */
  private def findGraphWithNodeWithDegree(degree: Int): String =
    s"""MATCH (k)
       |WITH k, size((k)-[:SAMEAS]->()) as degree
       |WHERE degree > $degree
       |MATCH (k)-[r:SAMEAS]->(n)
       |RETURN k, r, n LIMIT 20""".stripMargin

}
