package com.lansalo.util

import org.neo4j.driver.v1.{AuthTokens, Driver, GraphDatabase}

object Neo4jDriver {

  def getDriver: Driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "password"))

}