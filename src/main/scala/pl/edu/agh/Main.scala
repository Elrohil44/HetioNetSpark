package pl.edu.agh


import scala.collection.JavaConverters._

object Main extends SparkInit {
  def main(args: Array[String]): Unit = {
    import org.neo4j.spark.{Neo4jGraph, Neo4jGraphFrame, Neo4j}
    import org.neo4j.driver.v1.types.{Path, Node, Relationship, Entity}
    import org.graphframes.GraphFrame

    val neo4j = Neo4j(sc = sc)

    val graph = neo4j
      .cypher("MATCH p = (:Compound)-[*2..4]-(:Disease) RETURN p LIMIT 50")
      .partitions(4)
      .batch(50)
      .loadRowRdd
      .flatMap(r => {
        val path = r.getAs[Path](0)
        Seq(
          ("nodes", path.nodes().asScala.toArray[Entity]),
          ("rels", path.relationships().asScala.toArray[Entity])
        )
      })
      .map(d => {
        val entities = d._1 match {
          case "nodes" =>
            d._2.map(e => (e.id(), null.asInstanceOf[Long], null.asInstanceOf[String]))
          case "rels" =>
            d._2.map(r => (r.asInstanceOf[org.neo4j.driver.v1.types.Relationship].startNodeId(), r.asInstanceOf[org.neo4j.driver.v1.types.Relationship].endNodeId(), r.asInstanceOf[org.neo4j.driver.v1.types.Relationship].`type`()))
        }
        (d._1, entities)
      })
      .reduceByKey((a, b) => Array.concat(a, b))
      .collect()

    println(graph(0), graph(1))
  }
}
