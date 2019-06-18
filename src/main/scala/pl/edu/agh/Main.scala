package pl.edu.agh

object Main extends SparkInit {
  def main(args: Array[String]): Unit = {
    import org.neo4j.spark.Neo4j

    val neo4j = Neo4j(sc = sc)

    val graphFrame = neo4j
      .rels("MATCH (a)-[r]->(b) RETURN ID(a) as src, ID(b) as dst, type(r) as relationship")
      .nodes("MATCH (n) RETURN ID(n) as id, labels(n)[0] as type", Map.empty)
      .loadGraphFrame

    val paths = graphFrame
      .find("(a)-[e]->(b)")
      .filter("a.type = 'Compound'")
      .filter("b.type = 'Disease'")

  }
}
