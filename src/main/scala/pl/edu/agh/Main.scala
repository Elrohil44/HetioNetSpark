package pl.edu.agh

object Main extends SparkInit {
  def main(args: Array[String]): Unit = {
    import org.neo4j.spark.Neo4jGraph
    import Label._
    import Relationship._

    val graph = Neo4jGraph.loadGraph(sc = sc,
      Compound, Seq(CAUSES_CcSE), SideEffect)

    println(graph.vertices.count())
    println(graph.edges.count())
  }
}
